# CERULION fork of `re_grpc_server` 0.34.1 — CER-858 + CER-959

This repository (`cerulion-inc/re_grpc_server`) is a **sparse crate fork** of
**upstream `re_grpc_server` 0.34.1** (from `rerun-io/rerun`, exactly as published
to crates.io) **plus one localized patch**. It is pinned into `cerulion-base` via
the root `Cargo.toml` `[patch.crates-io]` git rev (the `cerulion-inc/RustDDS` fork
precedent) and allow-listed in `cerulion-base`'s `deny.toml [sources]`.

The `upstream` branch holds the crates.io 0.34.1 tarball verbatim; `main` is
`upstream` plus the patch. See `README.md` for the branch model and the
`./import-upstream.sh` upgrade procedure.

## Why a fork

`cerulion-vizd` hosts a `re_grpc_server` gRPC message proxy (rerun's `server`
feature, via `RecordingStreamBuilder::serve_grpc_opts`). Cerulion Studio uses
rerun as **pure live viz** — a plot starts getting data when its checkbox is
checked; no backlog, no recording, no history, no catch-up burst (founder
ruling, CER-858).

The proxy's message buffer is a **per-client replay buffer**: on every new
`ReadMessages` connection the `EventLoop` streams the retained history to *that
client only* (`Event::NewClient` → `history.all()`, `lib.rs`), then subscribes it
to the live broadcast. Stock 0.34.1 offers two knobs (`ServerOptions`):
`playback_behavior` and `memory_limit`. Neither can express **"deliver the scene
skeleton to a late joiner, but never replay temporal data"**:

- `memory_limit = ZERO` disables history entirely — a late joiner gets a **blank
  scene** (no blueprint / robot model / TF), because it also drops
  `persistent` + `static_`.
- Any non-zero `memory_limit` retains statics **and** a byte-sized tail of
  temporal frames. That tail is worst exactly for the founder's complaint: a
  low-byte high-rate plot topic (IMU/odom/twist) replays *thousands* of points
  on connect.

A periodic re-log ("heartbeat") from vizd was rejected: re-sending the blueprint
with `make_active` clobbers every existing viewer's layout, and re-logging
`Transform3D`/`Pinhole` statics (full-history archetypes, not latest-at)
accumulates unboundedly in every viewer's chunk store.

The connect-history is already the correct **per-client, once-per-connect**
delivery mechanism — it just needs to carry statics-only. That is this patch.

## The patch (delta vs upstream 0.34.1)

A new `ServerOptions` field `drop_temporal_history: bool` (default `false` — stock
behavior). When `true`, the history buffer **retains `persistent` (SetStoreInfo +
blueprint + BlueprintActivationCommand) and `static_` (`is_static` chunks) but
NEVER buffers disposable/temporal frames** (recording data, tables, UI commands).
Live subscribers still receive live temporal data over the broadcast; only the
per-client *replay* buffer drops it. Result: a fresh client's connect-history is
the scene skeleton with **zero temporal replay, at any producer bandwidth**, and
because nothing is periodically re-sent there is **no blueprint clobber and no
accumulation**.

Files touched, all in `src/lib.rs` (plus literal updates so the crate compiles):

| Site | Change |
|---|---|
| `struct ServerOptions` | `+ pub drop_temporal_history: bool` (+ doc) |
| `impl Default for ServerOptions` | `+ drop_temporal_history: false` |
| `struct MessageBuffer` | `+ drop_temporal: bool` |
| `MessageBuffer::add_msg` | guard the `Table` + `UiCommand` disposable pushes with `if !self.drop_temporal` |
| `MessageBuffer::add_log_msg` | guard the recording-data (disposable) push with `else if !self.drop_temporal` |
| `EventLoop::new` | build `MessageBuffer { drop_temporal: options.drop_temporal_history, ..Default::default() }` |
| `EventLoop::is_history_enabled` | `\|\| self.options.drop_temporal_history` — statics are retained even at a ZERO byte budget |
| `EventLoop::gc_if_using_too_much_ram` | `!self.options.drop_temporal_history && …` — GC is OFF under the flag |
| `src/main.rs`, `#[cfg(test)] setup()` / `setup_with_memory_limit()` / `playback_newest_first` | add `drop_temporal_history: false` to every `ServerOptions` literal |
| `#[cfg(test)] mod tests` | `+ setup_drop_temporal()` helper + `drop_temporal_history_retains_statics_not_temporal` test |

> **Fork completion (test-only).** The originally vendored tree left one
> `#[cfg(test)]` `ServerOptions` literal — in the `playback_newest_first` test —
> without the new field, so `cargo test` on the crate did NOT compile. This was
> invisible while the crate was a *vendored dependency* of `cerulion-base`
> (cargo does not compile a dependency's own unit tests), so the production
> `[lib]` artifact `cerulion-base` builds was always correct and unchanged. The
> fork adds `drop_temporal_history: false` to that literal so the fork's own
> `cargo test` is green (15/15 at fork creation; 18/18 after the F1/F2
> review-train tests below). The change is inside `#[cfg(test)]` — the
> compiled dependency artifact is byte-identical to the vendored tree.

## History-bounding under `drop_temporal` (F1 + F2, CER-858 review train)

Under `drop_temporal_history`, the stock `gc()` is OFF (every retained frame is a
needed skeleton frame — a `memory_limit` eviction could only drop something a late
joiner needs). Two paths would otherwise grow the retained history **without
bound**, so both are bounded at buffer time — **ONLY under the flag**; with
`drop_temporal_history == false` behavior is byte-identical to upstream.

| # | Unbounded path | Fix |
|---|---|---|
| F1 | `persistent` grows on every Studio `set_blueprint`: each layout change mints a **FRESH blueprint store**, and every new client would replay ALL superseded blueprints. | When a `BlueprintActivationCommand` for blueprint `B` is buffered, evict from `persistent` every message belonging to a Blueprint-KIND store ≠ `B` (its `SetStoreInfo`, chunks, and stale activation commands), preserving relative order + exact `size_bytes`. A recording-data `SetStoreInfo` is **not** blueprint-kind, so it always survives. Keyed on the blueprint store's `recording_id` (its unique uuid). Gated on the flag — stock upstream keeps every blueprint (the flag's owner activates each new blueprint `make_active`, so "activated = supersedes prior" is exact for Studio). |
| F2 | `static_` grows on every reconnect: a reconnecting client re-logs the same statics, appending duplicates. | A static `ArrowMsg` for entity `E` **REPLACES** the prior retained static for the same `(store_id, E)` (latest-wins, matching rerun's own static semantics) instead of appending. The entity path is decoded from the arrow payload (`ArrowMsg::to_application` → the batch schema's `rerun:entity_path` metadata) **only on the infrequent static path**; a decode failure falls back to a plain append (never a wrong-key drop). |

> **F2 caveat.** Replacement is **entity-level**: logging DIFFERENT component sets
> statically to the SAME entity across sends would drop the earlier components.
> vizd never does this — statics are logged once per entity and re-logged
> IDENTICALLY on reconnect — so entity-level replacement is exactly latest-wins
> with no loss.

Files: all in `src/lib.rs` — `MessageBuffer::{evict_superseded_blueprints,
message_store_id, static_key_from_arrow, static_key_from_msg}`, `MsgQueue::retain`
(order-preserving, byte-exact), and the split `add_log_msg`
blueprint-activation / static arms. Pinned by the fork tests
`drop_temporal_evicts_superseded_blueprint_stores`,
`drop_temporal_blueprint_eviction_preserves_activation_ordering`, and
`drop_temporal_dedups_static_relog_per_entity`.

`memory_limit` / `playback_behavior` semantics are **byte-identical to upstream
when `drop_temporal_history == false`**. When the flag is set, the mode is
**self-contained**: temporal is never buffered, statics are always retained and
**never GC-evicted** (every retained frame is a needed skeleton frame — logged
once by the producer — so a `memory_limit`-driven eviction could only ever drop
something a late joiner needs). `memory_limit` is therefore **moot** under the
flag.

Base drop-temporal delta: ~16 lines (the review-train history-bounding is the
separate F1/F2 section below). No `unsafe`, no new deps (F1/F2 reuse the existing
`re_sorbet` / `re_log_encoding` deps), no signature changes to public functions
(only an additive struct field).

## `Cargo.toml` transformation (packaging cleanup, `main` vs `upstream`)

The `upstream` branch carries the crates.io-normalized `Cargo.toml` verbatim. On
`main` the manifest is rewritten for a `git`-dependency fork (semantically
dependency-identical — the compiled crate is unchanged). The full transformation:

| Manifest / file change | Detail |
|---|---|
| Packaging-only fields dropped | `build = false`, `include`, `publish = true`, `homepage`, `readme`, `repository`, `[package.metadata.docs.rs]` — a git-pinned fork is never published to crates.io. |
| `description` suffixed | `(Cerulion vendored fork, CER-858)`. |
| Dependencies reformatted | From the crates.io `[dependencies.<name>]` table form into inline `[dependencies]` entries (identical versions + features). |
| `[lints.*]` **RESTORED** (F3) | The upstream `[lints.clippy]` / `[lints.rust]` / `[lints.rustdoc]` blocks (incl. `unsafe_code = "deny"`) — the crates.io materialization of the rerun **workspace** lints (workspace root `Cargo.toml` @ `4efb18f17f6f0e41985cda99a2bdcd012febc8d5`, identical to the `upstream` branch's flattened blocks) — are carried **verbatim**. An earlier fork revision had dropped them (fidelity loss). The patch adds no `unsafe`; the whole crate + tests build clean under them. The lint name `empty_enum` is kept verbatim as the workspace has it (a newer clippy renamed it to `empty_enums` — a benign toolchain-drift *warning* under `-D warnings`, clippy still exits 0). |
| `clippy.toml` **added** (F3) | `allow-unwrap-in-tests = true`, mirroring the ONE setting from rerun's workspace `clippy.toml` that the restored `unwrap_used = "warn"` depends on. Keeps `unwrap_used` protecting **production** code while exempting this crate's own `#[cfg(test)]` code (invisible when consumed as a dependency, since cargo does not compile a dependency's tests). No lint was dropped. |

These `Cargo.toml` / `clippy.toml` changes change no dependency, feature, or
compiled behavior.

## Compatibility notes

- **`src/lib.rs` doc pointer (F5).** The `ServerOptions::drop_temporal_history`
  doc comment now points at `CERULION-PATCH.md` at this crate's root — the
  authoritative copy of this document. It previously carried the stale pre-fork
  `vendor/re_grpc_server/CERULION-PATCH.md` path (fixed in the CER-858 review
  train; the same edit backticks `re_grpc_server` to satisfy the restored
  `doc_markdown` lint).

- **Upstream `ServerOptions` literals under other features.** Upstream constructs
  `ServerOptions` with explicit-field literals (no `..Default::default()`) in
  `rerun`'s `clap.rs` (feature `clap`/`web_viewer`) and
  `commands/entrypoint.rs` (feature `run`). Those modules are **not compiled** in
  Cerulion's build (`rerun` is pulled with only `sdk` + `server`), so the added
  field does not break them. **LANDMINE:** if a future workspace change enables
  `run`/`clap`/`web_viewer` on `rerun`, those upstream literals would need the
  field. Watch this on any `rerun` feature change.

## Patch 2 (CER-959) — `live_temporal_budget_bytes`: the LIVE queue

CER-858 (above) killed the per-client **replay** buffer. There is a **second,
independent buffer on the same path** it did not touch: the **live broadcast
queue** every already-connected viewer reads from.

`EventLoop::handle_msg` hands every message to a byte-quota'd broadcast channel
(`CHANNEL_SIZE_MESSAGES` = 1024 messages, `CHANNEL_SIZE_BYTES` = 128 MiB — private
crate constants, not `ServerOptions` knobs, which is why CER-858 could not reach
them) and **awaits** space when it is full. So a viewer that renders slower than
the producer publishes accumulates frames there.

Measured (`cerulion-base`, `cer959_live_backlog_test.rs`, a 1280x720 RGB8 frame at
30 Hz, which is what `cerulion-vizd` logs per decoded H.264 picture since CER-922
decodes on the desk): against a receiver that keeps up the queue holds **0 bytes**;
against one that does not it reached **27.8 MB — 10.1 frames — in 2.4 s** and kept
climbing, headed for the 128 MiB ceiling at ~47 frames ≈ **1.6 s of stale video** a
viewer must play through before it shows the present. That is the founder's pt58
report ("h264 topic in the shell is still really laggy") surviving a decode fix
that made the decode itself sub-millisecond.

Founder ruling (pt58, verbatim): *"we want no history wherever possible (ofc things
like plots should only start showing history etc when you start vizing them, but
not before). so there shouldnt be a buffer for clouds or images etc."*

### The knob

`ServerOptions::live_temporal_budget_bytes: Option<u64>`. `None` (default) is
stock behaviour. `Some(n)`: when the live queue already holds more than `n` bytes,
a further **temporal** message is DROPPED instead of awaited.

- **Only temporal is eligible.** `SetStoreInfo`, blueprint chunks, blueprint
  activation commands and `is_static` chunks are the scene skeleton a viewer
  cannot render without, and always take the reliable awaiting path. So are
  `TableMsg` (a one-shot dataframe — dropping it loses the whole table while the
  writer still gets an `Ok`) and `DataSourceUiCommand` (control, carrying an
  `on_done` responder whose loss surfaces to the caller as a *viewer* fault).
  Those two are `disposable` in `MessageBuffer`, but that word there means "not
  worth REPLAYING to a late joiner", which does not imply "safe to never deliver".
  The classifier (`is_temporal`) mirrors `add_log_msg` for `LogMsg` and is
  oracle-tested in both directions
  (`only_recording_data_on_a_timeline_is_temporal`) — mutation-verified: dropping
  either the `is_static` guard or the blueprint guard fails it.
- **Both quotas are guarded.** The channel is bounded by `CHANNEL_SIZE_BYTES`
  *and* `CHANNEL_SIZE_MESSAGES` (1024), and `send_async` awaits when EITHER is
  reached. A byte-only gate would therefore still wedge: a stream of small
  messages (plots, `/tf`, telemetry at a few KB each) reaches 1024 messages at
  only a few MiB, so an 8 MiB budget never fires, the send awaits, and the event
  loop — which also serves `Event::NewClient` — stalls. The gate keeps
  `LIVE_MESSAGE_RESERVE` (64) slots free on the message axis too, which is also
  what guarantees skeleton messages somewhere to go while temporal is being shed.
- **Small messages are exempt from the BYTE axis** (`LIVE_SMALL_MESSAGE_FLOOR_BYTES`
  = 8 KiB, `pub` so a host can pin its own message classes against it). The byte
  comparison is against OCCUPANCY, not against the arriving message, so without a
  floor a queue held over budget by a few megabyte-sized camera frames sheds EVERY
  temporal message at any size — every plot sample, `/tf` update, `TextLog` line
  and `Clear` sharing the one broadcast queue. That trade is wrong on both halves:
  it saves the viewer well under a millisecond (MEASURED through the production
  encode path: `Scalars` 1229 B, recursive `Clear` 1223 B, `TextLog` 1282 B,
  `Transform3D` 1537 B, against 696_500 B for the smallest camera rendition and
  2_779_467 B for 1280x720), and it can lose a message that never comes again — a
  `rerun::Clear` is a one-shot STATE TRANSITION, so dropping it leaves a deleted
  entity rendered for the rest of the session, converting a latency problem into a
  persistently wrong picture. 8 KiB is 5.3x the largest control-class message and
  85x below the smallest image-class one, so the band it separates is two orders
  of magnitude wide. The MESSAGE axis is untouched (that is what prevents the
  wedge), which also bounds the exemption: floor-admitted traffic can add at most
  `(1024 - 64) * 8 KiB` = 7.5 MiB on top of the byte budget, and only if all 960
  slots hold a message just under the floor. Oracle-tested on both sides and on
  both axes (`the_small_message_floor_exempts_the_byte_axis_and_only_the_byte_axis`).
- **A budget outside the usable band is reported, not clamped.** The byte axis can
  only fire for `LIVE_SMALL_MESSAGE_FLOOR_BYTES < n < CHANNEL_SIZE_BYTES`: at or
  below the floor nothing is ever over-budget-dropped, and at or above the
  channel's own quota occupancy can never reach the budget (the sender awaits
  first). Either way the option is accepted and then inert on that axis while the
  server looks configured, so `MessageProxy::new` emits one `warn!` naming which
  end was crossed. Clamping would hide the mistake. Pure classifier
  (`live_budget_band_complaint`), oracle-tested on both thresholds.
- **Dropping, not a smaller quota.** Shrinking either quota would make the event
  loop AWAIT sooner, with the same `Event::NewClient` consequence.
- **One message always fits.** The gate compares the budget against the CURRENT
  occupancy, so an empty queue accepts a message of any size.
- **Never silent, never a flood.** A dropped frame must not leave an operator
  staring at choppy video with no explanation, and must not emit a line per frame
  for as long as a viewer is behind. So drops ride a loud-once regime cycle
  mirroring `cerulion_core`'s `FailureRegimeLatch` (which cannot be imported
  here): the FIRST drop of a regime is one `warn!` naming the cause and the
  budget, repeats are `debug!` carrying the running totals, and the first
  temporal message that gets through emits one `info!` reporting what the regime
  cost and RE-ARMS — so a viewer that flaps does not go silent after its first
  bad patch. The policy is the pure `decide_live` and is oracle-tested
  (`a_drop_regime_is_loud_once_then_accumulates_until_it_closes`). The
  unconditional running total is additionally reported as `live_dropped` by
  `MessageProxyHandle::capture_memory`, beside `broadcast` / `disposable` /
  `static` / `persistent`.
- **Honest limit on that counter.** A host that reaches this server through
  `re_sdk`'s `GrpcServerSink` (which is how `cerulion-vizd` hosts it) gets no
  `MessageProxyHandle` back — `serve_from_channel` constructs the `MessageProxy`
  internally and never returns a handle — so `capture_memory` is not callable
  from such a host at all, and for them the LOG is the entire window. Exposing it
  properly needs a `spawn_from_channel` returning the handle (mirroring
  `spawn_from_rx_set`) plus plumbing through `re_sdk`; recorded as a follow-up
  rather than claimed.

### Interaction with the replay history

A dropped message returns BEFORE `MessageBuffer::add_msg`, so it does not enter
the per-client replay history either. That is the intended reading — a frame not
worth sending live is not worth replaying — and under vizd's configuration it is
inert anyway, since `drop_temporal_history` already refuses temporal data. It is
called out because a future user who sets a budget WITHOUT
`drop_temporal_history` would otherwise be surprised that the live budget also
thins their retained history.

### Wiring coverage

`the_live_budget_gate_is_wired_to_the_real_broadcast_queue` drives the REAL
`EventLoop::handle_msg` with a budget actually set, over a real broadcast channel
whose receiver is never drained. Every other `ServerOptions` in this crate's test
module sets `live_temporal_budget_bytes: None`, so without that arm the suite
never executed the gate, `live_occupancy()`, `report_live_drop` or the floor even
once — a wiring regression (a swapped axis, a floor compared against the wrong
quantity, a budget never threaded through) would have left it green while the pure
`decide_live` oracles, which are handed a hand-written `LiveOccupancy`, all passed.

### Residuals

A `rerun::Clear` — or any other sub-floor one-shot — can still be dropped on the
MESSAGE axis, i.e. when the queue is at `LIVE_MESSAGE_RESERVE` because ~960
messages are already in flight. That axis is what keeps `send_async` from awaiting
and the event loop from wedging, so nothing can be exempt from it; the alternative
to dropping there is stalling `Event::NewClient`. The byte axis was the one that
fires in the condition this patch exists for (a viewer behind on video), and the
floor closes it. A per-stream budget is the real answer to both and is not what
this ships.

In the CER-906 *fallback* regime (no local H.264 decoder, so the VIEWER decodes and
vizd forwards raw `VideoStream` samples) a dropped sample breaks the P-frame
reference chain until the next keyframe. The drop only fires once the viewer is
already megabytes behind — a stream that is already unusable — and CER-922 made
desk-side decode the default, so the fallback is the rare path. Recorded here
rather than guarded, because the alternative (never dropping) is the seconds-of-lag
behaviour this patch exists to remove.

## Exit condition

Drop this fork entirely (revert to the crates.io `re_grpc_server`, prune the
`[patch.crates-io]` rev and the `deny.toml [sources]` entry) if upstream
`re_grpc_server` ever ships BOTH a statics-only / drop-temporal history mode
(CER-858) and a bounded, non-blocking live queue (CER-959). Upstream already
carries a `TODO(emilk)` to move `CHANNEL_SIZE_MESSAGES`/`CHANNEL_SIZE_BYTES` into
`ServerOptions`, which would be the natural home for the second half.
