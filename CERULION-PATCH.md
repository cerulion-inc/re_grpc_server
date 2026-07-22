# CERULION fork of `re_grpc_server` 0.34.1 — CER-858

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
> `cargo test` is green (15/15). The change is inside `#[cfg(test)]` — the
> compiled dependency artifact is byte-identical to the vendored tree.

`memory_limit` / `playback_behavior` semantics are **byte-identical to upstream
when `drop_temporal_history == false`**. When the flag is set, the mode is
**self-contained**: temporal is never buffered, statics are always retained and
**never GC-evicted** (every retained frame is a needed skeleton frame — logged
once by the producer — so a `memory_limit`-driven eviction could only ever drop
something a late joiner needs). `memory_limit` is therefore **moot** under the
flag.

Total functional delta: ~16 lines. No `unsafe`, no new deps, no signature
changes to public functions (only an additive struct field).

## `Cargo.toml` transformation (packaging cleanup, `main` vs `upstream`)

The `upstream` branch carries the crates.io-normalized `Cargo.toml` verbatim. On
`main` the manifest is rewritten for a `git`-dependency fork (semantically
dependency-identical — the compiled crate is unchanged):

- Packaging-only fields dropped: `build = false`, `include`, `publish = true`,
  `homepage`, `readme`, `repository`, `[package.metadata.docs.rs]` — a
  git-pinned fork is never published to crates.io.
- `description` gains a `(Cerulion vendored fork, CER-858)` suffix.
- Dependencies reformatted from the crates.io `[dependencies.<name>]` table form
  into inline `[dependencies]` entries (identical versions + features).
- The upstream `[lints.clippy]` / `[lints.rust]` / `[lints.rustdoc]` blocks
  (including `unsafe_code = "deny"`) are dropped — they were rerun's workspace
  lint set, irrelevant to a consumed dependency. (The patch adds no `unsafe`.)

These `Cargo.toml` changes are the ONLY divergence beyond the ~16-line functional
patch, and they change no dependency, feature, or compiled behavior.

## Compatibility notes

- **`src/lib.rs` doc pointer.** The `ServerOptions::drop_temporal_history` doc
  comment ends with `See vendor/re_grpc_server/CERULION-PATCH.md.` — a stale path
  from the pre-fork vendored era. It is **left byte-identical to the tree
  `cerulion-base` built and tested green** (build fidelity of the git pin > a
  cosmetic comment). The authoritative copy of this document is at this repo's
  root (`CERULION-PATCH.md`).

- **Upstream `ServerOptions` literals under other features.** Upstream constructs
  `ServerOptions` with explicit-field literals (no `..Default::default()`) in
  `rerun`'s `clap.rs` (feature `clap`/`web_viewer`) and
  `commands/entrypoint.rs` (feature `run`). Those modules are **not compiled** in
  Cerulion's build (`rerun` is pulled with only `sdk` + `server`), so the added
  field does not break them. **LANDMINE:** if a future workspace change enables
  `run`/`clap`/`web_viewer` on `rerun`, those upstream literals would need the
  field. Watch this on any `rerun` feature change.

## Exit condition

Drop this fork entirely (revert to the crates.io `re_grpc_server`, prune the
`[patch.crates-io]` rev and the `deny.toml [sources]` entry) if upstream
`re_grpc_server` ever ships a statics-only / drop-temporal history mode.
