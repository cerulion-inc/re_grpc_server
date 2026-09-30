# `re_grpc_server` — Cerulion sparse fork

A **sparse crate fork** of [`re_grpc_server`](https://crates.io/crates/re_grpc_server)
`0.34.1` (from [`rerun-io/rerun`](https://github.com/rerun-io/rerun)) carrying
**two localized Cerulion patches** for Cerulion Studio's instant-only live viz:
a `ServerOptions::drop_temporal_history` mode (fresh viewer clients receive the
scene skeleton — statics + blueprint — with **zero temporal replay**, at any
producer bandwidth; tracked in **CER-858**) and a
`ServerOptions::live_temporal_budget_bytes` bound on the live broadcast queue
(when set, a viewer that renders slower than the producer publishes is served
the present instead of a backlog; tracked in **CER-959**). The full delta is in
[`CERULION-PATCH.md`](./CERULION-PATCH.md).

This is a *single-crate* fork (one crate at the repo root), not a fork of the
whole rerun monorepo. It is consumed by `cerulion`
(github.com/cerulion-inc/cerulion) as a `git` `[patch.crates-io]` pin, keeping
cold-CI git-clone weight to this one small crate instead of rerun's ~278 MB
monorepo.

## Branch model

| Branch | Contents |
|---|---|
| `upstream` | The crates.io `re_grpc_server` 0.34.1 tarball **verbatim** at repo root (packaging artifacts `.cargo_vcs_info.json` / `Cargo.toml.orig` stripped). Tagged `upstream/<version>`. |
| `main` | `upstream` + the Cerulion patches. This is the branch `cerulion` pins. |

Keeping the pristine upstream tree on its own branch makes the Cerulion delta a
reviewable `git diff upstream..main` and makes version bumps a clean merge.

## Upgrading to a new upstream release

```sh
# 1. Refresh the upstream branch to the new crates.io tarball + tag it.
./import-upstream.sh 0.35.0

# 2. Bring the patches forward onto main (resolve any conflicts in src/lib.rs).
git checkout main
git merge upstream

# 3. Rebuild + retest, then update the pinned rev in cerulion's
#    root Cargo.toml [patch.crates-io].
```

`import-upstream.sh` downloads the crates.io tarball for the given version,
replaces the tree on the `upstream` branch, commits, and tags `upstream/<version>`.

## Exit condition

Drop this fork (revert `cerulion` to the crates.io `re_grpc_server`) if
upstream ever ships BOTH a statics-only / drop-temporal history mode and a
bounded, non-blocking live queue. Either alone is not enough: the two patches
bound two different buffers on the same path. See `CERULION-PATCH.md`.

## License

Same as upstream rerun: dual-licensed under
[MIT](./LICENSE-MIT) or [Apache-2.0](./LICENSE-APACHE), at your option.
