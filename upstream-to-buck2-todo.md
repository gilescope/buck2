# Upstream-to-buck2 TODO

Gift-PR candidates for facebook/buck2, held back deliberately: each
should soak in the fork across many sweep laps before we offer it
upstream. Ordered roughly by upstream-readiness.

Policy: nothing here goes upstream without an explicit per-PR decision
(and Giles' consent) - see the fork-maintenance notes in CLAUDE.md.

## Soaking (in fork, awaiting mileage)

- **Expose `rustc_env` on `system_rust_toolchain`** (`a2fc3cbf8`).
  `RustToolchainInfo.rustc_env` was always plumbed into rustc action
  env (`prelude/rust/build.bzl:1547`); the system toolchain never
  surfaced it. Mechanical, low-risk. Soak: needs laps with
  `SOURCE_DATE_EPOCH = "0"` set in buck2-fixups.
- **Reproducible rust links on MSVC** (fixups-side today: `342652f`,
  `-Clink-arg=/Brepro` + `/pdbaltpath:%_PDB%`). The proper upstream fix
  is in the prelude rust rules: route rust link steps through the same
  determinism flags `get_output_flags` gives cxx links
  (`prelude/cxx/linker.bzl:250`). Field evidence: one lost win root
  re-keyed 2,269 downstream actions per lap until the flag landed.
- **macOS/BSD `rss_bytes` via sysinfo** (`2e77fb9ab`).
  `process_stats().rss_bytes` was linux-procfs-only; sysinfo is already
  a workspace dep. Small, generally useful (snapshots report live RSS
  on mac).

## Longer soak / needs carving out of the persistence stack

- **Checked eviction** (`ebdba955a`, part of): `CoreState::evict_keys`
  entries carry the serialized value; page-out only lands on pointer
  match. Closes a serialize-then-evict TOCTOU that exists upstream in
  spirit wherever eviction meets a live graph. Only meaningful to
  upstream alongside the pagable/page-out stack itself.
- **Pagable fixes** (from the S1-S3 work): SmallVec bound, rusqlite
  limits, PagablePanic-declines-serialization, enum equality by
  (type id, index).
- **`host_info()` -> exec-config select in prelude cxx discovery**
  (`d8ac18faf`) and **vswhere remote-routable, never cached**
  (`76ff2e49b`).
- **`download_file` declared_metadata HEAD fallback**: static.crates.io
  answers HEAD without Content-Length and ignores Range, so deferred
  materialization silently fails; fixups works around it with
  size_bytes= injection (`ci/add-archive-sizes.py`). Upstream fix:
  fall back to GET or treat missing metadata as non-deferrable.

## Not upstream material

- Dice persistence S1-S3, L0 watermark eviction (parked), dup-strings
  instrumentation: fork experiments; revisit only if the approach
  proves out long-term.
