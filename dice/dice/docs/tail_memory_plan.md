# Tail-memory plan: bounding and shrinking frozen analysis heaps

Status: L0 implemented 2026-07-14 (see § L0 "As shipped"); L1-L3 planned.
Attribution + rates measured 2026-07-14 (see `persistence_impl.md`
§ "Tail-memory attribution"). Owner branch: `giles-dice-persistence`.

## Problem

Daemon RSS peaks at the build tail (hetero sweep: 2.3GB steady → 9GB,
swap exhausted, one runner OOM — buck2-fixups run 29232220897). The
retained bytes are dominated by frozen Starlark analysis heaps:

- 812MB `FrozenFrozenHeap` arenas + most of a 1.3GB untyped residual
  (17.3k AnalysisKey × ~47KB), single-platform 17.8k-command specimen
- ~400MB action/artifact graph; 117MB materializer; 78MB TSets
- ×3 target platforms in the hetero sweep, cresting when the last
  leg's top-of-DAG values land

Two structural causes: arena-granular retention (one live ref pins a
whole arena, no post-freeze GC) and independent construction of equal
values (every module allocates its own `"-Copt-level=3"`; sharing is
by pointer only for values passed BETWEEN modules).

## Measured rates (NixOS x86, warm cache, single stream)

- serialize (page-out): ~250MB/s — full 1.4GB graph in ~5s
- hydrate (page-in): ~60MB/s, ~3,600 values/s → ~0.5-1ms per analysis
  value; 2-3 orders cheaper than recompute
- byte redundancy in serialized analysis values: ≥3.3:1 (zstd-19,
  8MB window — a LOWER bound; global dedup sees more)
- CI asymmetry: the driver box's cores idle while the fleet compiles —
  eviction CPU rides surplus capacity

## L0 — watermark eviction during the build (days)

Bound RSS; ends the OOM class. No new serialization code: S1-S3's
DiceStorage page-out/page-in is the mechanism.

- A new targeted entry point with no idle requirement (`page_out`
  itself keeps its idle-only contract):
  `evict_under_pressure(max_values, allowed_key_types)` — candidate
  collection and eviction run as core-state-thread messages (all graph
  mutations already serialize there — no new locking).
- Eviction policy: allowed key types only (AnalysisKey first; the
  snapshot side-effect denylist — BuildKey & co — is orthogonal and
  untouched: it guards CROSS-PROCESS hydration, not in-process
  eviction). Coldest-first; skip values pinned by an active
  transaction's cache or with a pending rdep task (see "As shipped").
- Trigger: watermark check on /proc/self RSS every N seconds;
  hysteresis — trigger at H, evict down to L (default L = 75% of H) to
  prevent thrash.
- Values are immutable post-freeze: cache serialized bytes beside the
  arena so re-eviction of a hydrated value writes nothing.
- Acceptance: hetero sweep lap with rig in scope holds daemon RSS
  under the watermark; STARVED/vitals lines show no thrash (hydrations
  per minute bounded); wall time within noise of baseline.

### As shipped (2026-07-14)

`Dice::evict_under_pressure(max_values, allowed_key_types)` + a daemon
watcher in `configure_dice.rs`. Deviations from the sketch above, and
what the field taught us:

- **Count-paced, not `target_bytes`.** Per-value resident size is not
  measurable pre-L2: values live behind `Arc`s and share arenas, so
  allocative's unique-ownership walk reports 0 and serialized size
  mis-states resident bytes. The watcher is the controller instead:
  evict a chunk (`BUCK2_DICE_EVICT_CHUNK`, default 4096), purge
  jemalloc, re-measure RSS, repeat until under the low watermark.
- **Pinned filter, not just rdeps.** A transaction's `SharedCache`
  retains every completed value for the transaction's lifetime, so
  evicting a node referenced by any active version frees nothing.
  Candidates exclude keys in any active cache (pending or completed);
  the uncomputed-rdeps guard additionally skips values whose rdep task
  is pending. Net effect on the hetero sweep: legs 1..n-1 are evictable
  while leg n runs - RSS bounded at steady + one leg.
- **Checked eviction (TOCTOU).** Mid-build, a node can be recomputed
  between serialization and the evict message; blind `set_paged_out`
  would pair the new value with stale bytes. Evict messages carry the
  serialized value; the state thread pages out only on pointer match.
- Coldness rank: begin of the last verified range (DICE tracks no
  access times); re-eviction of a hydrated value reuses its `DataKey`
  (no re-serialization), as planned.

Knobs (all env, watcher armed only with `BUCK2_DICE_DB_PATH` set):
`BUCK2_DICE_EVICT_HIGH` / `_LOW` (bytes, K/M/G suffix; LOW defaults to
75% of HIGH), `_POLL_SECS` (10), `_CHUNK` (4096), `_ALLOW`
(`AnalysisKey`).

### Measured verdict (2026-07-14, local M-series specimen) — PARKED

Two-leg analysis-only A/B (`//third-party:` default + t-linux platforms,
34.6k AnalysisKeys stacked in one daemon, `fs_hash_crawler` watcher so
legs actually stack; notify drops its cursor between commands and wipes
the graph):

- Baseline: peak RSS 4.46GB, 12:12 wall.
- L0, single-sweep eviction: 17,305/34,635 AnalysisKeys paged out
  (exactly leg 1's cold half - selection works), wall in noise, but
  peak 5.99GB: the sweep's serialization transients (blob slots + WAL;
  2.5GB serialized) stack on the still-resident graph. An OOM
  accelerant at exactly the wrong moment.
- L0, sub-batched (256/call): peak 7.15GB, leg 2 +3.5min - dribbled
  eviction thrashes against cross-platform-shared values leg 2 reads
  straight back.
- Serialized AnalysisKey ≈ 145KB (2.5GB / 17.3k), ~3× the 47KB arena
  estimate - page-out I/O is fatter than planned.

The disqualifying fact is topology, not tuning: in the real hetero
sweep each leg is its own daemon running ONE build command, and the
transaction's SharedCache pins every value the running command
computes. During the actual 9GB crest L0 has zero candidates. The
multi-leg-per-daemon scenario it does address does not occur in CI.

Status: code stays on this branch, dormant (env-gated, default off).
The rig knob wiring was reverted. The CI crest's cheap fixes are
operational (per-daemon memory caps, leg staggering); the deep fix is
SharedCache release surgery - unfunded.

## Where this leaves the ladder

- L1: dead (see gate 2 - 6.4MB ceiling).
- L0: parked (above).
- L2 is the only live lever for the in-command crest: hash-consing
  shrinks resident bytes even while values are pinned, which is what
  pinning-proofs L0 could not do. It is also the largest fork-drift
  item; whether it pays is now an explicit cost/benefit decision, not
  a given. The 145KB/value serialized size and ≥3.3:1 redundancy floor
  say the bytes exist; the maintenance bill says get a second opinion.

## L1 — freeze-time string interning (small starlark-rust change)

Shrink the arenas' dominant primitive. At freeze, strings consult a
shared frozen intern arena (sharded map, keyed by content hash);
duplicate strings across all modules collapse to one allocation.

- Cost: one fast hash per string inside the existing freeze walk
  (sub-second per full build); likely net-positive via cache locality.
- Risk: intern arena lifetime = daemon lifetime (monotone growth) —
  acceptable, it holds one copy of each distinct string; measure size.
- Acceptance: duplicate-string histogram (instrument first) shows the
  reclaimed fraction; arena bytes per AnalysisKey drop accordingly.

## L2 — hash-consing at freeze (the dedup ceiling)

Content-address the frozen object graph: bottom-up structural hash of
every frozen value; equal subtrees share one allocation globally.
Same idea as the CAS and the engine's name-independent action keys,
applied to in-memory values.

- Cost: ~1-3s CPU per full build (GB/s-class hashes over ~1.4GB),
  parallel per-module, sharded global table; partially recovered by
  skipped memcpy/allocations and 3.3×-smaller page-outs.
- Floor on the win: ≥3.3:1 measured; instrument exact duplicate-value
  histogram before building.
- Risk: identity-sensitive code (pointer equality as object identity)
  — audit starlark `ptr_eq` uses; hash collisions handled by full
  compare on hash match (hash-consing classic).
- Acceptance: resident analysis bytes shrink ≥2× on the specimen
  build; no behavioural diffs (frozen values are immutable — sharing
  is unobservable except via identity checks found in the audit).

## L3 — position-independent, mmap-backed frozen heaps (north star)

`FrozenValue` pointers become arena-relative offsets; freeze writes
the arena in its on-disk format; arenas are mmap'd files.

- Retrieval = page fault (µs); serialization CPU abolished (the arena
  IS the format); the OS page cache replaces L0's eviction policy.
- Composes with L2: content-addressed arena files dedup at rest and
  in memory (same mapping shared).
- Cost: starlark-rust value-representation surgery — weeks, not days;
  design doc first. L0-L2 land value independently and are not
  throwaway (L0's policy becomes a fallback; L1/L2's dedup carries
  over as smaller arenas to map).

## Order & measurement gates

1. L0 now — removes the OOM cliff; gates: no-thrash + wall-time-noise.
   [shipped 2026-07-14, see § L0 "As shipped"; acceptance lap pending]
2. Instrument duplicate histograms (strings, whole values) on the
   specimen build — one allocative-style walk; sizes L1 vs L2.
   [strings half shipped 2026-07-14: `buck2 debug hydration
   dup-strings` — a global weak registry of frozen heaps in
   starlark-rust (registered at freeze, pruned amortized) + a two-pass
   FNV-content-hash walk (`starlark::values::dup_string_stats`).
   Learned: a single FrozenHeap already interns its own strings, so
   ALL duplication is cross-heap — precisely what L1 collapses. The
   whole-value histogram is still open; its floor is the measured
   ≥3.3:1 serialized redundancy.]
3. L1 if strings dominate the histogram; L2 either way once L0 is
   stable. Re-run the specimen attribution after each layer.
4. L3 design doc after L2's numbers are in.
