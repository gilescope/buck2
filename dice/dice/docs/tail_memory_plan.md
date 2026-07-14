# Tail-memory plan: bounding and shrinking frozen analysis heaps

Status: planned. Attribution + rates measured 2026-07-14 (see
`persistence_impl.md` § "Tail-memory attribution"). Owner branch:
`giles-dice-persistence`.

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

- Remove `Dice::page_out`'s idle-only restriction for a new targeted
  entry point: `evict_under_pressure(target_bytes)` runs as a
  core-state-thread message (all graph mutations already serialize
  there — no new locking).
- Eviction policy: allowed key types only (AnalysisKey first; the
  snapshot side-effect denylist — BuildKey & co — is orthogonal and
  untouched: it guards CROSS-PROCESS hydration, not in-process
  eviction). Coldest-first; NEVER evict a value with uncomputed rdeps.
- Trigger: watermark check on the existing memory tracker (or
  /proc/self RSS) every N seconds; hysteresis — trigger at H, evict
  down to L (e.g. 60% / 45% of box RAM) to prevent thrash.
- Values are immutable post-freeze: cache serialized bytes beside the
  arena so re-eviction of a hydrated value writes nothing.
- Acceptance: hetero sweep lap with rig in scope holds daemon RSS
  under the watermark; STARVED/vitals lines show no thrash (hydrations
  per minute bounded); wall time within noise of baseline.

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
2. Instrument duplicate histograms (strings, whole values) on the
   specimen build — one allocative-style walk; sizes L1 vs L2.
3. L1 if strings dominate the histogram; L2 either way once L0 is
   stable. Re-run the specimen attribution after each layer.
4. L3 design doc after L2's numbers are in.
