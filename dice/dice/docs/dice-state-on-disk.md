# DICE state on disk

Fork document (gilescope/buck2, branch `giles-dice-persistence`), written with
an eye to upstream discussion.

Two features, one mechanism. **Persistence** writes the graph to disk so it
survives a daemon restart. **Pressure eviction** writes values to disk so RSS
survives a build. Both ride Meta's `DiceStorage` page-out path and its
content-addressed `DataKey` blobs; they differ only in what triggers the write
and what comes back.

| | persistence | pressure eviction |
| -------- | ----------------------- | ----------------------------- |
| trigger | daemon shutdown / start | RSS crosses a watermark |
| writes | graph skeleton + values | values only |
| boundary | cross-process | in-process |
| gate | side-effect **denylist** | key-type **allowlist** |
| status | shipped, S1-S3 | shipped, parked (default off) |

The denylist and the allowlist are orthogonal and are the thing people most
often conflate: the denylist guards hydration across a *process* boundary; the
allowlist picks what is worth evicting *within* one.

## Why

DICE state lives only in the daemon's memory. Long-lived daemons (Meta's
deployment) never notice; ephemeral-CI daemons pay full parse + configure +
analysis every run - a cost no action cache can touch, because it scales with
graph size rather than with what changed.

Measured (2026-07-05): on a flat third-party rig (2,181 alias targets) the whole
graph stack is cheap - parse 1.1s / configure 0.6s / analysis 9.9s cold, 13.9s
total after a daemon kill. **Small flat graphs are not the payoff.** The
motivating workload is large first-party graphs: substrate-scale builds (~1M
edges) measured at ~6 minutes of graph computation in Bazel. That is the shape
Bazel built Skycache for, and the shape this targets.

## The key observation

Persistence is **not a new invalidation algorithm**. DICE is already a versioned
incremental graph: injected leaves bump versions when they change, dependents
are transitively dirtied, requests re-validate lazily. That is partial
invalidation; it just doesn't survive a restart. So:

1. Serialize the versioned graph - `(key, value, deps, verified-at)` per node.
2. On load, treat it as a prior version universe.
3. Recompute the injected leaves against reality. **The diff is the dirty
   frontier.**
4. DICE's existing transitive dirtying does the rest.

All-or-nothing behaviour is avoided by construction: a one-file change
invalidates that file's dependents and nothing else, exactly as in a warm
daemon.

## Golden rules

The part that must not break. Each of these was learned the hard way; the
citation is where it bit.

**A key whose compute has side effects on per-daemon state must never be served
clean across a process boundary.** `BuildKey`'s compute declares action outputs
to the deferred materializer's in-memory artifact tree. A dice-clean `BuildKey`
in a fresh daemon skips that; the RE uploader then applies "not in tree =>
assume source" and opens paths that `materializations=none` never wrote. This
was the LPTF upload failure. Hence the default denylist - `EvalImportKey`
(hydrate panic), `BuildKey`, `EnsureProjectedArtifactKey`,
`EnsureTransitiveSetProjectionKey`. Re-executing them is cheap: real actions
AC-hit.

**`ForceDirtyHistory` round-trips verbatim.** Two runtime mechanisms enforce
force-dirty boundaries and both are defeated by one missing marker: `at_version`
emits `CheckDeps` only when `restricted_range(v)` contains the previously
verified version (a gap widens the window, permitting `CheckDeps` where
`Compute` is required), and `on_computed` intersects `valid_deps_versions` with
the restricted range (a gap makes the intersect a no-op). Upstream's
`check_that_force_dirty_does_not_get_forgotten_after_later_computes` covers the
in-memory case; the persistence suite must keep a round-trip variant.

**rdeps are re-derived on load, never persisted.** They are load-bearing for
invalidation - a leaf change propagates dirt through them - so a loaded graph
without them would serve stale values. The loader rebuilds every edge from the
persisted dep lists.

**A snapshot is a cache, never truth.** The header carries an opaque
`inputs_digest` (the embedder hashes binary + config reality into it). Any
mismatch, unknown type tag or decode failure is a *silent cold start*.
Corruption costs time, never correctness.

**Eviction is checked, not blind (TOCTOU).** Mid-build a node can be recomputed
between serialization and the evict message landing on the core-state thread;
paging out blindly would pair the new value's node with stale on-disk bytes.
Evict messages carry the serialized value and the state thread pages out only on
pointer match.

**`page_out` keeps its idle-only contract.** Pressure eviction is an additive
entry point with no idle requirement, not a relaxation of the existing one.

**A value is paged out at most once.** Upstream's invariant
(`PagableNodeValue::after_recompute`): a value paged back in becomes
`Recomputed` and is never a page-out candidate again, and its previous
`DataKey` is no longer tracked. Do not reintroduce that tracking to make
re-eviction free - it is a deliberate upstream choice.

**Values are immutable post-freeze**, which is what makes all of the above safe
to do while readers hold `Arc`s.

## How it works, as built

Values ride the existing page-out path unchanged. This adds the graph
*skeleton*: erased keys via the `pagable_typetag` registry, dep edges as record
ordinals, `VersionRanges` and `ForceDirtyHistory` as bincode metadata in a
`dice-graph.meta` file beside the store, canonically sorted so save/load/save is
byte-identical (pinned by a test).

- Entry points: `Dice::save_persisted_snapshot` / `load_persisted_snapshot`,
  both in `dice/dice/src/persist_ext.rs`; the codec and driver live in
  `dice/dice/src/persist.rs`.
- Load reinstalls nodes paged-out (values hydrate lazily on demand) and injected
  leaves hydrated - they are the baseline the first command's `changed_to` calls
  diff against - then re-derives rdeps and fast-forwards the version counter. On
  a successful load the `set_none_*` seeding is skipped, so no spurious `None`
  interlude dirties the graph.
- Projections persist as `(typetag blob, base ordinal)` sorted after all plain
  keys, resolved in one forward pass at load.
- Wiring: load at `Dice` construction (`BUCK2_DICE_DB_PATH` +
  `BUCK2_DICE_SNAPSHOT_PATH`), save via `buck2 debug hydration page-out`. Reuse
  gate: blake3(buck2 revision, `BUCK2_DICE_SNAPSHOT_SEED`).
- Pressure eviction: `Dice::evict_under_pressure(max_values, allowed_key_types)`
  in `dice/dice/src/pressure.rs`, plus a watcher in `configure_dice.rs`. Knobs
  (env, armed only with `BUCK2_DICE_DB_PATH` set): `BUCK2_DICE_EVICT_HIGH` /
  `_LOW` (bytes, K/M/G; LOW defaults to 75% of HIGH), `_POLL_SECS` (10),
  `_CHUNK` (4096), `_ALLOW` (`AnalysisKey`).

### The analysis dodge

The fork's original contribution, and why the starlark-heap problem never had to
be solved. **Do not serialize providers at all.** Persist the derived action
graph - actions and artifacts, which are stable serializable structures - keyed
by configured-target fingerprint. Execution only needs actions. Providers are
consumed in exactly two places: deriving a target's own actions (already
derived), and analysis of *dependents* - but a dependent needing re-analysis is
dirty, and frontier invalidation already guarantees dirty subgraphs recompute
fully. Clean subgraphs never re-materialize their providers because nothing
clean ever asks for them.

As built, `AnalysisValueSerialize` strips the frozen heap
(`actions_only_for_persist`) when snapshot mode is active. Every provider/tset
accessor already returns `Result`, so a provider read on a persisted-clean node
is a clean "missing analysis storage" error, never a panic. Builds that only
need actions - the code-change CI case - never read providers. Queries
(`cquery -a`, provider inspection) on such nodes degrade to recompute:
acceptable, correct, measurable.

### Two counts, why not bytes

Pressure eviction is paced by value count, not `target_bytes`. Per-value
resident size is not measurable pre-L2: values live behind `Arc`s and share
arenas, so allocative's unique-ownership walk reports 0 and serialized size
mis-states resident bytes. The watcher is the controller instead - evict a
chunk, purge jemalloc, re-measure RSS, repeat until under the low watermark.

## What we measured

Rates (NixOS x86, warm cache, single stream):

- serialize (page-out) ~250MB/s - the full 1.4GB graph in ~5s
- hydrate (page-in) ~60MB/s, ~3,600 values/s, ~0.5-1ms per analysis value:
  two to three orders cheaper than recompute
- byte redundancy in serialized analysis values ≥3.3:1 (zstd-19, 8MB window - a
  *lower* bound; global dedup sees more)
- serialized `AnalysisKey` ≈145KB, about 3× the 47KB arena estimate: page-out
  I/O is fatter than planned

Attribution on a real 17.8k-command build (2.9GB-resident daemon):

- 812MB frozen Starlark heaps (17.3k `AnalysisKey` ≈47KB each) plus most of a
  1.29GB untyped residual of the same shape
- ~400MB action/artifact graph; 117MB deferred materializer; 78MB TSet
  projections; 53MB dice graph shape

Two structural causes: arena-granular retention (one live ref pins a whole
arena; no post-freeze GC) and independent construction of equal values (every
module allocates its own `"-Copt-level=3"`; sharing is by pointer only for
values passed *between* modules). In the hetero sweep this triples across three
target platforms and crests at the build tail - 9GB observed, swap exhausted,
one runner OOM.

### The L0 verdict: parked, and why

A two-leg analysis-only A/B (34.6k `AnalysisKey`s stacked in one daemon):

- baseline peak RSS 4.46GB, 12:12 wall
- single-sweep eviction: 17,305/34,635 paged out - exactly leg 1's cold half, so
  *selection works* - wall in noise, but peak **5.99GB**: the sweep's
  serialization transients (blob slots + WAL, 2.5GB serialized) stack on the
  still-resident graph. An OOM accelerant at exactly the wrong moment.
- sub-batched (256/call): peak 7.15GB, leg 2 +3.5min - dribbled eviction
  thrashes against cross-platform-shared values leg 2 reads straight back.

**The disqualifying fact is topology, not tuning.** In the real hetero sweep
each leg is its own daemon running one build command, and the transaction's
`SharedCache` pins every value that command computes. During the actual 9GB
crest, L0 has zero candidates. The multi-leg-per-daemon scenario it does address
does not occur in CI.

The code stays, dormant and env-gated. The crest's cheap fixes are operational
(per-daemon memory caps, leg staggering); the deep fix is `SharedCache` release
surgery, unfunded.

## What's next

The tail-memory ladder, with honest status:

- **L0 watermark eviction** - shipped, parked (above).
- **L1 freeze-time string interning** - *dead*. Instrumenting it
  (`buck2 debug hydration dup-strings`) taught us a single `FrozenHeap` already
  interns its own strings, so all duplication is cross-heap - and the ceiling
  measured only 6.4MB. Measurement before optimisation earned its keep here.
- **L2 hash-consing at freeze** - the only live lever for the in-command crest:
  it shrinks resident bytes *while values are pinned*, which is exactly what
  pinning stopped L0 from doing. Content-address the frozen object graph so
  equal subtrees share one allocation globally; same idea as the CAS and the
  engine's name-independent action keys, applied in memory. Cost ~1-3s CPU per
  full build; floor on the win ≥3.3:1. Risk: identity-sensitive code - audit
  starlark `ptr_eq` uses; collisions handled by full compare on hash match. It
  is also the largest fork-drift item, so whether it pays is an explicit
  cost/benefit call, not a given. Get a second opinion.
- **L3 mmap-backed frozen heaps** - north star. `FrozenValue` pointers become
  arena-relative offsets, freeze writes the on-disk format directly, arenas are
  mmap'd. Retrieval becomes a page fault; serialization CPU is abolished because
  the arena *is* the format; the OS page cache replaces L0's policy entirely.
  Composes with L2. Weeks of starlark-rust value-representation surgery; design
  doc first.

Open questions on the persistence side, ranked:

- **HIGH - `EvalImportKey` blocked.** `NativeFunc`/`NativeMeth` carry
  `#[starlark_pagable(skip = unimplemented())]`, and every `.bzl` exports native
  functions, so serializing at `FrozenModule` level panics. The parse-layer win
  is blocked until those fields are handled - either stored as `Static`
  references (they are always in the globals heap) or the skip closure fixed.
- **HIGH - `ForceDirtyHistory` property test.** The `restricted_range`
  invariant deserves proptest coverage: for any sequence of force-dirty
  versions, a round-trip preserves `restricted_range(v)` for all sampled v.
- **MEDIUM - dirty-frontier cascade size.** If a buckconfig change invalidates
  thousands of injected leaves, a load is effectively a cold start. Profile a
  real buckconfig change before committing to CI; if it is too large, consider
  coarser injected keys (one per buckconfig section rather than per property).
- **MEDIUM - `ConfigurationHash` stability.** `DefaultHasher` (SipHash) is
  deterministic within a process and Rust version, but the stdlib guarantees
  nothing across versions. Cross-binary reuse is gated off by the header, so
  this matters only for standalone cache keys - deferred with S4.
- **LOW - bincode layout stability.** `VersionRanges` and `SeriesParallelDeps`
  live in dice core and may evolve; bincode is sensitive to field reordering and
  enum variant changes. `SNAPSHOT_SCHEMA_VERSION` is the escape hatch - bump it
  on any change and force a cold start.

## Non-goals

- Cross-binary-version reuse.
- Sharing persisted state between differently-configured checkouts.
- Serializing starlark heaps (explicitly dodged; revisit only if query workloads
  on evicted nodes prove hot).

## Prior art and history

Bazel's Skycache ("remote analysis caching", 2024-25) does frontier-based
serialization of Skyframe values and validates the same way; its codec-registry
shape maps onto what buck2 needed. `dice/read_dump` and the introspection layer
already serialize the graph *shape* for debugging - the walker half of a
persister. `DiceIncrementalityAlgorithms.pdf` in this directory formalizes the
invalidation model the loader reproduces.

This file replaces `persistence_plan.md`, `persistence_impl.md` and
`tail_memory_plan.md`. Those held the staged S0-S4 plan, a pre-implementation
design that did not ship (a four-file `persist/` module, a `postcard` codec, a
`with_persisted_state` builder entry point), per-key layer inventories with line
numbers that rotted, and the full L0 A/B write-up. They are in git history if
the reasoning is ever needed; everything load-bearing was carried across.
