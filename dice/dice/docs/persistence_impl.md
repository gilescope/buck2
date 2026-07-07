# DICE persistence: implementation plan

Complements `persistence_plan.md` (read that first for motivation and staged plan).
Status: **S1-S3 implemented** on this branch (see `dice/dice/src/persist.rs` and
`impls/tests/persist.rs`); this doc now records the as-built decisions where they
diverged from the pre-implementation design below.

## Invariant learned in the field (2026-07-07)

A DICE key whose compute has SIDE EFFECTS on per-daemon state must never
be served clean across a process boundary. The concrete case: BuildKey's
compute declares action outputs to the deferred materializer's in-memory
artifact tree; a dice-clean BuildKey in a fresh daemon skips that, and the
RE uploader then misclassifies undeclared outputs as source files ("not in
tree => assume source") and opens paths that materializations=none never
wrote. Hence the default snapshot denylist: EvalImportKey (hydrate panic),
BuildKey + EnsureProjectedArtifactKey + EnsureTransitiveSetProjectionKey
(materializer side effects). Re-execution is cheap: real actions AC-hit.

## As-built summary (2026-07-05)

- Values ride Meta's existing page-out path (`DiceStorage`, content-addressed
  `DataKey` blobs in sqlite); the persist module adds the graph skeleton: keys
  via the `pagable_typetag` registry, deps as record ordinals, VersionRanges +
  ForceDirtyHistory via bincode metadata (`dice-graph.meta`), canonically
  sorted for byte-stable output (save/load/save is byte-identical, tested).
- Load rebuilds nodes paged-out (values hydrate lazily on demand), reinstalls
  injected leaves hydrated as the diff baseline, re-derives rdep edges (they
  are load-bearing for invalidation), and fast-forwards the version counter.
  The daemon's normal per-command changed_to calls ARE the frontier diff; on a
  successful load the set_none_* seeding is skipped so no spurious None
  interlude dirties the graph.
- Wiring: load at Dice construction (BUCK2_DICE_DB_PATH +
  BUCK2_DICE_SNAPSHOT_PATH); save via `buck2 debug hydration page-out`.
  Reuse gate: blake3(buck2 revision, BUCK2_DICE_SNAPSHOT_SEED).
- Projections persist as (typetag blob, base ordinal) records sorted after all
  plain keys - single forward resolution pass at load.
- EvalImportKey values serialize but panic on hydrate (NativeFunc skipped
  slots), so BUCK2_DICE_SNAPSHOT_DENY (default "EvalImportKey") persists such
  types key-only. Injected leaves that cannot round-trip are dropped whole.
- S3 dodge as-built: `AnalysisValueSerialize` strips the frozen heap
  (`actions_only_for_persist`) when snapshot mode is active. All provider/tset
  accessors already return Result - a provider read on a persisted-clean node
  is a clean "missing analysis storage" error, never a panic. Builds that only
  need actions (the code-change CI case) never read providers.
- ConfigurationHash/SipHash: unchanged. Within one binary the hash is
  consistent, and cross-binary reuse is gated off by the header, so the Blake3
  rehash matters only for S4-style standalone cache keys - deferred with S4.

---

## 1. DICE state map

### 1.1 Per-node in-memory tuple

`OccupiedGraphNode` (`dice/dice/src/impls/core/graph/nodes.rs:356`):

| Field | Type | Persist? | Notes |
| ----- | ---- | -------- | ----- |
| `key` | `DiceKey` (u32) | as erased key | u32 is process-scoped; serialize `DiceKeyErased` content via `DiceKeyIndex::get(key)` |
| `value` | `PagableNodeValue` | yes (opt-in) | codec via `Key::value_serialize()` → postcard |
| `deps` | `Arc<SeriesParallelDeps>` | yes | serialize dep list as erased key content; remap on load via `DiceKeyIndex::index()` |
| `rdeps` | `LazyDepsSet` | **no** | accumulated live as computations run; start empty on load |
| `verified_ranges` | `Arc<VersionRanges>` | yes | cell history; must not extend past `V_persist` |
| `dirtied_history` | `ForceDirtyHistory` | **verbatim** | load-bearing invariant (see 1.3); losing any marker enables incorrect reuse |
| `invalidation_paths` | `TrackedInvalidationPaths` | **no** | reconstruct as `TrackedInvalidationPaths::clean()` on load |

`InjectedGraphNode` (`nodes.rs:724`): persist all `(VersionNumber, value)` pairs in
`values: SortedVectorMap<VersionNumber, InjectedNodeData>` (`nodes.rs:726`). Injected values
cannot be recomputed; they are the load-time source of truth for the dirty frontier.

### 1.2 Hook points

**Save** — at daemon shutdown, iterate `VersionedGraph::nodes` (`storage.rs:204`, `pub(crate)`).
For each node: `DiceKeyIndex::get(key)` → erased key bytes; `value_serialize()` → value bytes;
direct field access for `deps`, `verified_ranges`, `dirtied_history`. Save is inside the `dice`
crate (visibility constraint). Incremental delta writes (post-commit) are S2+.

**Load** — before the first transaction. Must run inside the `dice` crate via a new entry point:

```rust
// dice/dice/src/persist/loader.rs
pub(crate) fn load_persisted_state(
    snapshot: &DiceSnapshot,
    graph: &mut VersionedGraph,
    key_index: &DiceKeyIndex,
    global_version: &mut VersionNumber,
) -> DiceResult<Vec<InjectedLeaf>>
```

Step-by-step:

1. For each persisted `OccupiedGraphNode`: intern key via `DiceKeyIndex::index()`, remap dep
   `DiceKey` u32s, call `OccupiedGraphNode::new(...)` (`nodes.rs:482`), insert into
   `VersionedGraph::nodes` (direct field write; inside crate).
2. Set global version counter to `V_persist` (the max version in the snapshot).
3. Return the persisted `InjectedGraphNode` values as a list of `(key, value)` pairs.

Caller (integration layer) then calls:

```rust
updater.changed_to([(key, current_real_value), ...])?;
updater.commit().await;
```

`changed_to` handles both "node absent" (creates fresh `InjectedGraphNode`) and "node present"
(calls `on_injected`, propagates rdeps). The rdep BFS is vacuous on a fresh graph; the dirty
frontier materialises lazily as nodes are requested and find `CheckDeps` → dependency changed.

### 1.3 ForceDirtyHistory invariant

`ForceDirtyHistory` (`nodes.rs:399`) must be loaded verbatim. Two mechanisms enforce
force-dirty boundaries at runtime, both defeated by a missing marker:

- `at_version` (`nodes.rs:591`): emits `CheckDeps` only when
  `dirtied_history.restricted_range(v).contains(&prev_verified_version)`. A missing
  marker widens the window, allowing CheckDeps where Compute is required.
- `on_computed` (`nodes.rs:194`): intersects `valid_deps_versions` with
  `force_dirty_restricted_range`. A missing marker makes the intersect a no-op.

Test `check_that_force_dirty_does_not_get_forgotten_after_later_computes`
(`storage.rs:1545-1601`) covers this invariant. The persistence suite must add a
round-trip variant of the same test.

---

## 2. Codec registry design

### 2.1 Existing infrastructure (no new registry needed)

| Component | Where | Role |
| --------- | ----- | ---- |
| `#[pagable_typetag(dice::DiceKeyDyn)]` | per-key impl | inventory-collected at startup; sorted by name → stable u32 tag; key binary round-trip |
| `Key::value_serialize() -> impl ValueSerialize` | `dice/dice/src/api/key.rs:93` (method); trait at `pagable/src/value_serialize.rs:24` | per-key-type value codec via postcard |
| `PagableStorage` | `pagable/src/storage/traits.rs:95` | storage backend (SQLite/sled/noop) |
| `OccupiedGraphNode::set_paged_out` / `rehydrate` | `nodes.rs:539 / 533` | value-level page-out hooks |

Graph structure (deps, `VersionRanges`, `ForceDirtyHistory`) is not covered by value_serialize.
Add a thin postcard codec for those fields in `dice/dice/src/persist/graph_codec.rs` — plain data,
no trait needed; these types are already `serde::Serialize`-able or trivially so.

### 2.2 Opt-in marker trait

```rust
// dice/dice/src/persist/mod.rs

/// Marker for Key types that participate in the DICE snapshot.
/// Default impl = excluded.  Register in buck2 integration layer, not in dice core.
pub trait PersistableKey: Key {
    /// If false, the value bytes are omitted (graph structure still persisted for
    /// dep-edge fidelity); default false.
    fn persist_value() -> bool { true }
}
```

Registration in `app/buck2_common/src/persist.rs`:

```rust
impl PersistableKey for ReadFileKey {}
impl PersistableKey for ReadDirKey {}
impl PersistableKey for PathMetadataKey {}
impl PersistableKey for CellResolverKey {}
// etc.
```

No macro needed. `dice` crate stays generic: it calls `<K as PersistableKey>::persist_value()`
only through a `dyn PersistableKeyDyn` vtable registered via `inventory`, same shape as
`pagable_typetag`.

### 2.3 Snapshot file format

```text
[Header: postcard][zstd-compressed body]

Header {
    magic: [u8; 8]          = b"DICE\x00SNP\x00"
    schema_version: u32,    // bump on any structural change
    binary_hash: [u8; 32],  // sha256 of the buck2 binary
    buckconfig_digest: [u8; 32],
    cell_digest: [u8; 32],
    prelude_digest: [u8; 32],
    node_count: u64,
    max_version: u64,       // V_persist
}

Body: Vec<PersistedNode> (postcard)

PersistedNode {
    key_tag: u32,           // pagable_typetag index (stable within binary)
    key_bytes: Vec<u8>,     // postcard of DiceKeyErased content
    value_bytes: Option<Vec<u8>>,  // None if !PersistableKey::persist_value
    deps: Vec<(u32, Vec<u8>)>,     // (tag, key_bytes) pairs
    verified_ranges: Vec<(u64, Option<u64>)>, // [begin, end) intervals
    force_dirty_versions: Vec<u64>,
    is_injected: bool,
}
```

Any header field mismatch → cold start (log the mismatch at debug level). Any node decode
failure → drop that node, continue (corruption costs time, never correctness).

---

## 3. Layer inventory

Key types by persistence stage. `NoValueSerialize` keys are always excluded.

| Key type | Value type | File | Serializability | Stage |
| -------- | ---------- | ---- | --------------- | ----- |
| `ReadFileKey`, `ReadDirKey`, `PathMetadataKey`, `ExistsMatchingExactCaseKey` | file ops results | `buck2_common/src/file_ops/dice.rs:289,322,358,392` | trivial - plain data, `OkPagableValueSerialize` | S1 |
| `CellResolverKey` (injected), `LegacyExternalBuckConfigDataKey` (injected) | cell/config | `dice/cells.rs:62`, `legacy_configs/dice.rs:183` | trivial - `PagableValueSerialize` | S1 |
| `LegacyBuckConfigForCellKey`, `LegacyBuckConfigPropertyProjectionKey` | buckconfig | `legacy_configs/dice.rs:200,299` | trivial | S1 |
| `PackageListingKey`, `BuildfilesKey`, `PackageBoundaryExceptionKey` | package listing | `package_listing/dice.rs:44`, `buildfiles.rs:92` | trivial | S2 |
| `InterpreterResultsKey(PackageLabel)` | `EvaluationResult` | `interpreter/calculation.rs:63` | structured-effort - `Pagable`, no starlark heap | S2 |
| `EvalImportKey` | `LoadedModule` → `FrozenModule` | `calculation.rs:150` | **blocked** - `NativeFunc`/`NativeMeth` fields use `#[starlark_pagable(skip = unimplemented())]`; every .bzl exports native functions; panics on serialize | skip until fixed |
| `ConfiguredTargetNodeKey` | `ConfiguredTargetNode` | `configured/nodes.rs:1097` | structured-effort - derives `Pagable`, starlark-free | S2 |
| `ConfigurationNodeKey`, `PlatformConfigurationKey`, `ExecutionPlatformResolutionKey`, `TransitionKey` | configuration results | `configuration.rs:123`, `execution.rs:211`, `calculation_apply_transition.rs:260` | structured - `OkPagableValueSerialize` | S2 |
| `AnalysisKey(ConfiguredTargetLabel)` | `AnalysisResult` | `analysis/calculation.rs:89` | pagable impls wired; round-trip **untested**; dodge recommended (§4) | S3 (dodge) |
| `BuildKey(ActionKey)` | `ActionOutputs` | `actions/calculation.rs:792` | trivial - `BuckIndexMap<path, ArtifactValue>`, derives `Pagable` | S3 |
| `EnsureProjectedArtifactKey`, `EnsureTransitiveSetProjectionKey` | artifact groups | `artifact_groups/calculation.rs:304,497` | structured | S3 |
| `TestExecutionKey` | - | `buck2_test/src/orchestrator.rs:696` | `NoValueSerialize` - excluded permanently | never |
| `PoisonedDueToDetectedCycleKey` | - | `buck2_common/src/dice/cycles.rs:110` | `NoValueSerialize` - excluded permanently | never |

`EvalImportKey` exclusion shrinks S2's win: parse cost survives until the NativeFunc skip is
resolved. File ops + buckconfig (S1) still proves the end-to-end plumbing.

---

## 4. S3 action-graph persistence (the analysis dodge)

### 4.1 Concrete types

Persist `RecordedActions` rather than full `AnalysisResult`:

- `RecordedActions` = `Vec<ActionLookup>` (`app/buck2_build_api/src/actions/registry.rs:324`)
- Each `ActionLookup::Action(Arc<RegisteredAction>)` (`actions.rs:376`):
  - `key: ActionKey` (`artifact/src/actions/key.rs:39`) = `DeferredHolderKey + ActionIndex`
  - `action: Box<dyn Action>` - 8 concrete impls, all `#[pagable_typetag]` (confirmed)
  - `executor_config: Arc<CommandExecutorConfig>`

The dodge mechanism: `RecordedAnalysisValues` (`registry.rs:673`) has
`analysis_storage: Option<OwnedFrozenValueTyped<StarlarkAnyComplex<FrozenAnalysisValueStorage>>>` (`registry.rs:676`).
When `None`, the struct contains only `RecordedActions` - the provider heap is absent.
`testing_new_actions_only` (`registry.rs:685`) already sets this to `None`. A new
`RecordedAnalysisValues::actions_only_for_persist()` constructor uses the same path.

`AnalysisKey::equality` returns `false` (`calculation.rs:114`). Cross-process warm-start
reuses nodes via `CheckDeps`, not equality - no change needed.

### 4.2 Fingerprint

`ConfiguredTargetLabel` = `TargetLabel(pkg, name)` + `Configuration` (interned
`ConfigurationPairData { cfg, exec_cfg }`). `ConfigurationHash` is a 16-hex u64 from
`DefaultHasher` (SipHash) at `configuration/data.rs:416-421` - NOT Blake3 (the dossier
misattributed the algorithm). SipHash is process-deterministic within a Rust version but
not guaranteed stable across Rust releases; use a Blake3 rehash of the `ConfigurationHash`
string for S3 cache keys.

Stable fingerprint string:

```text
"{cell}//{pkg}:{name}#{cfg_output_hash_blake3_rehash}"
```

This is sufficient as an S3/GH-cache key within one binary version. Cross-version reuse is
a non-goal (§5).

### 4.3 Starlark verdict

`Box<dyn Action>` is fully serializable. `RunAction` holds
`OwnedFrozenValueTyped<FrozenStarlarkRunActionValues>` (`run.rs:403`);
`FrozenStarlarkRunActionValues` derives `StarlarkPagable` (`run.rs:301`) and contains command
args (strings, artifact handles, concat lists) - none of which are `NativeFunction` values.
The unimplemented() panic in `NativeFunc`/`NativeMeth` (starlark `function.rs:91,255`) is in
the .bzl module heap, not in action values. Action persistence does not trigger it.

Full `AnalysisResult` persistence (providers included) is mechanically plumbed but has no
round-trip integration test. Treat as "wired, not proven" and keep it behind the dodge for S3.

---

## 5. Lessons from Skycache and Gradle

### Adopt

| Lesson | Source | Application |
| ------ | ------ | ----------- |
| Shared-reference deduplication | Bazel Skycache (10x blowup on nested sets) | `SeriesParallelDeps` uses `Arc` for structural sharing in-memory; the serialized dep list must deduplicate repeated subtrees via an interning table in the snapshot writer. Add this before S3 (analysis graphs have larger dep lists). |
| Diagnostics early | Gradle configuration cache HTML report was the adoption unlock | Extend `buck2 debug dice-dump` to show which nodes are snapshot-eligible, which are excluded and why (NoValueSerialize / EvalImportKey block / etc.). Add before S1 ships. |
| Audit store-time inputs | Gradle 8.4 — inputs accessed *while storing* caused silent false hits | The snapshot writer must record which injected leaves it reads during serialization and include them in the header digest. |

### Skip

| Pattern | Why |
| ------- | --- |
| Cross-version reuse | Binary-hash-strict header; any mismatch → cold start. Relaxing to schema-versioned is S4+. |
| Remote graph topology service | S4 transport rides existing GH-cache machinery; no new service. |
| Full provider serialization | Dodged for S3; revisit only if `cquery`/provider-inspection on evicted nodes proves hot. |
| All-or-nothing invalidation | Gradle's weakness; DICE dirty-frontier avoids it by construction. |

---

## 6. S1 first-PR outline

### File-level touch list

| File | Change |
| ---- | ------ |
| `dice/dice/src/persist/mod.rs` (new) | `PersistableKey` marker trait; `SnapshotHeader`; `save_snapshot()`; `load_persisted_state()` entry point |
| `dice/dice/src/persist/graph_codec.rs` (new) | postcard serde for `VersionRanges`, `ForceDirtyHistory`, `SeriesParallelDeps` dep lists |
| `dice/dice/src/persist/loader.rs` (new) | `OccupiedGraphNode`/`InjectedGraphNode` construction from persisted data; key remapping via `DiceKeyIndex::index()` |
| `dice/dice/src/persist/header.rs` (new) | `SnapshotHeader` struct; magic bytes; validation logic |
| `dice/dice/src/impls/storage.rs` | add `pub(crate) fn iter_nodes_for_persist(&self) -> impl Iterator<Item=(DiceKey, &VersionedGraphNode)>` |
| `dice/dice/src/impls/core/graph/nodes.rs` | add `pub(crate) fn new_from_persisted(...)` constructors for `OccupiedGraphNode` and `InjectedGraphNode` |
| `dice/dice/src/lib.rs` | expose `pub fn with_persisted_state(snapshot_path: &Path) -> DiceBuilder` entry point |
| `app/buck2_common/src/persist.rs` (new) | `impl PersistableKey for` ReadFileKey, ReadDirKey, PathMetadataKey, ExistsMatchingExactCaseKey, CellResolverKey, LegacyExternalBuckConfigDataKey, LegacyBuckConfigForCellKey, LegacyBuckConfigPropertyProjectionKey |
| `app/buck2/src/commands/` | `--unstable-dice-save <path>` flag (write at daemon shutdown); `--unstable-dice-load <path>` flag (load before first build, silent cold-start on header mismatch) |

### New flags

```text
--unstable-dice-save <path>   Write DICE snapshot at daemon shutdown
--unstable-dice-load <path>   Load snapshot before first build; silent cold-start on mismatch
```

Both gated by `--unstable-` prefix. Production use requires neither; CI scripts opt in explicitly.

### Test plan

| Test | Type | What it proves |
| ---- | ---- | -------------- |
| `round_trip_occupied_node` | unit | `OccupiedGraphNode` survives postcard encode/decode with all fields equal |
| `round_trip_injected_node` | unit | `InjectedGraphNode` value list survives |
| `force_dirty_preserved` | unit | `ForceDirtyHistory` round-trip: `restricted_range(v)` identical before and after |
| `key_remap_on_load` | unit | Keys interned in different order in a fresh `DiceKeyIndex`; dep edges remap correctly |
| `header_mismatch_cold_start` | unit | Mismatched binary hash triggers cold start, no panic |
| `node_decode_failure_continues` | unit | Corrupted node bytes → that node dropped, remainder loaded |
| `s1_warm_start_file_key` | integration | Build, save snapshot, start fresh daemon with `--dice-load`, assert `ReadFileKey` node is `Match` for unchanged files |
| `s1_dirty_frontier` | integration | Build, modify one file, save snapshot, load, assert only that file's dependents recompute |
| **`determinism`** | integration | Build → save A → load → save B: `sha256(A) == sha256(B)`. Catches undeclared deps and non-deterministic serialization. This test is mandatory before S1 merges. |

### Estimated size and timing

S1 scope: file ops + buckconfig keys. For a 100k-node graph (typical for a large
third-party sweep):

- Key string avg ~60 bytes, deps avg 5 × 8 bytes, versions ~16 bytes, force-dirty ~4 bytes
- Raw: ~120 bytes/node × 100k ≈ 12 MB
- After zstd level 3: ~2-4 MB
- Load time: O(n) `DiceKeyIndex::index()` hash lookups + postcard decodes ≈ <500 ms for 100k nodes

S1 win is correctness proof, not wall-clock reduction. The ~2 min savings comes in S2 (parse/configure) and S3 (analysis).

---

## 7. Open questions ranked by risk

**HIGH - access to `VersionedGraph::nodes`**
`nodes` is `pub(crate)` (`storage.rs:204`). The persister must live inside the `dice` crate.
The new `persist/` module satisfies this; the buck2 integration layer calls `dice`-exposed
entry points. If the persister ever needs to move to `app/`, a new internal API surface is
required. Decision: keep it inside `dice` for S1-S3.

**HIGH - `ForceDirtyHistory` property test**
The `restricted_range` invariant is the key correctness guarantee. Need a property-based test
(proptest or similar): for any sequence of force-dirty versions, round-trip through postcard
preserves `restricted_range(v)` for all v in a sample set. Add alongside `force_dirty_preserved`.

**HIGH - `EvalImportKey` blocked**
`NativeFunc`/`NativeMeth` carry `#[starlark_pagable(skip = unimplemented())]`
(`starlark-rust/starlark/src/values/types/function.rs:91,255`). Any real .bzl module serialized
at the `FrozenModule` level panics. Parse-layer (S2) win is blocked until these fields are
handled - either by storing them as `Static` references (they are always in the globals heap)
or by fixing the skip closure. Resolution is a prerequisite for S2, not S1.

**HIGH - pagable compilation error**
`pagable/src/impls/collections.rs:250`: `impl<T, const N: usize> PagableSerialize for SmallVec<[T; N]>` is missing `where [T; N]: smallvec::Array`. Build fails with `cargo test --package starlark --features pagable`. Must fix before any integration test touches `SmallVec`-containing types. Check whether the fix requires a `smallvec` version bump or just a where-clause addition.

**MEDIUM - `CheckDeps` correctness on fresh load**
After cross-restart reload, rdeps are empty. The dirty frontier propagates lazily as nodes are
requested (not eagerly via BFS). Need an integration test that builds, saves, loads, then
requests a node transitively downstream of a changed leaf - confirming the full `CheckDeps`
chain fires, not just the direct dep. The `s1_dirty_frontier` test above covers this.

**MEDIUM - `ConfigurationHash` stability**
`DefaultHasher` (SipHash) is deterministic within a process and Rust version but the stdlib
does not guarantee cross-version stability. S3 fingerprints must be computed via Blake3 over
the `ConfigurationHash` string, not stored as raw SipHash u64.

**MEDIUM - dirty-frontier cascade size**
If a buckconfig change invalidates thousands of injected leaves, the snapshot load effectively
becomes a cold start. Profile the cascade size on a real buckconfig change before committing
S3 to CI. If too large, consider coarser-grained injected keys (one key per buckconfig section
rather than per-property).

**LOW - postcard `VersionRanges` / `SeriesParallelDeps` stability**
postcard format is stable for fixed-layout structs but sensitive to field reordering and enum
variant changes. Both types are in `dice` core and may evolve. The schema_version field in
`SnapshotHeader` is the escape hatch: bump it on any change to these types and force a cold
start. Document this in a comment on `SNAPSHOT_SCHEMA_VERSION`.

**LOW - rdep rebuild on first post-load build**
rdeps start empty after load. The first build cannot benefit from rdep-based early-exit on
dirty propagation (the invalidation BFS has no edges to follow). Cost is bounded: the first
build re-accumulates rdeps as it revalidates nodes, same as a warm daemon after a clean
invalidation. No action required for S1; revisit if profiling shows the first-build penalty
is non-trivial.
