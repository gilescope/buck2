# DICE persistence: a plan

Status: design proposal, unimplemented. Fork planning document
(gilescope/buck2), written with an eye to upstream discussion.

## Why

DICE state lives only in the daemon's memory. Long-lived daemons (Meta's
deployment) never notice; ephemeral-CI deployments pay full parse +
configure + analysis on every run. Concrete numbers from a distributed-RE
sweep of ~2,185 third-party crates on GitHub Actions runners
(gilescope/rebuck's rebuck2 engine): with a warm remote action cache the
build phase is ~97% cache hits, and the dominant residual cost is the
~2 minutes of cold-daemon graph computation that no action cache can touch.
Persistence turns "recompute the world's shape" into "reload it and check
what moved".

## The key observation: the algorithm already exists

DICE is a versioned incremental graph (`VersionNumber`,
`VersionedGraph`): injected leaves (file digests, config values) bump
versions when they change; dependents are transitively dirtied; requests
re-validate lazily, reusing any node whose recorded deps are clean. That IS
partial invalidation — it just doesn't survive a restart.

Persistence is therefore **not a new invalidation algorithm**. It is:

1. Serialize the versioned graph: `(key, value, deps, verified-at)` per
   node, for key types that opt in.
2. On load, treat the persisted state as a prior version universe.
3. Recompute the injected leaves against reality (file digests, buckconfig,
   cell layout). The diff is the dirty frontier.
4. DICE's existing transitive dirtying + lazy re-validation does the rest:
   every clean subgraph is reused, every dirty one recomputes exactly as it
   would have in-memory.

All-or-nothing behaviour is thus avoided by construction: a one-file change
invalidates that file's dependents and nothing else, same as a warm daemon.

Prior art: Bazel's Skycache ("remote analysis caching", 2024-25) does
frontier-based serialization of Skyframe values and validates the same way.
Their codec-registry approach maps directly onto what buck2 would need.

## The hard part: serialization, not invalidation

DICE values are `Arc<dyn Any>`-shaped; DICE itself cannot serialize them.
A persister needs a per-key-type codec registry at the buck2 integration
layer (dice stays generic; buck2 registers codecs for the key types it
wants persisted — exactly Bazel's shape). Layers by difficulty:

| layer | value types | serializability |
| ----- | ----------- | --------------- |
| file hashes, dir listings, package listings | plain data | trivial |
| parse results (starlark ASTs) | structured | plausible |
| unconfigured/configured TargetNode | mostly attr literals | with effort |
| analysis results (providers) | frozen starlark heaps | the boss fight |

### The analysis dodge (proposed original contribution)

Do not serialize providers at all. Persist the **derived action graph**
(actions + artifacts, which are stable, serializable structures) keyed by
configured-target fingerprint. Execution only needs actions. Providers are
consumed in exactly two places: (a) deriving a target's own actions —
already derived; (b) analysis of *dependents* — but a dependent that needs
re-analysis is dirty, and frontier invalidation already guarantees dirty
subgraphs recompute fully (re-running analysis for the dirty node's deps as
needed via normal DICE demand). Clean subgraphs never re-materialize their
providers because nothing clean ever asks for them.

Consequence: the starlark-heap serialization problem is bypassed for the
build path. Queries (`cquery -a`, provider inspection) on clean-but-evicted
nodes degrade to recompute — acceptable, correct, and measurable.

## Trust and safety

- Persisted state is a cache, never truth: header carries a schema
  version, the exact buck2 binary hash, buckconfig/cell/prelude digests.
  Any mismatch = silent cold start (initially binary-hash-strict; relax to
  schema-versioned once formats settle).
- Values checksummed; any decode failure = drop that node (or whole file)
  and recompute. Corruption can cost time, never correctness.
- Injected-leaf recomputation on load is the honesty boundary — identical
  in spirit to a remote action cache validating by digest.

## Staged plan

- **S0 measure**: profile the cold 2 minutes (parse vs configure vs
  analysis) on the rebuck2 sweep workload; pick the layer that pays.
- **S1 plumbing**: save/load for the trivial layers (file hash, dir
  listing) behind `--unstable-dice-save/--unstable-dice-load`. Small win;
  proves header validation, frontier diff, codec registry end to end.
- **S2 target graph**: TargetNode codecs. Expected to kill the
  parse+configure share.
- **S3 action graph**: the analysis dodge above. Expected to kill the
  analysis share for clean subgraphs.
- **S4 transport**: the persisted file rides existing CI cache machinery
  (in rebuck2's case, the same GH-cache seed/snapshot as the action cache).

## Existing scaffolding in-tree

- `dice/read_dump` + the introspection layer already serialize the graph
  *shape* (keys, edges) for debugging — the walker half of a persister.
- `dice/dice/docs/DiceIncrementalityAlgorithms.pdf` formalizes the
  invalidation model the loader must reproduce.

## Non-goals (for now)

- Cross-binary-version reuse.
- Sharing persisted state between differently-configured checkouts.
- Serializing starlark heaps (explicitly dodged; revisit only if query
  workloads on evicted nodes prove hot).
