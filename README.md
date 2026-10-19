<p align="center">
  <img src="assets/logo.png" alt="Hitorro Mesh Pipelines Kvstore logo" width="180">
</p>

# hitorro-mesh-pipelines-kvstore

RocksDB backend for the `kvstore` sink + source kinds in
[hitorro-mesh-pipelines](https://github.com/geekychris/hitorro-mesh-pipelines).
Drop this jar on the classpath and the pipelines' `SinkRegistry` and
`SourceFactory` auto-load the adapter via `ServiceLoader` —
`{kind: kvstore, name: X, keyExpr: id}` in a job spec starts writing
to a real RocksDB store at `${HITORRO_PIPELINES_HOME}/kv/X/` with no
extra wiring.

## Wire shape

Sink:

```yaml
sinks:
  - {kind: kvstore, name: users, keyExpr: user.id}
```

Source (round-trip in a downstream node):

```yaml
source: {kind: kvstore, name: users}
```

## Exactly-once contract

`KvStoreSink` overrides `Sink.addIdempotent(taskId, seq, row)` — a
retried mapper that re-sends the same `(taskId, seq)` row is a no-op
(the sink tracks the highest seq per task ephemerally). Null taskId
falls back to plain `add()` semantics (at-least-once). Coverage in
`KvStoreSinkTest` (13 tests in `hitorro-kvstore`) including a
concurrent-acquire race test on the shared-handle registry.

## Handle sharing

`RocksDbSharedRegistry` refcounts open `RocksDB` handles keyed by
store name — a sink + source on the same store in one JVM share a
single handle instead of fighting for RocksDB's exclusive lock.
Handles close cleanly when the last user releases.

## Tests

- `RocksDbRoundTripTest` — end-to-end sink → source through the
  pipelines' `SinkRegistry` / `SourceFactory`
- `KvStoreSinkTest` (in `hitorro-kvstore`) — key extraction, idempotent
  writes, shared handle refcount lifecycle
