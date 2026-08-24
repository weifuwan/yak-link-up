# ADR-0004: Standardize Source split discovery with Enumerator and SourceCoordinator

- Status: Accepted
- Date: 2026-08-24

## Context

Phases 1-3 established role vocabulary, separated physical/runtime state, and split job coordination from scheduling and
execution. One important boundary was still transitional: `JobPlanner` called `Source#createSplits(...)` directly.

The API already contained both `SourceSplitEnumerator` and `SourceEnumeratorContext`, and JDBC already had a
`JdbcSourceSplitEnumerator`, but these concepts were not part of the framework's canonical planning path. As a result:

- `JobPlanner` owned a connector lifecycle concern in addition to topology planning;
- enumerator creation/close semantics were not controlled by one framework role;
- connector classloader scope during split discovery was implicit;
- split validation differed between static planning and dynamic `LocalSplitQueue` execution;
- new connectors had no meaningful reason to implement the existing Enumerator abstraction.

## Decision

Bounded split discovery is standardized on this path:

```text
JobPlanner
    |
    v
SourceCoordinator                 framework lifecycle boundary
    |
    v
Source#createEnumerator(...)
    |
    v
SourceSplitEnumerator
    |
    v
enumerateSplits()
```

### Source preparation vs Source coordination

Connector configuration and connector-specific parallelism validation remain in `ConnectorPreparer`. This preserves the
existing fail-fast ordering: unsupported Source modes are rejected before Sink preparation can perform metadata/DDL side
effects.

`SourceCoordinator` starts after a validated `PreparedSource` exists. It owns:

- `SourceEnumeratorContext` creation;
- connector thread-context classloader scope for Enumerator creation/enumeration/close;
- `SourceSplitEnumerator` creation and close lifecycle;
- validation and immutable copying of enumerated splits.

It does not repeat connector-specific configuration/parallelism validation, assign splits to tasks, create readers, create
runtime split queues, or schedule work.

### JobPlanner

`JobPlanner` no longer calls Source split-discovery methods directly. It receives validated splits from
`SourceCoordinator` and remains responsible for:

- grouping splits by `dataSetId`;
- task parallelism/topology;
- static round-robin assignment;
- `JobGraph` / `PipelineGraph` construction.

### Source API compatibility

`Source#createEnumerator(Map, SourceEnumeratorContext)` becomes the canonical extension point.

The legacy `createSplits(...)` overloads remain as deprecated compatibility hooks. The default `createEnumerator(...)`
adapts an existing connector by calling its legacy split method, so connectors such as the current HTTP Source continue
to work without immediate changes.

The single-argument legacy method is changed from abstract to a default compatibility method. This allows a new connector
to implement only `createEnumerator(...)` and `createReader(...)` without also implementing a dead legacy method.

### JDBC migration

JDBC is migrated in this phase as the first native implementation:

```text
JdbcSource#createEnumerator
    |
    v
JdbcSourceSplitGenerator         prepare metadata/statistics
    |
    v
JdbcSourceSplitEnumerator       calculate bounded JDBC splits
```

Its legacy `createSplits(...)` methods remain as compatibility bridges and delegate back through the Enumerator path.

### Split contract

`SourceCoordinator` accepts an empty split list, which represents a valid empty bounded input. For non-empty input it
requires:

- no null split values;
- non-blank `splitId`;
- non-blank `dataSetId`;
- unique `splitId` values within each `dataSetId`.

The same `splitId` may appear in different data sets because each data set becomes an independent `PipelineGraph`.

## Consequences

Positive:

- split discovery has one canonical extension contract;
- planner responsibilities are narrower and easier to test;
- Enumerator lifecycle and connector classloader scope are deterministic;
- static and dynamic execution consume the same validated split contract;
- existing connectors can migrate independently;
- connector-specific validation still fails before Sink preparation side effects;
- future Source coordination can evolve without reintroducing split lifecycle code into `JobPlanner`.

Trade-offs:

- split enumeration still happens before `JobGraph` construction because the current local physical graph needs the full
  bounded split set;
- this is not Flink's distributed runtime `SplitEnumerator` protocol: there is no reader registration, asynchronous split
  requests, checkpointed enumerator state, or remote coordinator;
- legacy `createSplits(...)` methods remain in the API during the compatibility period.

## Compatibility

This phase does not change:

- `JobSpec` or Worker submission protocols;
- source/sink task parallelism semantics;
- static/dynamic split assignment policy;
- `SourceReader` behavior;
- existing connector factories;
- HTTP Source behavior through the legacy adapter;
- JDBC split statistics/planning semantics.

## Follow-up

After connectors have migrated, a later compatibility cleanup may remove the legacy `createSplits(...)` hooks in a major
API version. That cleanup is deliberately not part of this phase.
