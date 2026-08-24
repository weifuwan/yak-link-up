# ADR-0001: Use Flink-inspired runtime roles

- Status: Accepted
- Date: 2026-08-24

## Context

Link-Up has grown the pieces of a data synchronization runtime organically: connector factories, source/sink contracts,
splits, readers, planning, tasks, channels, metrics, a local engine, and a standalone Worker. Several concepts were
implemented more than once, including factory registries and source execution paths. Without a shared architecture
vocabulary, new features can easily create another parallel abstraction rather than extend the active one.

Hadoop MapReduce, Spark, and Flink were considered as references. Link-Up is connector-centric and already exposes
`Source`, `SourceReader`, `SourceSplit`, and `SourceSplitEnumerator`, which align most naturally with Flink's connector
role model.

## Decision

Link-Up will use Apache Flink as the primary conceptual reference for runtime role separation, while keeping a much
smaller local batch architecture.

We adopt these ideas:

- stable connector-facing API contracts are separate from runtime implementation;
- source work is expressed through `Source`, `SourceSplit`, `SourceReader`, and eventually an explicit enumerator/coordinator role;
- job definition, physical planning, and runtime execution are separate lifecycle stages;
- planner components do not execute tasks;
- executable work is represented by task roles;
- composition roots assemble runtime and connector implementations without creating compile-time framework-to-connector coupling.

We do not adopt Flink's distributed runtime, RPC stack, checkpoint subsystem, cluster resource management, or compatibility
surface.

## Consequences

Positive:

- existing Link-Up concepts gain consistent names and ownership boundaries;
- connector authors depend on a stable API rather than framework internals;
- duplicate planners/executors/registries become architecture violations instead of acceptable alternatives;
- future split coordination and execution-state work has a known direction.

Trade-offs:

- some current names (`ExecutionPlan`, `LocalFluxEngine`, server `runtime` classes) remain transitional in Phase 1;
- existing connector package layouts are not moved solely for cosmetic consistency;
- further separation into `JobGraph` and runtime execution state is deferred until it can be done with tests and clear behavior preservation.

## Alternatives considered

### Hadoop MapReduce

Its input split/task model is simple and proven, but its execution vocabulary is too MapReduce-specific for a general
source-to-sink synchronization engine.

### Apache Spark

Spark's Dataset/RDD/Stage model is optimized around computation APIs and SQL execution. It is less directly aligned with
Link-Up's connector-first architecture.

### A custom architecture with no external reference

This avoids borrowed terminology but increases the chance of inventing overlapping roles. Link-Up will remain custom in
implementation while using Flink's role boundaries as a design reference.
