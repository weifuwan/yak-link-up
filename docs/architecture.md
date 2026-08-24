# Link-Up Architecture

This document is the architecture baseline for Link-Up. It describes the boundaries that new code must follow and the
vocabulary used in reviews. The runtime design is inspired by Apache Flink's separation of connector contracts,
planning, and runtime roles, but Link-Up remains a small local batch synchronization engine.

## Goals

The architecture is evolving incrementally instead of through a full rewrite:

- one canonical implementation for each framework role;
- explicit module and package dependency direction;
- separate protocol, definition, physical-plan, runtime-state, and read-model lifecycles;
- predictable naming for factories, planners, coordinators, executors, readers, writers, and repositories;
- a repeatable connector package template;
- no mutable runtime ownership inside planner models.

Link-Up does not attempt to reproduce Flink's distributed scheduler, RPC stack, checkpoint subsystem, resource manager,
or compatibility surface.

## Module boundaries

```text
                    link-up-launcher / link-up-server
                       /                     \
                      v                       v
             link-up-framework        connector implementations
                      \                       /
                       v                     v
                           link-up-api
```

### `link-up-api`

Owns stable contracts that connector authors may depend on: configuration, connector factories, source/sink contracts,
table/catalog types, row types, errors, connector schema metadata, and the public `JobSpec` protocol.

Forbidden dependencies: `link-up-framework`, `link-up-server`, `link-up-launcher`, and concrete connector modules.

### `link-up-framework`

Owns local engine internals: connector discovery/preparation, job compilation, physical planning, channels, routing,
execution state, task execution, metrics, and connector classloader isolation.

It may depend on `link-up-api`. It must not import concrete connector implementations.

### `link-up-connectors/*`

Own concrete integration behavior. A connector depends on `link-up-api` and third-party client libraries. It must not
reach into `link-up-framework` internals. The runtime discovers connector factories through API contracts and
`ServiceLoader`.

### `link-up-launcher` and `link-up-server`

These are composition roots. They may assemble framework services and concrete connector artifacts. Business logic that
belongs to the engine must not be placed here merely because these modules own the process entry point.

## Model lifecycle

Link-Up uses different models for different lifecycle stages. A transport DTO, physical graph, runtime state, and API read
model are intentionally different objects.

```text
YAML / REST
    |
    v
JobSpec                      public protocol
    |
    | JobSpecCompiler
    v
JobDefinition                validated internal definition
    |
    | ConnectorPreparer
    v
PreparedJob                  resolved connector/schema resources
    |
    | JobPlanner
    v
JobGraph                     immutable physical graph
    |
    | JobExecution
    v
ExecutionGraph               mutable state for one run
    |
    v
PipelineExecution
    |
    v
SourceTask / SinkTask / Channel
    |
    v
JobResult                    terminal engine result
    |
    v
JobSnapshot                  server-side read model
```

The core distinction is:

- `JobGraph` answers **what should be executed**;
- `ExecutionGraph` answers **what is happening in this run**.

See [ADR-0002](adr/0002-jobgraph-executiongraph.md) for the decision and consequences.

## Physical planning

### `JobGraph`

`JobGraph` is the immutable top-level physical plan. It owns the job name, execution configuration, and a list of
`PipelineGraph` objects. It contains no cancellation token, executor, channel, runtime metrics, or status.

### `PipelineGraph`

A `PipelineGraph` is the physical execution boundary for one logical data set. It owns:

- `pipelineId` and `dataSetId`;
- the output catalog table;
- source task plans;
- sink task plans;
- immutable source splits represented by the pipeline;
- the split-assignment mode.

The graph records the selected policy but does not instantiate a mutable split queue.

### `SourceTaskPlan` / `SinkTaskPlan`

Task plans describe inputs required to construct executable tasks. Runtime ownership objects do not belong here.
In particular, `SourceTaskPlan` must never store `SplitProvider`, `LocalSplitQueue`, cancellation tokens, metrics, or
channels.

## Runtime execution state

### `ExecutionGraph`

Every invocation of a `JobGraph` gets a distinct `ExecutionGraph`. It owns:

- `JobStatus` for the runtime execution;
- run/log identity;
- execution timestamps;
- `CancellationToken`;
- `JobMetrics`;
- failure and terminal `JobResult`.

The object is intentionally small. It is the state root that future scheduling, recovery, or read-only runtime snapshots
should extend rather than adding more mutable state to `JobGraph` or `JobExecution`.

### `JobExecution`

`JobExecution` is the local coordinator for one `ExecutionGraph`. It:

- marks the execution lifecycle;
- starts pipeline executions with the configured pipeline parallelism;
- propagates cancellation after the first failure;
- aggregates pipeline outcomes into `JobResult`.

It does not compile `JobSpec`, prepare connectors, or mutate the physical graph.

### `PipelineExecution`

`PipelineExecution` materializes runtime-only resources for one `PipelineGraph`: channels, runtime tasks, task executor,
and, for dynamic assignment, the shared `LocalSplitQueue`.

For dynamic split assignment:

```text
PipelineGraph
  sourceSplits + DYNAMIC
          |
          v
PipelineExecution
          |
          +--> LocalSplitQueue        runtime ownership
          |
          +--> SourceTask #0 ---------+
          +--> SourceTask #1 ---------+ shared provider
          +--> SourceTask #N ---------+
```

For static assignment, each `SourceTask` consumes the immutable split list already present in its `SourceTaskPlan`.

## Framework package roles

### `framework.connector`

Connector discovery and preparation. `FactoryRegistry` in this package is the single canonical runtime registry.
Prepared connector objects live here because they are framework-owned resolved resources, not API contracts.

This package may create/resolve connector instances. It must not schedule or execute tasks.

### `framework.job`

Normalized job definition, execution configuration, statuses, and result value objects. `JobSpecCompiler` translates the
public protocol into this model.

This package must not create threads, open connector resources, or perform task execution.

### `framework.planner`

Transforms prepared input into immutable `JobGraph` / `PipelineGraph` physical models. Planner code may calculate split
assignment, parallelism, pipeline topology, and task plans.

Planner code must not:

- create threads or executor services;
- create channels;
- create `SplitProvider` / `LocalSplitQueue`;
- own cancellation or runtime metrics;
- execute source/sink I/O.

### `framework.execution`

Owns runtime lifecycle, `ExecutionGraph`, cancellation, coordination, runtime resource materialization, and task execution.
Concrete executable tasks belong under `framework.execution.task`.

The active execution path is:

```text
JobExecution
  -> ExecutionGraph
  -> PipelineExecution
     -> ExecutionCoordinator
        -> TaskExecutor
           -> execution.task.SourceTask / execution.task.SinkTask
```

Do not add a second source/sink execution pipeline beside this path.

### `framework.channel` and `framework.routing`

Own data transport between tasks and channel selection. They do not discover connectors or compile job definitions.

### `framework.metrics`

Own runtime measurements only. Metrics observe execution; they must not become a planner or scheduler.

### `framework.classloading`

Own connector classloader isolation and classloader scope management.

## Role vocabulary

Use role names consistently. A suffix is a contract, not decoration.

| Role | Responsibility |
| --- | --- |
| `Factory` | Construct an extension-facing component from validated configuration. |
| `Registry` | Discover/index implementations and resolve them by stable identifier. |
| `Compiler` | Translate one model/protocol into another normalized model. |
| `Planner` | Produce an immutable physical graph without executing it. |
| `Graph` | Describe a topology or state root for one lifecycle stage. Name the stage explicitly (`JobGraph`, `ExecutionGraph`). |
| `Coordinator` | Coordinate lifecycle and outcomes across multiple runtime actors. |
| `Scheduler` | Decide when/where executable work should run. Add only when this responsibility exists. |
| `Executor` | Execute already-planned work. |
| `Reader` | Read records from an external source. |
| `Writer` | Write records to an external sink. |
| `Enumerator` | Discover or enumerate source splits. |
| `Repository` | Persist or retrieve state; no scheduling side effects. |
| `Gateway` | Cross a process/system boundary behind a narrow interface. |
| `Manager` | Reserved for a true top-level lifecycle owner; prefer a more precise role when possible. |

Avoid catch-all names such as `Common`, `Helper`, `Misc`, and generic `Utils` when a domain role can be named.

## Source roles

The API exposes the core Flink-inspired concepts:

```text
Source
  -> SourceSplit
  -> SourceReader
  -> SourceSplitEnumerator
```

The physical/runtime split is now explicit, but split discovery is still transitional: `JobPlanner` invokes
`Source#createSplits(...)` during preparation/planning for compatibility. A later phase should make
`SourceSplitEnumerator` / source coordination the standard split-discovery role.

Do not add another split abstraction to work around this transition.

## Server boundary

`link-up-server` is the local Worker control plane. It owns HTTP adaptation, submission/idempotency, queue admission,
worker identity, server job status/read models, and runtime invocation. It must not implement connector-specific
behavior.

The desired direction remains:

```text
HTTP adapter
    -> application/use-case service
       -> job domain/state
          -> runtime/repository ports
```

`ExecutionGraph` is framework runtime state; `JobSnapshot` remains the server read model. The server must not expose the
mutable graph object directly to HTTP clients.

## Dependency rules for reviews

Reject a change when it introduces any of the following without an explicit architecture decision:

1. a connector importing `com.link.up.framework.*`;
2. framework code importing a concrete connector package;
3. transport DTOs being used as mutable runtime state;
4. planner code creating threads, channels, split queues, cancellation tokens, or runtime metrics;
5. `SourceTaskPlan`, `PipelineGraph`, or `JobGraph` holding `SplitProvider` or other mutable execution ownership;
6. a second `FactoryRegistry`, task hierarchy, or source execution path;
7. a generic `common`, `core`, `helper`, or `utils` package used as a dumping ground;
8. server HTTP code manipulating task/channel internals directly.

## Completed architecture phases

### Phase 1: role and package baseline

- established Flink-inspired runtime vocabulary;
- documented module/package dependency direction;
- removed duplicate registry and legacy execution paths;
- standardized connector development guidance.

### Phase 2: physical graph and runtime state

- replace transitional `ExecutionPlan` with `JobGraph`;
- replace `PipelinePlan` with `PipelineGraph`;
- introduce `ExecutionGraph` as mutable state for one run;
- remove `SplitProvider` from planner task models;
- move `LocalSplitQueue` creation into `PipelineExecution`;
- keep static/dynamic split semantics and external protocols unchanged.

## Next architecture steps

The next phases should remain incremental:

1. separate coordinator/scheduler/executor responsibilities where current classes still own multiple lifecycle stages;
2. integrate `SourceSplitEnumerator` as the standard split-discovery/coordinator role;
3. migrate connector package layouts toward the connector development template as connectors are touched;
4. improve server application/domain/adapter boundaries without coupling them to execution internals;
5. split Maven modules only if package boundaries prove insufficient.
