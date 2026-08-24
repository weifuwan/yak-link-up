# Link-Up Architecture

This document is the architecture baseline for Link-Up. It describes the boundaries that new code must follow and the
vocabulary used in reviews. The runtime design is inspired by Apache Flink's separation of connector contracts,
planning, and runtime roles, but Link-Up remains a small local batch synchronization engine.

## Goals

Phase 1 establishes a stable architecture language without changing the core execution semantics:

- one canonical implementation for each framework role;
- explicit module and package dependency direction;
- clear model lifecycle from external job specification to runtime execution;
- predictable naming for factories, planners, coordinators, executors, readers, writers, and repositories;
- a repeatable connector package template;
- removal of obsolete parallel execution paths that duplicate the active runtime.

This phase does not introduce distributed scheduling, remote task execution, checkpoints, failover, or a Flink-compatible
API.

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

Allowed dependencies: general-purpose libraries required by the API itself.

Forbidden dependencies: `link-up-framework`, `link-up-server`, `link-up-launcher`, and concrete connector modules.

### `link-up-framework`

Owns local engine internals: connector discovery/preparation, job compilation, planning, channels, routing, execution,
metrics, and connector classloader isolation.

It may depend on `link-up-api`. It must not import concrete connector implementations.

### `link-up-connectors/*`

Own concrete integration behavior. A connector depends on `link-up-api` and third-party client libraries. It must not
reach into `link-up-framework` internals. The runtime discovers connector factories through API contracts and
`ServiceLoader`.

### `link-up-launcher` and `link-up-server`

These are composition roots. They may assemble framework services and concrete connector artifacts. Business logic that
belongs to the engine must not be placed here merely because these modules own the process entry point.

## Model lifecycle

Link-Up uses different models for different lifecycle stages. Do not reuse a transport DTO as a runtime object.

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
PreparedJob                  resolved factories, schemas and connector resources
    |
    | JobPlanner
    v
ExecutionPlan                immutable physical plan (Phase 1 name)
    |
    | JobExecution
    v
Task execution               SourceTask / SinkTask / Channel
    |
    v
JobResult                    engine result
    |
    v
JobSnapshot                  server-side read model
```

`ExecutionPlan` is intentionally retained in Phase 1 to avoid a behavior-changing rename. A later phase may evolve this
model into an explicit `JobGraph`/`ExecutionGraph` split once planner and runtime state are separated further.

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

Transforms prepared input into an immutable physical execution plan. Planner code may calculate split assignment,
parallelism, pipeline topology, and task plans.

Planner code must not start tasks, create executor services, or write data.

### `framework.execution`

Owns runtime lifecycle, cancellation, coordination, task execution, and execution state. Concrete executable tasks belong
under `framework.execution.task`.

The active execution path is:

```text
JobExecution
  -> PipelineExecution
     -> ExecutionCoordinator
        -> TaskExecutor
           -> execution.task.SourceTask / execution.task.SinkTask
```

Do not add a second source/sink execution pipeline alongside this path.

### `framework.channel` and `framework.routing`

Own data transport between tasks and channel selection. They do not discover connectors or compile job definitions.

### `framework.metrics`

Own runtime measurements only. Metrics must observe execution; they must not become a control plane or scheduler.

### `framework.classloading`

Own connector classloader isolation and classloader scope management.

## Role vocabulary

Use role names consistently. A suffix is a contract, not decoration.

| Role | Responsibility |
| --- | --- |
| `Factory` | Construct an extension-facing component from validated configuration. |
| `Registry` | Discover/index implementations and resolve them by stable identifier. |
| `Compiler` | Translate one model/protocol into another normalized model. |
| `Planner` | Produce an immutable execution plan without executing it. |
| `Coordinator` | Coordinate lifecycle and outcomes across multiple runtime actors. |
| `Scheduler` | Decide when/where executable work should run. Add only when this responsibility exists. |
| `Executor` | Execute already-planned work. |
| `Reader` | Read records from an external source. |
| `Writer` | Write records to an external sink. |
| `Enumerator` | Discover or enumerate source splits. |
| `Repository` | Persist or retrieve state; no scheduling side effects. |
| `Gateway` | Cross a process/system boundary behind a narrow interface. |
| `Manager` | Reserved for a true top-level lifecycle owner; prefer a more precise role when possible. |

Avoid catch-all names such as `Common`, `Helper`, `Misc`, and generic `Utils` when a domain role can be named. Existing
legacy packages may be migrated incrementally, but new code should use role-specific packages.

## Source roles

The API already exposes the core Flink-inspired concepts:

```text
Source
  -> SourceSplit
  -> SourceReader
  -> SourceSplitEnumerator
```

In Phase 1 the planner still calls `Source#createSplits(...)` for compatibility. `SourceSplitEnumerator` is therefore a
contract whose runtime integration is not yet complete. A later phase should make split enumeration/coordinator
ownership explicit instead of placing split discovery inside `JobPlanner`.

Do not add another split abstraction in the framework to work around this transition.

## Server boundary

`link-up-server` is the local Worker control plane. It owns HTTP adaptation, submission/idempotency, queue admission,
worker identity, job status/read models, and runtime invocation. It must not implement connector-specific behavior.

The desired direction is:

```text
HTTP adapter
    -> application/use-case service
       -> job domain/state
          -> runtime/repository ports
```

Phase 1 documents this direction without forcing a large package migration. Server package restructuring belongs to a
later behavior-preserving step.

## Dependency rules for reviews

Reject a change when it introduces any of the following without an explicit architecture decision:

1. a connector importing `com.link.up.framework.*`;
2. framework code importing a concrete connector package;
3. transport DTOs being used as mutable runtime state;
4. planner code creating threads or performing I/O that belongs to readers/writers;
5. a second `FactoryRegistry`, task hierarchy, or source execution path;
6. a generic `common`, `core`, `helper`, or `utils` package used as a dumping ground;
7. server HTTP code manipulating task/channel internals directly.

## Phase 1 cleanup

The Phase 1 refactor removes obsolete framework internals that duplicated the active runtime:

- the old `framework.plugin.FactoryRegistry` in favor of `framework.connector.FactoryRegistry`;
- the legacy `framework.factory.PreparedSource` branch;
- the legacy `execution.source.*` processor/task path and `execution.sink.SinkExecuteProcessor`;
- placeholder `Test` source files.

Round-robin split assignment remains covered through the active `planner.SplitAssigner` implementation.

## Next architecture steps

The next phases should be incremental:

1. make the plan model explicit (`JobDefinition -> JobGraph/ExecutionPlan -> runtime execution state`);
2. separate coordinator/scheduler/executor responsibilities where current classes own multiple lifecycle stages;
3. integrate `SourceSplitEnumerator` as the standard split discovery role;
4. migrate connector package layouts toward the connector development template;
5. split Maven modules only if package boundaries prove insufficient.
