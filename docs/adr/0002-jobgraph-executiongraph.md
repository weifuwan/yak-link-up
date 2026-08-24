# ADR-0002: Separate JobGraph from ExecutionGraph

- Status: Accepted
- Date: 2026-08-24

## Context

Phase 1 established a Flink-inspired architecture vocabulary and made `ExecutionPlan` the transitional name for the
physical plan produced by `JobPlanner`. It also documented a future `JobGraph` / `ExecutionGraph` separation.

The existing implementation still mixed the two lifecycle stages in one important place: `SourceTaskPlan` stored a
runtime `SplitProvider`, and `JobPlanner` created `LocalSplitQueue` when dynamic split assignment was enabled. That meant
planning created mutable execution state even though the plan was intended to be immutable.

This coupling makes future scheduling, retries, execution snapshots, and recovery harder because the same object graph
contains both the description of work and the ownership state of a particular run.

## Decision

Link-Up separates the physical plan from runtime state:

```text
JobDefinition
    |
    v
PreparedJob
    |
    | JobPlanner
    v
JobGraph                 immutable physical plan
    |
    | JobExecution
    v
ExecutionGraph           mutable state for one run
    |
    v
PipelineExecution / Task execution
    |
    v
JobResult
```

### JobGraph

`JobGraph` and `PipelineGraph` are planner-owned immutable models. They describe:

- job and pipeline identity;
- execution configuration;
- source/sink task plans;
- static split assignments;
- all source splits represented by a pipeline;
- the selected split-assignment mode.

They must not own:

- `ExecutorService` or threads;
- `CancellationToken`;
- `JobMetrics` / channel runtime metrics;
- `SplitProvider` / `LocalSplitQueue`;
- channels or opened connector resources;
- mutable execution status.

### ExecutionGraph

`ExecutionGraph` represents one run of one `JobGraph`. It owns:

- run/log identity;
- start/end timestamps;
- runtime status;
- cancellation state;
- job metrics;
- terminal failure/result.

A new run of the same physical graph receives a new `ExecutionGraph`.

### Dynamic split ownership

`JobPlanner` still computes the source task count and static assignment because task topology is a planning concern.
When `SplitAssignmentMode.DYNAMIC` is selected, `PipelineExecution` creates one `LocalSplitQueue` from the immutable
pipeline split list and injects that provider into the runtime `SourceTask` instances.

This preserves the existing dynamic assignment behavior while moving mutable ownership into the execution layer.

## Consequences

Positive:

- planner output is genuinely free of mutable runtime ownership state;
- the same `JobGraph` can conceptually be executed more than once with independent runtime state;
- cancellation and metrics have a single execution-level owner;
- future Scheduler / recovery / snapshot work has an explicit state object to extend;
- runtime split coordination can evolve without changing the physical-plan contract.

Trade-offs:

- `PreparedSource` / `PreparedSink` are still referenced by task plans, so `JobGraph` is an internal physical graph rather
  than a serializable remote-deployment graph;
- task planning still uses framework `TaskId` / `TaskType`; moving pure task identity out of the execution package is a
  possible later cleanup;
- `JobExecution` remains the local pipeline coordinator and is not yet a general scheduler.

## Compatibility

This decision does not change:

- `link-up-api` connector contracts;
- public `JobSpec` submission protocol;
- server queue/idempotency/state-machine behavior;
- source/sink parallelism semantics;
- static or dynamic split-assignment behavior;
- sink commit semantics.

`ExecutionPlan` and `PipelinePlan` were framework-internal transitional model names and are replaced by `JobGraph` and
`PipelineGraph`.
