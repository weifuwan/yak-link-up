# ADR-0003: Separate runtime coordination, scheduling, and execution

- Status: Accepted
- Date: 2026-08-24

## Context

Phase 2 separated the immutable `JobGraph` from mutable `ExecutionGraph`, but `JobExecution` still owned several runtime
roles at once. It transitioned job state, created the pipeline thread pool, submitted `PipelineGraph` work, collected
completion-order results, propagated first failure cancellation, instantiated `PipelineExecution`, aggregated commit
summaries, and exposed the public cancellation/metrics handle.

That implementation worked for the local runtime, but the role boundary was still ambiguous. In particular, adding a
future scheduling policy would require editing the same class that owns job lifecycle and public execution access.

Phase 1 defined role names as contracts: a Coordinator coordinates lifecycle, a Scheduler decides when executable work
runs, and an Executor executes already-planned work. Phase 3 applies that vocabulary to the active runtime.

## Decision

The local runtime is separated into the following chain:

```text
JobExecution                 public execution facade / handle
    |
    v
JobCoordinator               job lifecycle + failure policy + result aggregation
    |
    +--> PipelineScheduler   pipeline concurrency + completion-order collection
    |        |
    |        v
    |    PipelineExecutor    execute one PipelineGraph
    |        |
    |        v
    |    PipelineExecution   materialize channels and runtime tasks
    |        |
    |        v
    |    ExecutionCoordinator
    |        |
    |        v
    |    TaskExecutor        execute concrete SourceTask / SinkTask
    |
    v
ExecutionGraph               mutable state root for the run
```

### `JobExecution`

`JobExecution` remains the public runtime handle used by the server. It owns the `ExecutionGraph` reference and delegates
`execute()` to `JobCoordinator`. It exposes cancellation, metrics, run identity, and the execution graph without owning
pipeline scheduling logic.

It must not own an `ExecutorService`, `JobGraph`, `PipelineGraph`, or pipeline scheduling policy as mutable fields.

### `JobCoordinator`

`JobCoordinator` owns job-level lifecycle semantics:

- transition the `ExecutionGraph` to running;
- invoke the configured `PipelineScheduler`;
- derive the final `JobStatus` from scheduling failure/cancellation;
- aggregate `PipelineResult` commit summaries into `JobResult`;
- complete or fail the `ExecutionGraph`;
- own job-level log context and terminal logging.

It does not create pipeline worker threads and does not instantiate `PipelineExecution` directly.

### `PipelineScheduler`

`PipelineScheduler` owns pipeline concurrency policy. The local implementation, `LocalPipelineScheduler`:

- bounds concurrency using `runtime.pipelineParallelism`;
- submits immutable `PipelineGraph` units;
- collects results in completion order, preserving previous runtime behavior;
- propagates the first pipeline failure into the shared `CancellationToken`;
- interrupts submitted work if the scheduling thread itself is interrupted.

The scheduler does not perform source/sink I/O and does not own `ExecutionGraph` state.

### `PipelineExecutor`

`PipelineExecutor` executes one already-selected `PipelineGraph`. `LocalPipelineExecutor` materializes the existing
`PipelineExecution` using the current `ExecutionGraph` runtime resources and classloader.

It does not decide concurrency or job-level failure policy.

### Existing task-level roles

`PipelineExecution`, `ExecutionCoordinator`, and `TaskExecutor` remain in place. They form a lower-level runtime boundary:

- `PipelineExecution` materializes channels, dynamic split providers, and executable tasks for one pipeline;
- `ExecutionCoordinator` coordinates source/sink task outcomes inside that pipeline;
- `TaskExecutor` owns the task worker threads.

Phase 3 therefore introduces hierarchy rather than replacing the proven task runtime.

## Consequences

Positive:

- `JobExecution` becomes a thin public facade instead of a runtime god object;
- pipeline scheduling can evolve independently from lifecycle/result aggregation;
- local execution remains behind a narrow `PipelineExecutor` role;
- tests can inject scheduler/executor doubles without opening connector resources;
- scheduling concurrency and first-failure behavior now have focused unit tests;
- future scheduling policies have an explicit extension seam without changing `JobGraph` or `ExecutionGraph`.

Trade-offs:

- the framework gains several small internal role classes;
- `LocalPipelineScheduler` still uses an in-process fixed thread pool and is not a distributed scheduler;
- job-level and task-level coordinators coexist intentionally at different hierarchy levels;
- `PipelineExecution` is still a concrete local runtime class and may be refined in a later phase.

## Compatibility

This decision preserves:

- public `JobExecution` constructors and cancellation/metrics accessors;
- `JobSpec` and connector APIs;
- pipeline parallelism semantics;
- completion-order `PipelineResult` collection;
- first-failure cancellation propagation;
- static/dynamic split behavior;
- sink commit/result aggregation semantics.

## Non-goals

Phase 3 does not introduce remote execution, resource placement, retries, checkpoints, HA, worker discovery, or
`SourceSplitEnumerator` integration. Those require separate design decisions.
