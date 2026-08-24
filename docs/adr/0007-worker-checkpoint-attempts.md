# ADR-0007: Persist Worker checkpoints and model execution attempts

- Status: Accepted
- Date: 2026-08-24

## Context

Phase 6 separated the Worker control plane into application, domain, ports and infrastructure. The remaining reliability gap was process restart:

- active job state existed only in `JobApplicationService` memory;
- idempotency/external-execution indexes were rebuilt only from new submissions;
- `InMemoryJobRepository` preserved terminal history only for the current process;
- a process crash could leave the control plane unable to explain whether a previously RUNNING job finished;
- the model had one Job lifecycle but no explicit execution-attempt identity for future retry/recovery work.

Automatically rerunning an interrupted synchronization is not safe by default. A Sink may have committed some data before the Worker disappeared, and current commit/retry semantics differ by connector.

## Decision

Phase 7 introduces two foundations without automatic retry:

```text
Job identity
    |
    +--> Attempt #1
            |
            +--> QUEUED
            +--> RUNNING
            +--> SUCCEEDED / FAILED / CANCELED / LOST
```

and:

```text
JobApplicationService
    |
    +--> checkpoint after lifecycle changes
    |
    v
JobRepository
    |
    v
FileJobRepository
    |
    +--> versioned per-job JSON checkpoint
    +--> fsync temporary file
    +--> atomic replace when supported
```

### Attempt model

`domain.JobExecutionAttempt` is the state of one concrete execution attempt. It records:

- `attemptNumber` and stable `attemptId`;
- attempt lifecycle status;
- create/queue/start/end timestamps;
- framework `runId` and job log path as values only;
- terminal failure type/message;
- connector/framework `retryAdvice` from the terminal `CommitSummary` when available.

It does not own a Thread, Future, Semaphore, ExecutorService or framework `JobExecution` object.

Phase 7 creates exactly one attempt. The job state stores a list intentionally so a later retry policy can append Attempt #2/#3 without changing the stable Job identity or REST resource.

`JobAttemptMetadata` is the read-model representation exposed additively through `JobResponse.attempts` and `attemptCount`.

### Checkpoint semantics

The application upserts the same Job record after meaningful state changes:

```text
SUBMITTED
QUEUED
RUNNING
job log identity bound
cancellation requested
terminal completion
```

If scheduling is rejected or fails before acceptance is complete, the partially persisted checkpoint is deleted together with the in-process idempotency registration.

`JobRepository.save(...)` is therefore an upsert, not a terminal-only append.

### Local durable repository

The standalone Worker now composes `FileJobRepository` by default. `InMemoryJobRepository` remains available for tests and embedded use.

Default state directory:

```text
data/worker-state
```

Override it with:

```text
--state-dir /path/to/link-up-state
```

Each Job is written to one versioned JSON file. Job IDs are URL-safe-base64 encoded before becoming file names. Writes use a temporary file, flush + file-descriptor sync, and atomic move where the file system supports it.

A malformed or unsupported checkpoint fails repository initialization instead of being silently ignored, because silently losing idempotency/recovery state is more dangerous than failing startup.

### Persisted boundary

The durable format intentionally stores control-plane recovery information:

- Job identity/name/status and top-level timestamps/errors;
- external execution ID and idempotency key;
- definition version/config digest;
- lifecycle transition audit;
- cancellation intent;
- run/log identity;
- attempt history.

Detailed pipeline/task/channel metrics and Table-DDL views remain process-local in Phase 7. Terminal jobs keep the full rich snapshot while the process is alive; after restart they are reconstructed as a lifecycle/history view with empty detailed runtime metrics.

This prevents the persistence layer from serializing framework runtime objects merely to make a historical dashboard richer.

### Startup recovery

`JobApplicationService` scans `JobRepository.listEntries()` during construction.

For every persisted record it restores the external-execution/idempotency index. Terminal records remain terminal.

Any non-terminal checkpoint is deterministically converted to `LOST`:

```text
CREATED / SUBMITTED / QUEUED / RUNNING
                    |
                    v
                   LOST
```

Recovery appends an auditable `worker-restart-recovery` state transition and changes any non-terminal Attempt to `LOST` with a `WorkerRestartRecovery` reason.

The interrupted execution is **not** automatically scheduled again.

### Why no automatic retry yet

Retry requires a safety decision, not only a retry count. For example:

- a Sink may commit per batch;
- a 2PC Sink may have prepared but uncommitted transactions;
- a failure may occur before any write, after partial write, or during commit;
- connector retry advice and commit summaries already expose useful evidence.

Phase 7 records Attempt outcome/retry advice so the next phase can define retry eligibility deliberately.

## Compatibility

Phase 7 preserves:

- existing Job IDs and REST routes;
- existing lifecycle status names;
- idempotency conflict rules;
- queue/admission behavior;
- cancellation behavior;
- Framework `JobGraph` / `ExecutionGraph` semantics;
- connector execution/commit behavior.

REST changes are additive: `attempts` and `attemptCount` are new response properties.

## Consequences

Positive:

- standalone Worker history/idempotency survives process restart;
- interrupted jobs no longer disappear or remain ambiguously RUNNING after restart;
- the stable Job identity is separated from execution-attempt identity;
- future retries can append attempts instead of overwriting prior execution history;
- persistence remains a control-plane concern rather than a framework-runtime serialization concern;
- state writes are deterministic and inspectable JSON files.

Trade-offs:

- durable history after restart does not yet retain detailed pipeline/task/channel metrics or Table-DDL projections;
- persistence is local-file based, not a distributed database;
- active checkpoints are state-transition checkpoints, not continuous metrics snapshots;
- no retry or resume is performed automatically.

## Follow-up

A later retry/recovery phase can use Attempt history plus `CommitSummary`/`retryAdvice` to decide whether a new attempt is safe. Durable detailed observability can be added independently through a purpose-built metrics/event store if it becomes necessary.
