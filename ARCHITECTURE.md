# Architecture

## 目标

Link-Up 是本地优先的离线数据同步引擎。架构只解决现在需要解决的问题：清晰的扩展契约、可测试的本地运行时、稳定的 Worker 控制面，以及安全的失败处理。

不复制 Flink 的分布式复杂度；只借鉴它清晰的角色边界。

## 模块依赖

```text
                  launcher / server
                   /           \
                  v             v
             framework      connectors
                  \             /
                   v           v
                        api
```

硬规则：

- `api` 不依赖 framework/server/launcher/具体 connector。
- `framework` 不依赖具体 connector。
- connector 只实现 `api` 契约，不引用 framework 内部。
- `server` 和 `launcher` 是 composition root。

## Data Plane

```text
JobSpec
  -> JobDefinition
  -> ConnectorPreparer
  -> PreparedJob
  -> JobPlanner
  -> JobGraph
  -> ExecutionGraph
  -> JobExecution
  -> JobCoordinator
  -> PipelineScheduler
  -> PipelineExecutor
  -> PipelineExecution
  -> TaskExecutor
```

`JobGraph` 是不可变物理计划；`ExecutionGraph` 是一次运行的可变状态。

Source 分片链路：

```text
JobPlanner
  -> SourceCoordinator
  -> Source#createEnumerator(...)
  -> SourceSplitEnumerator
  -> validated splits
```

Planner 不创建 Reader、线程、Channel、Split Queue 或 CancellationToken。

## Control Plane

```text
HTTP / registration
  -> JobApplication
  -> JobApplicationService
     -> JobExecutionState
        -> JobExecutionAttempt
     -> application ports
        -> JobRuntimeScheduler
        -> JobRepository
        -> JobExecutor
        -> JobIdGenerator
  -> infrastructure adapters
```

Domain 不持有 `Thread`、`Future`、`Semaphore`、`ExecutorService` 或 framework `JobExecution`。

## Retry

Retry 是新的 Attempt，不是把旧 Attempt 改活：

```text
FAILED Attempt #1
  -> RetryPolicy
  -> commit evidence == SAFE
  -> Attempt #2
```

允许 Retry 的最低条件：

- Job 当前为 `FAILED`；
- 上一次 Attempt 有 commit evidence；
- `dataCommittedTaskCount == 0`；
- `successfullyCommittedRecordCount == 0`；
- `unknownStateRecordCount == 0`；
- `partialDataCommit == false`。

`LOST` 和 `CANCELED` 默认不重试。

## Connector 结构

推荐角色包：

```text
source / sink / catalog / client / config / converter / internal
```

JDBC `core/{converter,dialect,split}` 是历史兼容区，不允许继续扩张。

## 一条判断标准

新增类时先问：**它是在描述计划、状态、协调、调度、执行，还是外部系统边界？**

如果一个类同时回答了两个以上问题，通常应该拆。
