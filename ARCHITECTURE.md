# Architecture

## 目标

Link-Up 是本地优先的离线数据同步引擎。架构只解决现在需要解决的问题：清晰的扩展契约、可测试的本地运行时、稳定的 Worker 控制面，以及安全的失败处理。

不复制 Flink/Spark 的分布式复杂度；只借鉴它们清晰的角色边界、计划先于执行和可解释运行模型。

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
  -> LogicalJobPlan
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

## Plan / Explain

Plan / Explain 不创建第二套执行图：

```text
JobDefinition
  -> LogicalJobPlan              # 用户意图、默认值、Fingerprint
  -> ConnectorPreparer
       validate                  # 无 Connector IO
       prepareForExplain         # Source discovery + Sink planning stub
  -> JobPlanner
  -> JobGraph                    # 与正式执行相同的物理计划
  -> PhysicalJobPlan             # Secret-safe Explain projection
  -> JobPlanResult               # JSON + deterministic text + diagnostics
```

### Validate 边界

`validate` 可以：

- 校验 JobSpec/HOCON；
- 解析默认值；
- 发现 Source/Sink Factory；
- 执行 Connector OptionRule 校验；
- 生成 LogicalJobPlan 和稳定 Fingerprint。

`validate` 不可以：

- 创建 Source/Sink；
- 发现远端 Schema；
- 枚举 Split；
- 打开连接或执行外部 IO。

### Explain 边界

`explain` 可以：

- 创建 Source；
- 发现 Source Schema；
- 枚举并校验 Source Split；
- 应用字段映射；
- 使用正式 JobPlanner 生成 JobGraph；
- 投影 Pipeline/Task 数量、并行度和数据集信息。

`explain` 不可以：

- 调用 SinkPreparer；
- 创建、删除或清空目标表；
- 执行目标端 DDL；
- 创建 SinkWriter；
- 写入数据；
- 序列化 Connector options、ReadonlyConfig、Prepared Connector、ClassLoader 或私有 metadata。

完整 Connector 配置只进入 SHA-256 Fingerprint 的内存计算。Fingerprint 能识别包括 Secret 在内的配置变化，但计划和诊断不输出原始值。

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

规划 API 是独立的只读控制边界：

```text
HTTP
  -> JobPlanningService
  -> JobPlanExplainer
  -> ConnectorPreparer / JobPlanner
```

它不创建 Job、Attempt、Checkpoint，也不进入 JobRuntimeScheduler。

Domain 不持有 `Thread`、`Future`、`Semaphore`、`ExecutorService` 或 framework `JobExecution`。

## Runtime Event Journal

Runtime Event 不替代现有状态模型，而是观察成功持久化的 checkpoint：

```text
JobRuntimeLifecycle
  -> JobRepository.save(snapshot, metadata)
  -> EventPublishingJobRepository
       1. durable checkpoint
       2. derive one lifecycle fact
       3. JobEventBus.publish
  -> JsonLineJobEventStore
       <stateDirectory>/job-events/<jobId>.jsonl
```

边界约束：

- `JobSnapshot` / `JobExecutionMetadata` 仍是控制面状态真相。
- Event Journal 只追加，不反向修改 Job 状态，不参与 Retry/Commit 决策。
- 只有 checkpoint 成功后才派生事件。
- `JobEventBus` 保持同一 Job 的同步顺序，并隔离 Listener 失败。
- 每条事件使用 `checkpointVersion` 作为单 Job 序号，Retry 后继续递增。
- Event JSON 不包含 JobSpec、Connector options、SQL、Secret、异常正文、Prepared Connector 或私有 metadata。
- 当前只记录 Job/Attempt 生命周期；Pipeline/Task 细粒度事件留给后续阶段。

查询链路：

```text
GET /api/v1/jobs/{jobId}/events?afterSequence=N&limit=M
  -> JobEventRestService
  -> JobEventReader
  -> sequence-based page
```

`afterSequence` 是排他游标。读取损坏或不支持版本的事件文件会返回稳定的 Event History 错误，但不会改变已运行任务的结果。

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
