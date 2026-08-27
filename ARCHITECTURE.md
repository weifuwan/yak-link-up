# Architecture

## 目标

Link-Up 是本地优先的离线数据同步引擎。架构只解决现在需要解决的问题：清晰的扩展契约、可测试的本地运行时、稳定的 Worker 控制面，以及安全的失败处理。

不复制 Flink/Spark 的分布式复杂度；只借鉴它们清晰的角色边界、计划先于执行和可解释运行模型。

明确边界：**Link-Up 不做实时同步，不做 Flink 式数据 checkpoint/savepoint，也不从 Split/offset 自动恢复执行。**

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
  -> CapabilityNegotiator
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

## Capability Negotiation

Capability 描述 Connector 可以提供的稳定执行能力。Job 可以声明：

```text
required  -> 缺失即拒绝
preferred -> 缺失时允许降级，并返回 Warning
```

协商不是只读页面能力，而是 Validate、Explain 和正式 Runtime 共用的执行前置条件：

```text
JobDefinition
  -> negotiate explicit requirements       # 无 Connector IO
  -> validate option rules
  -> prepare Source + discover schemas
  -> negotiate derived topology requirements
  -> prepare Sink                          # 这里才允许 DDL/cleanup
  -> enumerate splits + JobGraph
  -> negotiate observed physical facts
  -> execute
```

多表边界必须特别明确：Source Schema 发现出多个数据集时，`MULTI_TABLE` 会同时成为 Source 和 Sink 的派生 Required Capability。协商发生在 `SinkPreparer` 之前，因此不兼容任务不会先建表、清表再失败。

Capability 保持离线同步所需的有限集合，不演化为通用规则引擎。

## Plan / Explain

Plan / Explain 不创建第二套执行图：

```text
JobDefinition
  -> LogicalJobPlan              # 用户意图、默认值、Capability、Fingerprint
  -> CapabilityNegotiator
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
- 协商显式 Required/Preferred Capability；
- 生成 LogicalJobPlan 和稳定 Fingerprint。

`validate` 不创建 Source/Sink，不发现远端 Schema，不枚举 Split，也不打开外部连接。

### Explain 边界

`explain` 可以创建 Source、发现 Source Schema、枚举 Split、应用字段映射并使用正式 JobPlanner 生成 JobGraph。

`explain` 不调用 SinkPreparer，不创建/删除/清空目标表，不执行目标端 DDL，不创建 SinkWriter，也不写数据。

## Structured Error

`FluxErrorCode` 是稳定错误目录，核心元数据：

```text
code
category
phase
retryable
retryScope
```

规划边界使用 `PlanningException`，错误参数只允许 role、connectorId、capability、format、reason 等安全值。REST 返回元数据和参数，不直接输出 cause message。

执行失败时，`JobExecutionAttempt` 从异常 cause chain 中提取结构化错误信息。RetryPolicy 先看错误是否允许重试，再看 Commit Evidence 是否证明数据可安全重放。

错误可重试不等于数据可重放。

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

状态角色固定为：

```text
JobExecutionState
  = 内存中的唯一可变控制状态

JobSnapshot + JobExecutionMetadata
  = 持久化与查询投影

Runtime Event
  = 生命周期时间线事实

History
  = 面向 Yak-Ops 的只读聚合视图
```

不再新增 `JobCheckpointState`、`JobReplayState`、`JobHistoryState` 等平行状态模型。

规划 API 是独立的只读控制边界，不创建 Job、Attempt 或 Worker state，也不进入 JobRuntimeScheduler。

Domain 不持有 `Thread`、`Future`、`Semaphore`、`ExecutorService` 或 framework `JobExecution`。

## Worker State Persistence

Worker state persistence 用于保存控制面状态，不保存可恢复的数据处理位置：

```text
JobRuntimeLifecycle.persistState
  -> JobRepository.save(snapshot, metadata)
  -> FileJobRepository
  -> JobStateFileStore
  -> <stateDirectory>/*.job.json
```

边界：

```text
Worker State Persistence
!= Data Checkpoint
!= Resume Point
!= Savepoint
```

- `stateVersion`：只记录业务状态转换版本。
- `stateRevision`：记录所有需要持久化的控制面变化，例如状态转换、日志身份、取消意图、Retry Attempt 创建。
- `stateRevision` 单调递增，旧状态不能覆盖新状态。
- 状态文件使用临时文件 + fsync + 原子替换。
- Worker 重启时，遗留非终态 Job 转为 `LOST`，不从 Split/offset 继续。

持久化格式：

```text
v1-v3  legacy: checkpointVersion
v4+    stateRevision
```

新文件只写 `stateRevision`；读取端继续接受 v1/v2/v3 的 `checkpointVersion`。

## Runtime Event Journal

Runtime Event 观察成功持久化的 Worker state：

```text
JobRuntimeLifecycle.persistState
  -> JobRepository.save(snapshot, metadata)
  -> EventPublishingJobRepository
       1. durable Worker state
       2. derive one lifecycle fact
       3. JobEventBus.publish
  -> JsonLineJobEventStore
       <stateDirectory>/job-events/<jobId>.jsonl
```

边界约束：

- `JobSnapshot` / `JobExecutionMetadata` 仍是控制面状态真相。
- Event Journal 只追加，不反向修改 Job 状态，不参与 Retry/Commit 决策。
- 只有 Worker state 保存成功后才派生事件。
- 每条事件使用 `stateRevision` 作为单 Job `sequence`，Retry 后继续递增。
- Event JSON 不包含 JobSpec、Connector options、SQL、Secret、异常正文、Prepared Connector 或私有 metadata。
- Event Journal 保持 Job/Attempt 生命周期粒度，不扩成 Task/Split 事件总线。

查询链路：

```text
GET /api/v1/jobs/{jobId}/events?afterSequence=N&limit=M
  -> JobEventRestService
  -> JobEventReader
  -> sequence-based page
```

## Retry

Retry 是新的 Attempt，不是把旧 Attempt 改活：

```text
FAILED Attempt #1
  -> RetryPolicy
  -> structured error permits retry
  -> commit evidence == SAFE
  -> Attempt #2
```

允许 Retry 的最低条件：

- Job 当前为 `FAILED`；
- 结构化错误没有明确声明不可重试；
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

如果一个类同时回答了两个以上问题，通常应该拆；如果一个新类只是重复表达已有 Job 状态，则不应该增加。
