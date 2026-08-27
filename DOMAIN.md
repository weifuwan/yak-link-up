# Domain

## Job 定义链

```text
JobSpec
  -> JobDefinition
  -> LogicalJobPlan
  -> CapabilityNegotiation
  -> PreparedJob
  -> JobGraph
  -> ExecutionGraph
  -> JobResult
```

- `JobSpec`：外部提交协议，可声明 Required/Preferred Capability。
- `JobDefinition`：校验、归一化后的内部定义。
- `LogicalJobPlan`：Secret-safe 用户意图和稳定 Fingerprint。
- `CapabilityNegotiation`：任务要求与 Connector 声明的匹配结果。
- `PreparedJob`：Connector 元数据准备完成后的输入。
- `JobGraph`：不可变物理拓扑。
- `ExecutionGraph`：单次运行状态。
- `JobResult`：一次 framework 执行结果。

## Capability

Capability 是稳定执行语义，不是前端控件：

```text
supported       Connector 声明可提供
required        Job 显式必须具备
preferred       Job 希望具备，缺失可降级
derivedRequired 根据真实拓扑推导的必须能力
observed        规划过程实际观察到的能力事实
```

协商状态：

| Status | 含义 |
| --- | --- |
| `SATISFIED` | Required 全部满足，没有降级或声明缺口 |
| `DEGRADED` | Required 满足，但 Preferred 缺失或观察到未声明能力 |
| `REJECTED` | 至少一个 Required Capability 缺失 |

多表 Source 会为 Source 和 Sink 派生 `MULTI_TABLE` Required Capability。该检查发生在 Sink Preparation 之前，避免不兼容任务产生目标端副作用。

## Worker Job

Worker 的 `jobId` 是稳定资源 ID。它不等于一次运行线程。

```text
Job
  ├── identity
  ├── lifecycle audit
  ├── stateRevision
  └── attempts[]
```

当前可见状态：

```text
CREATED -> SUBMITTED -> QUEUED -> RUNNING
                                  -> SUCCEEDED
                                  -> FAILED
                                  -> CANCELED
                                  -> LOST
```

普通状态转换中终态不可逆。只有显式安全 Retry 可以走专用转换：

```text
FAILED -> SUBMITTED
```

它表示创建了一个新的 Attempt，而不是复活旧 Attempt。

## Attempt

```text
Attempt #N
  CREATED -> QUEUED -> RUNNING
                      -> SUCCEEDED / FAILED / CANCELED / LOST
```

Attempt 记录：

- `attemptNumber` / `attemptId`；
- queue/start/end 时间；
- `runId` / 日志文件；
- failure type/message；
- structured error code/category/phase；
- failure retryable/retryScope；
- commit evidence；
- `retryAdvice`。

Attempt 不拥有线程和 framework 执行对象。

## 状态模型边界

只保留四个角色，不再新增平行的 Job state 模型：

```text
JobExecutionState
  = 内存中的可变运行/控制状态

JobSnapshot + JobExecutionMetadata
  = Worker 持久化与查询所需的状态投影

Runtime Event
  = 已成功持久化后的生命周期时间线事实

History
  = 面向 Yak-Ops 的只读聚合视图
```

Runtime Event 和 History 都不能反向驱动 Job 状态机。

## Structured Error

稳定错误由以下字段组成：

```text
code
category
phase
retryable
retryScope
parameters
```

当前规划错误目录：

| Code | 含义 |
| --- | --- |
| `PLAN-001` | 规划请求非法 |
| `PLAN-002` | Job 定义编译失败 |
| `PLAN-003` | Connector 无法解析 |
| `PLAN-004` | Connector options 非法 |
| `PLAN-005` | Required Capability 缺失 |
| `PLAN-006` | Source Preparation / Schema Discovery 失败 |
| `PLAN-007` | Split Discovery 失败 |
| `PLAN-008` | Physical Planning 失败 |
| `PLAN-009` | Sink Preparation 失败 |
| `PLAN-010` | 未预期的内部规划失败 |

错误参数只放安全身份和枚举。Secret、Connector options、SQL、完整 URL 和原始异常正文不得进入结构化参数。

## Runtime Event

Runtime Event 是一次已经成功持久化的 Worker 生命周期事实，不是新的状态机：

```text
JobEventEnvelope
  ├── schemaVersion
  ├── eventId
  ├── jobId
  ├── attemptId / attemptNumber
  ├── sequence == stateRevision
  ├── occurredAtMillis
  └── JobRuntimeEvent
```

当前稳定事件：

```text
JOB_SUBMITTED
JOB_RETRY_CREATED
JOB_QUEUED
JOB_STARTED
JOB_LOG_CREATED
JOB_CANCEL_REQUESTED
JOB_SUCCEEDED
JOB_FAILED
JOB_CANCELED
JOB_LOST
```

语义约束：

- Event 只在对应 Worker state 保存成功后发布。
- 同一个 `jobId` 的 `sequence` 单调递增，Retry 不重置序号。
- Event Journal 允许序号缺口，因为观察器写入失败不能反向让 Job 失败。
- 重复或晚到的旧序号不会再次追加。
- Event 不参与状态恢复、RetryPolicy 或 Commit 判定。
- Event 不保存用户配置、SQL、Secret 或异常正文。

## Retry Decision

RetryPolicy 输出显式决策码：

| Code | 含义 |
| --- | --- |
| `SAFE_NO_DATA_COMMITTED` | 可安全重试 |
| `JOB_ACTIVE` | 任务仍在运行 |
| `ALREADY_SUCCEEDED` | 已成功，不重试 |
| `CANCELED_OUTCOME` | 取消存在提交竞态 |
| `LOST_OUTCOME_UNKNOWN` | 最终结果未知 |
| `EVIDENCE_UNAVAILABLE` | 无法证明安全 |
| `UNKNOWN_COMMIT_STATE` | 存在未知提交状态 |
| `DATA_ALREADY_COMMITTED` | 已确认提交数据 |
| `NON_RETRYABLE_FAILURE` | 结构化错误明确禁止重试 |

错误可重试和数据可重放是两个条件：

```text
structured error allows retry
AND
commit evidence proves zero committed / unknown data
```

安全策略宁可拒绝，也不猜。

## Worker State Persistence

`stateVersion` 只记录业务状态转换。

`stateRevision` 记录所有需要持久化的控制面变化，包括日志绑定、取消意图和 Retry Attempt 创建。它用于防止旧 Worker state 覆盖新状态，也作为 Runtime Event 的单 Job 序号。

```text
Worker State Persistence
!= Flink Checkpoint
!= Split/Offset Resume
```

持久化 `formatVersion = 4` 开始写 `stateRevision`。v1/v2/v3 文件中的 `checkpointVersion` 只作为 legacy 字段兼容读取，不再作为新格式术语继续扩散。

Worker 重启后，遗留的非终态 Job 统一恢复为 `LOST`；不会自动执行 Retry，也不会从 Split/offset 继续。
