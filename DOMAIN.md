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

- `JobSpec`：外部提交协议。
- `JobDefinition`：校验、归一化后的内部定义。
- `LogicalJobPlan`：Secret-safe 用户意图和稳定 Fingerprint。
- `CapabilityNegotiation`：有限能力集合检查结果。
- `JobGraph`：不可变物理拓扑。
- `ExecutionGraph`：单次运行状态。

## Capability

Capability 只保留五组信息：

```text
supported
required      # 显式 + 拓扑派生后合并
preferred
missingRequired
missingPreferred
```

状态：

| Status | 含义 |
| --- | --- |
| `SATISFIED` | required/preferred 均满足 |
| `DEGRADED` | required 满足，preferred 缺失 |
| `REJECTED` | required 缺失 |

多表 Source 会把 `MULTI_TABLE` 直接并入 Source/Sink 的 required。Domain 不维护 `observed`、`undeclaredObserved` 或 Capability 依赖图。

## Worker Job

`jobId` 是稳定资源 ID，不等于一次运行线程。

```text
Job
  ├── identity
  ├── lifecycle audit
  ├── stateRevision
  └── attempts[]
```

状态：

```text
CREATED -> SUBMITTED -> QUEUED -> RUNNING
                                  -> SUCCEEDED
                                  -> FAILED
                                  -> CANCELED
                                  -> LOST
```

显式安全 Retry 使用专用转换 `FAILED -> SUBMITTED`，含义是创建新 Attempt。

## Attempt

Attempt 记录：

- `attemptNumber` / `attemptId`；
- queue/start/end 时间；
- runId / 日志身份；
- failure type/message；
- structured error；
- commit evidence；
- retry advice。

Attempt 不拥有线程和 framework 执行对象。

## 状态模型边界

```text
JobExecutionState
  = 内存中的可变控制状态

JobSnapshot + JobExecutionMetadata
  = Worker 持久化与查询状态

Runtime Event
  = 已成功持久化后的 Job/Attempt 生命周期事实
```

REST DTO 只做查询投影，不建立第二套状态真相。

## Runtime Event

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

稳定事件只允许：

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

Event schema v2 不保存执行快照。旧 v1 `execution` 字段兼容读取并忽略。

禁止扩成：

```text
TASK_*
SPLIT_*
BATCH_*
```

## Retry Decision

Retry 必须同时满足：

```text
structured error allows retry
AND
commit evidence proves zero committed / unknown data
```

安全策略宁可拒绝，也不猜。

## Worker State Persistence

`stateVersion` 只记录业务状态转换。

`stateRevision` 记录所有需要持久化的控制面变化，并作为 Runtime Event 的单 Job序号。

```text
Worker State Persistence
!= Flink Checkpoint
!= Split/Offset Resume
```

v4 写 `stateRevision`；v1/v2/v3 的 `checkpointVersion` 只作为 legacy 字段读取。

Worker 重启后非终态 Job 转 `LOST`，不会从 Split/offset 继续。
