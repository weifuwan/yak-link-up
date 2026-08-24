# Domain

## Job 定义链

```text
JobSpec -> JobDefinition -> PreparedJob -> JobGraph -> ExecutionGraph -> JobResult
```

- `JobSpec`：外部提交协议。
- `JobDefinition`：校验、归一化后的内部定义。
- `PreparedJob`：Connector 元数据准备完成后的输入。
- `JobGraph`：不可变物理拓扑。
- `ExecutionGraph`：单次运行状态。
- `JobResult`：一次 framework 执行结果。

## Worker Job

Worker 的 `jobId` 是稳定资源 ID。它不等于一次运行线程。

```text
Job
  ├── identity
  ├── lifecycle audit
  ├── checkpointVersion
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
- commit evidence；
- `retryAdvice`。

Attempt 不拥有线程和 framework 执行对象。

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

安全策略宁可拒绝，也不猜。

## Checkpoint

`stateVersion` 只记录业务状态转换。

`checkpointVersion` 记录所有需要持久化的变化，包括日志绑定、取消意图和 Retry Attempt 创建，用于防止旧 checkpoint 覆盖新状态。

Worker 重启后，遗留的非终态 Job 统一恢复为 `LOST`；不会自动执行 Retry。
