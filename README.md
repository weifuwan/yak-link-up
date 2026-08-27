# Link-Up

Link-Up 是一个轻量、可嵌入的**离线批量数据同步引擎**。它把数据同步拆成稳定的 API、运行时、Connector 和单节点 Worker 控制面，目标是：代码边界清楚、Connector 好扩展、任务可观察、失败可解释。

> Link-Up 不做实时同步，也不提供 Flink 式 checkpoint/savepoint 或 Split offset 自动恢复。

## 能做什么

- JDBC / HTTP Source，JDBC / Doris Sink 等 Connector 扩展。
- 单表、多表的 bounded batch sync。
- Source/Sink 并行执行、Split、Channel、Metrics、日志。
- 单节点 Worker：提交、查询、取消、状态持久化、重启 LOST 恢复。
- Job / Attempt 模型：同一个 Job 可以保留多次执行尝试。
- 安全手动 Retry：只有明确证明“没有已提交或未知数据”的 FAILED Attempt 才允许重试。
- 提交前 Validate / Explain。
- Required / Preferred Capability 检查。
- 结构化错误。
- Job / Attempt 级 Runtime Event Journal。
- 面向 Yak-Ops 的 Execution History 只读投影。

## 核心模型

```text
JobSpec
  -> JobDefinition
  -> LogicalJobPlan
  -> CapabilityNegotiation
  -> PreparedJob
  -> JobGraph
  -> PhysicalJobPlan (Explain projection)
  -> ExecutionGraph
  -> JobResult
```

`JobGraph` 始终是正式物理执行计划。`PhysicalJobPlan` 只从同一个 `JobGraph` 投影出 Secret-safe 的 Explain 结果，不维护第二套执行真相。

Worker 控制面：

```text
HTTP
  -> JobApplication
  -> JobApplicationService
     -> JobExecutionState
        -> Attempt #1 / #2 / ...
     -> JobRuntimeScheduler
     -> JobRepository
        -> JobSnapshot / JobExecutionMetadata
        -> Worker State Files
        -> Runtime Event Journal (observer)
```

## Worker State Persistence

这里最容易和流处理概念混淆，所以边界固定为：

```text
Worker State Persistence
!= Data Checkpoint
!= Resume Point
!= Savepoint
```

Worker state 只保存控制面事实，例如：

```text
Job status
Attempt history
stateRevision
cancellationRequested
runId / log identity
structured error metadata
commit evidence
```

`stateRevision` 是单 Job 的状态修订号。它用于阻止旧状态覆盖新状态，同时作为 Runtime Event 的顺序来源。

Worker 重启时：

```text
RUNNING / QUEUED / SUBMITTED
          -> LOST
```

不会：

```text
从 Split / offset / batch 继续执行
自动重放 LOST Job
跨 Worker failover
```

新状态文件格式为 v4，写 `stateRevision`。旧 v1/v2/v3 文件中的 `checkpointVersion` 仍兼容读取；`/history` 也暂时保留同名 legacy 字段，新的调用方应使用 `stateRevision`。

## Plan / Explain

```text
POST /api/v1/jobs/validate
POST /api/v1/jobs/explain
```

- `validate` 做协议编译、Factory 发现、OptionRule 校验和显式 Capability 检查，不创建 Source/Sink，不访问外部系统。
- `explain` 会创建 Source、发现 Source Schema 并枚举 Split，以生成真实 `JobGraph`。
- `explain` 不调用 SinkPreparer，因此不会建表、清表、执行 DDL、创建 Writer 或写数据。
- 正式 Runtime 与 Validate/Explain 共用同一套 Capability 判断。
- Explain 不输出 Connector options、Secret、Prepared Connector、ClassLoader 或 Connector 私有 metadata。

## Capability

Job 可以声明：

```text
required  -> 缺失即拒绝
preferred -> 缺失时允许降级并返回 Warning
```

Capability 只服务离线同步的真实执行语义，不演化成通用规则引擎。

## Structured Error

规划和 Connector API 错误使用稳定元数据，而不是让调用方匹配异常字符串：

```text
code
category
phase
retryable
retryScope
parameters
```

错误参数只允许安全身份和枚举，不放 Connector options、SQL、Secret 或原始异常正文。

错误是否值得重试，与数据是否能安全重放是两个条件。最终 Retry 仍由 Commit Evidence 决定。

## Runtime Event Journal

Worker state 保存成功后，才派生对应的 Job 生命周期事件：

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

事件文件：

```text
<stateDirectory>/job-events/<jobId>.jsonl
```

Event `sequence` 使用同一个 Job 的 `stateRevision`。Event Journal 只负责时间线和最终执行事实，不反向驱动状态机，不参与 Retry/Commit 决策，也不继续扩成 Task/Split 级事件系统。

查询：

```text
GET /api/v1/jobs/{jobId}/events?afterSequence=0&limit=200
```

## Execution History

```text
GET /api/v1/jobs/{jobId}/history?afterSequence=0&limit=200
```

History 只是 Yak-Ops 的只读聚合视图：

```text
events
attempts
execution metrics
pipeline/task final facts
structured error
commit evidence
stateRevision
```

History 不参与状态恢复、调度、Retry 或 Commit 决策。

## Safe Retry

一次安全重试不会创建新 Job：

```text
job-100 / Attempt #1 FAILED
          -> RetryPolicy: SAFE
          -> Attempt #2 RUNNING
          -> SUCCEEDED
```

`LOST`、`CANCELED`、存在 committed data、unknown state、缺少 commit evidence，或者结构化错误明确声明不可重试时，默认拒绝 Retry。

## 模块

| 模块 | 职责 |
| --- | --- |
| `link-up-api` | Connector 扩展契约、Capability、Structured Error 公共模型 |
| `link-up-framework` | Planning、JobGraph、ExecutionGraph、本地执行运行时 |
| `link-up-connectors` | JDBC / HTTP / Doris 等实现 |
| `link-up-server` | Worker 控制面、REST、状态持久化、Attempt/Retry、Runtime Event、History |
| `link-up-launcher` | 本地命令行组合入口 |
| `link-up-dist` | 分发包 |
| `link-up-bom` | 依赖版本管理 |

## 构建

要求 JDK 8+、Maven 3.8.1+：

```bash
mvn --batch-mode clean verify
```

本地运行示例：

```bash
mvn -pl link-up-launcher -am compile exec:java \
  -Dexec.mainClass=com.link.up.launcher.LocalSyncLauncher \
  -Dexec.args=link-up-launcher/examples/jdbc-single-table.conf
```

Standalone Worker 默认把控制面状态保存到：

```text
data/worker-state
```

可通过 `--state-dir` 指定目录。

## Worker API

```text
POST   /api/v1/jobs/validate
POST   /api/v1/jobs/explain
POST   /api/v1/jobs
GET    /api/v1/jobs/{jobId}
GET    /api/v1/jobs/{jobId}/events
GET    /api/v1/jobs/{jobId}/history
GET    /api/v1/jobs/{jobId}/logs
GET    /api/v1/jobs/{jobId}/metrics
DELETE /api/v1/jobs/{jobId}
POST   /api/v1/jobs/{jobId}/retry
GET    /api/v1/connectors
GET    /api/v1/node
```

Retry 请求必须重新携带与原 Job 完全一致的结构化提交内容。Worker 不会为了 Retry 或 History 持久化数据库密码、Token 或完整 JobSpec。

## 文档

- [ARCHITECTURE.md](ARCHITECTURE.md)：系统边界和主流程。
- [DOMAIN.md](DOMAIN.md)：Job、Attempt、Capability、错误和状态语义。
- [DEPENDENCIES.md](DEPENDENCIES.md)：模块依赖规则。
- [REQUIREMENTS.md](REQUIREMENTS.md)：项目范围和非目标。
- [CODE_STYLE.md](CODE_STYLE.md)：代码和包命名规范。
- [REVIEW.md](REVIEW.md)：提交前检查清单。
