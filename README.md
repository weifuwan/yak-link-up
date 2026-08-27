# Link-Up

Link-Up 是一个轻量、可嵌入的**离线批量数据同步引擎**。它专注 bounded batch sync：Connector 好扩展、任务能解释、失败能定位，但不复制 Flink/Spark 的流式和分布式复杂度。

> Link-Up 不做实时同步，不提供 Flink 式 checkpoint/savepoint，也不从 Split/offset 自动恢复执行。

## 能做什么

- JDBC / HTTP Source，JDBC / Doris Sink 等 Connector 扩展。
- 单表、多表离线批量同步。
- Source/Sink 并行执行、Split、Channel、Metrics、日志。
- 单节点 Worker：提交、查询、取消、状态持久化、重启后非终态 Job 转 `LOST`。
- Job / Attempt 模型和基于 Commit Evidence 的安全手动 Retry。
- Validate / Explain。
- 有限的 Connector Capability 检查。
- 结构化错误。
- Job / Attempt 生命周期 Event Journal。

## 核心执行链

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

`JobGraph` 是正式物理计划。`PhysicalJobPlan` 只投影 Explain 信息，不维护第二套执行真相。

Worker 控制面：

```text
HTTP
  -> JobApplicationService
     -> JobExecutionState
     -> JobRuntimeScheduler
     -> JobRepository
        -> JobSnapshot / JobExecutionMetadata
        -> Worker State Files
        -> Runtime Event Journal
```

## Worker State Persistence

```text
Worker State Persistence
!= Data Checkpoint
!= Resume Point
!= Savepoint
```

Worker state 只保存控制面事实：

```text
Job status
Attempt history
stateRevision
cancellationRequested
runId / log identity
structured error metadata
commit evidence
```

`stateRevision` 是单 Job 的状态修订号，用于阻止旧状态覆盖新状态，同时作为 Runtime Event 的顺序来源。

Worker 重启时：

```text
RUNNING / QUEUED / SUBMITTED
          -> LOST
```

不会从 Split、offset、batch 继续执行，也不会自动重放 LOST Job。

状态文件 v4 写 `stateRevision`；旧 v1/v2/v3 的 `checkpointVersion` 只用于兼容读取。

## Plan / Explain

```text
POST /api/v1/jobs/validate
POST /api/v1/jobs/explain
```

- `validate` 做协议、Factory、OptionRule 和 Capability 校验，不创建 Source/Sink，不访问外部系统。
- `explain` 可以创建 Source、发现 Schema、枚举 Split，并使用正式 `JobPlanner` 生成 `JobGraph`。
- `explain` 不调用 SinkPreparer，不执行目标端 DDL，不创建 Writer，也不写数据。
- Explain 不输出 Connector options、Secret、Prepared Connector、ClassLoader 或 Connector 私有 metadata。

## Capability

Capability 只做离线同步所需的有限枚举匹配：

```text
supported  -> Connector 声明支持
required   -> Job 显式要求 + 真实拓扑必须要求
preferred  -> 缺失时仅 Warning
```

协商结果只有：

```text
SATISFIED
DEGRADED   # preferred 缺失
REJECTED   # required 缺失
```

多表拓扑会把 `MULTI_TABLE` 直接并入 Source/Sink 的 `required`。不再维护 `observed`、`undeclaredObserved` 或通用 Capability 规则图。

当前 Capability 集合只覆盖离线执行需要：

```text
TABLE_SCHEMA_DISCOVERY
MULTI_TABLE
CUSTOM_SQL
PARTITION_SPLIT
UPSERT
AUTO_CREATE_TABLE
DIRTY_DATA_HANDLING
TWO_PHASE_COMMIT
```

## Runtime Event Journal

Worker state 保存成功后，才派生 Job/Attempt 生命周期事件：

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

Event v2 只保存生命周期事实，不再复制 Pipeline/Task/Metrics 快照。旧 v1 事件即使带 `execution` 字段仍可读取，新事件不会继续写该字段。

Event Journal 不扩成：

```text
TASK_*
SPLIT_*
BATCH_*
PIPELINE_PROGRESS_*
```

查询：

```text
GET /api/v1/jobs/{jobId}/events?afterSequence=0&limit=200
```

## Job 详情

不再维护单独的 History 聚合协议。Yak-Ops 直接使用稳定的任务资源接口：

```text
GET /api/v1/jobs/{jobId}
GET /api/v1/jobs/{jobId}/pipelines
GET /api/v1/jobs/{jobId}/tasks
GET /api/v1/jobs/{jobId}/metrics
GET /api/v1/jobs/{jobId}/events
GET /api/v1/jobs/{jobId}/logs
```

当前进程中可以查看 Pipeline/Task/Metrics 细节；Worker 重启后的持久历史只承诺 Job/Attempt、错误、Commit Evidence 和生命周期事件，不承诺恢复详细 Pipeline/Task 快照。

## Safe Retry

```text
FAILED Attempt
  -> error permits retry
  -> Commit Evidence proves zero committed/unknown data
  -> new Attempt on same jobId
```

`LOST`、`CANCELED`、存在 committed data、unknown state、缺少 commit evidence，或者结构化错误明确不可重试时，默认拒绝 Retry。

## 模块

| 模块 | 职责 |
| --- | --- |
| `link-up-api` | Connector 契约、Capability、Structured Error |
| `link-up-framework` | Planning、JobGraph、ExecutionGraph、本地执行运行时 |
| `link-up-connectors` | JDBC / HTTP / Doris 等实现 |
| `link-up-server` | Worker 控制面、REST、状态持久化、Attempt/Retry、Event Journal |
| `link-up-launcher` | 本地命令行组合入口 |
| `link-up-dist` | 分发包 |
| `link-up-bom` | 依赖版本管理 |

## 构建

要求 JDK 8+、Maven 3.8.1+：

```bash
mvn --batch-mode clean verify
```

Standalone Worker 默认把控制面状态保存到：

```text
data/worker-state
```

## Worker API

```text
POST   /api/v1/jobs/validate
POST   /api/v1/jobs/explain
POST   /api/v1/jobs
GET    /api/v1/jobs/{jobId}
GET    /api/v1/jobs/{jobId}/pipelines
GET    /api/v1/jobs/{jobId}/tasks
GET    /api/v1/jobs/{jobId}/metrics
GET    /api/v1/jobs/{jobId}/events
GET    /api/v1/jobs/{jobId}/logs
DELETE /api/v1/jobs/{jobId}
POST   /api/v1/jobs/{jobId}/retry
GET    /api/v1/connectors
GET    /api/v1/node
```

## 文档

- [ARCHITECTURE.md](ARCHITECTURE.md)：系统边界和主流程。
- [DOMAIN.md](DOMAIN.md)：Job、Attempt、Capability、错误和状态语义。
- [DEPENDENCIES.md](DEPENDENCIES.md)：模块依赖规则。
- [REQUIREMENTS.md](REQUIREMENTS.md)：范围与明确非目标。
- [CODE_STYLE.md](CODE_STYLE.md)：代码和包命名规范。
- [REVIEW.md](REVIEW.md)：提交前检查清单。
