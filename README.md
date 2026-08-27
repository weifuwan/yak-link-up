# Link-Up

Link-Up 是一个轻量、可嵌入的离线数据同步引擎。它把数据同步拆成稳定的 API、运行时、Connector 和单节点 Worker 控制面，目标是：代码边界清楚、Connector 好扩展、任务可观察、失败可解释。

## 能做什么

- JDBC / HTTP Source，JDBC / Doris Sink 等 Connector 扩展。
- 单表、多表的有界批量同步。
- Source/Sink 并行执行、Split、Channel、Metrics、日志。
- 单节点 Worker：提交、查询、取消、持久化状态、重启恢复。
- Job / Attempt 模型：同一个 Job 可以保留多次执行尝试。
- 安全手动重试：只有明确证明“没有已提交或未知数据”的 FAILED Attempt 才允许重试。
- 提交前校验和 Explain：查看规范化逻辑计划、实际 Source Split/Task 拓扑与稳定指纹。
- Required / Preferred Capability 协商：在 Connector I/O 和 Sink 副作用前发现不兼容任务。
- 结构化错误：稳定 code、category、phase、retryable、retryScope 和安全参数。
- 追加式 Runtime Event Journal：按 Job/Attempt 回看提交、排队、运行、取消与终态时间线。
- Execution History：把 Event、Attempt、Pipeline、Task 和执行指标投影成 Yak-Ops 可直接消费的历史视图。

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

`JobGraph` 始终是正式物理执行计划。`PhysicalJobPlan` 只从同一个 `JobGraph` 投影出可序列化、Secret-safe 的 Explain 结果，不维护第二套执行真相。

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
        -> Runtime Event Journal (observer)
```

一次安全重试不会创建新 Job：

```text
job-100 / Attempt #1 FAILED
          -> RetryPolicy: SAFE
          -> Attempt #2 RUNNING
          -> SUCCEEDED
```

`LOST`、`CANCELED`、存在已提交数据、存在 unknown commit state、缺少 commit evidence，或者结构化错误明确声明不可重试时，默认拒绝 Retry。

## Plan / Explain

```text
POST /api/v1/jobs/validate
POST /api/v1/jobs/explain
```

两个接口都接收与结构化提交一致的 JSON 外壳，只要求 `jobSpec` 或 `hocon` 二选一；规划时不要求 `externalExecutionId`、`idempotencyKey` 和 `definitionVersion`。

边界约束：

- `validate` 做协议编译、Factory 发现、OptionRule 校验和显式 Capability 协商，不创建 Source/Sink，不访问外部系统。
- `explain` 会创建 Source、发现 Source Schema 并枚举 Split，以生成真实 `JobGraph`。
- `explain` 不调用 `SinkPreparer`，因此不会建表、清表、执行 DDL、创建 Writer 或写数据。
- 正式 Runtime 与 Validate/Explain 共用同一个 `CapabilityNegotiator`，不会出现控制面通过、运行时绕过的第二套规则。
- Source Schema 发现后，如果实际拓扑是多表任务，会自动为 Source 和 Sink 派生 `MULTI_TABLE` Required Capability，并在 Sink DDL/清理之前拒绝不兼容组合。
- Explain 响应不包含 Connector options、`ReadonlyConfig`、Prepared Connector、ClassLoader 或 Connector 私有 metadata。
- Connector 完整配置和 Capability Intent 只参与 SHA-256 计划指纹，Secret 不会出现在逻辑计划、物理计划、诊断或文本 Explain 中。

结构化提交可以声明 Required 和 Preferred Capability：

```json
{
  "jobSpec": {
    "apiVersion": "link-up/v1",
    "kind": "BatchSyncJob",
    "name": "orders-sync",
    "source": {
      "connectorId": "jdbc",
      "options": {}
    },
    "sink": {
      "connectorId": "doris",
      "options": {}
    },
    "capabilities": {
      "source": {
        "required": ["TABLE_SCHEMA_DISCOVERY"],
        "preferred": ["PARTITION_SPLIT"]
      },
      "sink": {
        "required": ["TWO_PHASE_COMMIT"]
      }
    }
  }
}
```

语义：

- Required 缺失：返回结构化错误并拒绝规划/执行。
- Preferred 缺失：计划仍然有效，状态为 `DEGRADED`，并返回 Warning Diagnostic。
- 未声明但执行拓扑已经使用的能力：返回声明不完整 Warning，便于逐步收紧第三方 Connector Contract。

HOCON 使用同样的路径：

```hocon
capabilities {
  source.required = [TABLE_SCHEMA_DISCOVERY]
  source.preferred = [PARTITION_SPLIT]
  sink.required = [TWO_PHASE_COMMIT]
}
```

## Structured Error

规划和 Connector API 错误使用稳定元数据，而不是让调用方匹配异常字符串：

```json
{
  "code": "PLAN-005",
  "message": "A required Connector capability is missing",
  "requestId": "...",
  "category": "CAPABILITY",
  "phase": "CAPABILITY_NEGOTIATION",
  "retryable": false,
  "retryScope": "NONE",
  "parameters": {
    "role": "SINK",
    "connectorId": "doris",
    "capability": "TWO_PHASE_COMMIT"
  }
}
```

规则：

- 稳定 code 用于控制面分支、聚合告警和兼容判断。
- category / phase 描述问题边界。
- retryable / retryScope 只描述错误本身是否值得重试；数据是否已经提交仍由 Commit Evidence 决定。
- parameters 只允许安全身份和枚举，不放 Connector options、SQL、Secret 或原始异常正文。
- Attempt 会持久化结构化错误元数据；Worker 重启后 RetryPolicy 仍能拒绝明确的不可重试错误。

## Runtime Event Journal

每次持久化生命周期 checkpoint 后，Worker 会把对应事实追加到：

```text
<stateDirectory>/job-events/<jobId>.jsonl
```

当前事件包括：

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

事件使用 `checkpointVersion` 作为单 Job 单调递增序号。查询接口采用排他游标：

```text
GET /api/v1/jobs/{jobId}/events?afterSequence=0&limit=200
```

当前阶段仍以 `JobSnapshot` 和 `JobExecutionMetadata` 为控制面状态真相。Event Journal 只负责可观察历史，不反向驱动状态机；事件监听器或文件写入失败会被隔离，不改变任务执行、Commit 或 Retry 语义。

终态事件会附带一份 `JobExecutionFacts`：只保存 Pipeline/Task 身份、状态和数值指标，不新增事件序号。这样仍保持“一次 durable checkpoint = 一个 sequence”，同时正常结束的任务在 Worker 重启后仍能恢复最终执行事实。

安全边界：事件不保存 JobSpec、Connector options、SQL、Secret、异常正文、日志路径、当前表/分片或 Connector 私有 metadata。

## Execution History

History API 把事件时间线、Attempt 历史、结构化错误和最终/当前执行事实组合成一个稳定只读视图：

```text
GET /api/v1/jobs/{jobId}/history?afterSequence=0&limit=200
```

返回的 `apiVersion` 为 `link-up-job-history/v1`，核心内容包括：

```text
events          # sequence 游标分页的 durable lifecycle facts
attempts        # Attempt 状态、结构化错误和 Commit Evidence
execution       # Metrics + Pipeline + Task 安全投影
nextSequence
hasMore
completed
```

运行中的 Job 优先使用当前 `JobSnapshot` 生成 execution；Worker 重启后如果基础快照没有 Pipeline/Task 细节，则从终态 Event Journal 中恢复最近一次 `JobExecutionFacts`。History 是观察投影，不参与状态恢复、调度、Retry 或 Commit 决策。

为了保持协议可安全暴露给 Yak-Ops，History 不返回 failure message、retryAdvice、jobLogFile、Connector 物理表地址、currentTable/currentSplit、SQL 或任意 Connector options。

## 模块

| 模块 | 职责 |
| --- | --- |
| `link-up-api` | Connector 扩展契约、Capability、Structured Error 公共模型 |
| `link-up-framework` | Planning、Capability Negotiation、JobGraph、ExecutionGraph、本地执行运行时 |
| `link-up-connectors` | JDBC / HTTP / Doris 等实现 |
| `link-up-server` | Worker 控制面、REST、持久化、Attempt/Retry、Runtime Event Journal、Execution History |
| `link-up-launcher` | 本地命令行组合入口 |
| `link-up-dist` | 分发包 |
| `link-up-bom` | 依赖版本管理 |

## 构建

要求 JDK 8+、Maven 3.8.1+：

```bash
mvn --batch-mode clean verify
```

本地直接运行示例：

```bash
mvn -pl link-up-launcher -am compile exec:java \
  -Dexec.mainClass=com.link.up.launcher.LocalSyncLauncher \
  -Dexec.args=link-up-launcher/examples/jdbc-single-table.conf
```

Standalone Worker 默认把控制面 checkpoint 保存到：

```text
data/worker-state
```

可通过 `--state-dir` 指定独立持久化目录。

## Worker API

常用接口：

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

Retry 请求必须重新携带与原 Job 完全一致的结构化提交内容。Worker 不会为了 Retry 或 History 把数据库密码、Token 或完整 JobSpec 写进 checkpoint 或 Runtime Event Journal。

## 文档

只保留对开发真正有用的几份：

- [ARCHITECTURE.md](ARCHITECTURE.md)：系统边界和主流程。
- [DOMAIN.md](DOMAIN.md)：Job、Attempt、Capability、错误和状态语义。
- [DEPENDENCIES.md](DEPENDENCIES.md)：模块依赖规则。
- [REQUIREMENTS.md](REQUIREMENTS.md)：项目范围和非目标。
- [CODE_STYLE.md](CODE_STYLE.md)：代码和包命名规范。
- [REVIEW.md](REVIEW.md)：提交前检查清单。

文档原则：**写当前事实，不写历史流水账；能用一张图说清，就不要写十页。**
