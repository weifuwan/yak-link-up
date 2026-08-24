# Link-Up

Link-Up 是一个轻量、可嵌入的离线数据同步引擎。它把数据同步拆成稳定的 API、运行时、Connector 和单节点 Worker 控制面，目标是：代码边界清楚、Connector 好扩展、任务可观察、失败可解释。

## 能做什么

- JDBC / HTTP Source，JDBC / Doris Sink 等 Connector 扩展。
- 单表、多表的有界批量同步。
- Source/Sink 并行执行、Split、Channel、Metrics、日志。
- 单节点 Worker：提交、查询、取消、持久化状态、重启恢复。
- Job / Attempt 模型：同一个 Job 可以保留多次执行尝试。
- 安全手动重试：只有明确证明“没有已提交或未知数据”的 FAILED Attempt 才允许重试。

## 核心模型

```text
JobSpec
  -> JobDefinition
  -> PreparedJob
  -> JobGraph
  -> ExecutionGraph
  -> JobResult
```

Worker 控制面：

```text
HTTP
  -> JobApplication
  -> JobApplicationService
     -> JobExecutionState
        -> Attempt #1 / #2 / ...
     -> JobRuntimeScheduler
     -> JobRepository
```

一次安全重试不会创建新 Job：

```text
job-100 / Attempt #1 FAILED
          -> RetryPolicy: SAFE
          -> Attempt #2 RUNNING
          -> SUCCEEDED
```

`LOST`、`CANCELED`、存在已提交数据、存在 unknown commit state、缺少 commit evidence 时，默认拒绝重试。

## 模块

| 模块 | 职责 |
| --- | --- |
| `link-up-api` | Connector 扩展契约和公共模型 |
| `link-up-framework` | Planning、JobGraph、ExecutionGraph、本地执行运行时 |
| `link-up-connectors` | JDBC / HTTP / Doris 等实现 |
| `link-up-server` | Worker 控制面、REST、持久化、Attempt/Retry |
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
POST   /api/v1/jobs
GET    /api/v1/jobs/{jobId}
GET    /api/v1/jobs/{jobId}/logs
GET    /api/v1/jobs/{jobId}/metrics
DELETE /api/v1/jobs/{jobId}
POST   /api/v1/jobs/{jobId}/retry
GET    /api/v1/connectors
GET    /api/v1/node
```

Retry 请求必须重新携带与原 Job 完全一致的结构化提交内容。Worker 不会为了 Retry 把数据库密码、Token 或完整 JobSpec 写进 checkpoint。

## 文档

只保留对开发真正有用的几份：

- [ARCHITECTURE.md](ARCHITECTURE.md)：系统边界和主流程。
- [DOMAIN.md](DOMAIN.md)：Job、Attempt、Graph、状态语义。
- [DEPENDENCIES.md](DEPENDENCIES.md)：模块依赖规则。
- [REQUIREMENTS.md](REQUIREMENTS.md)：项目范围和非目标。
- [CODE_STYLE.md](CODE_STYLE.md)：代码和包命名规范。
- [REVIEW.md](REVIEW.md)：提交前检查清单。

文档原则：**写当前事实，不写历史流水账；能用一张图说清，就不要写十页。**
