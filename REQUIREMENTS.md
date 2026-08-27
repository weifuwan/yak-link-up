# Requirements

## 产品定位

Link-Up 是离线批量数据同步引擎，不是流处理引擎，也不是通用分布式计算平台。

当前优先级：**可靠、清楚、可扩展**，高于“功能看起来很多”。

## 必须支持

- 结构化 JobSpec / HOCON 提交。
- 单表、多表 bounded batch sync。
- Source Split + SourceReader。
- SinkWriter / SinkPreparer。
- Source/Sink/Pipeline 并行度。
- 任务取消、日志、Metrics、Pipeline/Task 当前状态查询。
- Worker 幂等提交和 externalExecutionId 查询。
- Worker 状态持久化和重启 LOST 恢复。
- Job / Attempt 持久历史与生命周期事件。
- 基于 Commit Evidence 的安全手动 Retry。
- Connector Schema / preflight。
- 有限的离线 Connector Capability 检查。

## 历史与可观察性边界

持久历史只承诺：

```text
Job status
Attempt history
structured error
commit evidence
Job/Attempt lifecycle events
```

Pipeline/Task/Metrics 是当前运行 read model，不建设独立 History Server，也不为了重启后恢复详细执行视图而复制一份终态快照到 Event Journal。

## Worker State Persistence

```text
Worker State Persistence
!= Data Checkpoint
!= Resume Point
!= Savepoint
```

Worker 重启时，遗留非终态 Job 统一转 `LOST`。不从 Split、offset 或 batch 自动继续。

## Capability 边界

Capability 只允许有限枚举集合检查：

```text
supported
required
preferred
missing
```

真实拓扑可以派生必要 Required（当前为 `MULTI_TABLE`），但不建设 observed graph、规则 DSL、fallback/cost/priority 系统。

## Retry 要求

- 只通过显式接口触发。
- 复用同一个 `jobId`，新增 Attempt。
- 请求必须与原 Job digest 一致。
- 已提交数据、unknown state、LOST、CANCELED 默认拒绝。
- Worker 不持久化密码、Token、完整 JobSpec 来实现 Retry。

## 非目标

当前不做：

- 实时同步 / CDC runtime；
- 分布式 Scheduler / ResourceManager；
- Flink 式 checkpoint/savepoint；
- Split/offset resume；
- LOST 自动重放；
- 跨 Worker failover；
- 全局 exactly-once runtime；
- Task/Split/Batch 级 Event Bus；
- 独立 History Server / Event Replay Engine；
- Capability 规则 DSL / 依赖图 / 成本优化；
- 为了未来可能使用而预留 streaming/checkpoint capability；
- 为了架构好看拆大量 Maven 模块。

## 非功能要求

- Java 8+。
- Maven 3.8.1+。
- Connector 不依赖 framework。
- JobGraph 等计划模型不可持有运行时资源。
- Worker state 使用单调 `stateRevision`、fsync 和原子替换。
- 日志不得输出密码、Token、完整 Connector options。
- 关键行为必须有单元测试或边界测试。

## 变更原则

一个 Phase 只解决一个主问题。未来需求不作为当前抽象的充分理由。
