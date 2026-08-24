# Requirements

## 产品定位

Link-Up 是离线批量数据同步引擎，不是通用分布式计算平台。

当前优先级：**可靠、清楚、可扩展**，高于“功能看起来很多”。

## 必须支持

- 结构化 JobSpec / HOCON 提交。
- 单表、多表 bounded batch sync。
- Source Split + SourceReader。
- SinkWriter / SinkPreparer。
- Source/Sink/Pipeline 并行度。
- 任务取消、日志、Metrics、Pipeline/Task 查询。
- Worker 幂等提交和 externalExecutionId 查询。
- Worker checkpoint 持久化和重启 LOST 恢复。
- Job / Attempt 历史。
- 基于 commit evidence 的安全手动 Retry。
- Connector Schema / preflight 能力。

## Retry 要求

Retry 必须满足：

- 只通过显式接口触发；
- 复用同一个 `jobId`，新增 Attempt；
- 重试请求内容必须与原 Job digest 完全一致；
- 已提交数据、unknown state、LOST、CANCELED 默认拒绝；
- Worker 不持久化密码、Token、完整 JobSpec 来实现 Retry。

## 非目标

当前不做：

- 分布式 Scheduler / ResourceManager；
- Flink 式 checkpoint/savepoint；
- 从某个 Split offset 自动 resume；
- LOST 自动重放；
- 跨 Worker failover；
- 全局 exactly-once；
- 自动推断“有副作用但可以重跑”；
- 为了架构好看拆大量 Maven 模块。

## 非功能要求

- Java 8+。
- Maven 3.8.1+。
- Connector 不依赖 framework。
- JobGraph 等计划模型不可持有运行时资源。
- checkpoint 写入使用单调版本和原子替换。
- 日志不得输出密码、Token、完整 Connector options。
- 关键状态行为必须有单元测试或边界测试。

## 变更原则

一个 Phase 只解决一个主问题。兼容性优先于漂亮的重命名；有真实收益时再迁移历史代码。
