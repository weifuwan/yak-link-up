# Review

提交 PR 前按这份清单过一遍。

## 架构

- [ ] `api` 没有依赖 framework/server/具体 connector。
- [ ] framework 没有依赖具体 connector。
- [ ] connector 没有 import `com.link.up.framework.*`。
- [ ] domain 没有 Thread/Future/Semaphore/framework `JobExecution`。
- [ ] Planner 没有创建线程、Reader、Channel、Split Queue。
- [ ] 没有新增与现有 Job 状态重复的平行模型。
- [ ] 新抽象直接服务离线同步当前需求，而不是为实时/分布式预留。

## Job / Attempt / Retry

- [ ] 普通终态不会被普通状态转换复活。
- [ ] Retry 创建新 Attempt，不覆盖旧 Attempt。
- [ ] LOST/CANCELED 默认不可 Retry。
- [ ] committed data / unknown state / 缺少 evidence 时不可猜测安全。
- [ ] `stateRevision` 单调递增。

## Worker State

- [ ] Worker state persistence 和数据 checkpoint/resume 明确区分。
- [ ] Worker 重启后的非终态 Job 转 `LOST`，没有自动重放。
- [ ] 没有新增 Split offset resume、savepoint、barrier 或跨 Worker failover。
- [ ] 持久化格式升级能读取上一版本。

## Runtime Event

- [ ] Event 仍然只描述 `JOB_*` / Attempt 生命周期。
- [ ] 没有新增 `TASK_*`、`SPLIT_*`、`BATCH_*` 事件。
- [ ] Event JSON 没有复制 Pipeline/Task/Metrics 执行快照。
- [ ] Event 不参与状态恢复、Retry 或 Commit 决策。
- [ ] Event schema 变更有旧 JSONL 兼容测试。

## Capability

- [ ] Capability 来自有限离线枚举集合。
- [ ] required 缺失才阻断，preferred 缺失只 Warning。
- [ ] 拓扑派生能力直接并入 required，不建立 observed/undeclared-observed 模型。
- [ ] 没有新增 Capability DSL、依赖图、fallback/cost/priority 系统。

## Connector

- [ ] Source/Sink 外部资源由 Reader/Writer 自己拥有。
- [ ] Split ID / dataSetId 合法稳定。
- [ ] 没有新增 `common/utils/helper/misc`。

## 安全

- [ ] 日志没有密码、Token、完整 Connector options。
- [ ] Worker state 没有持久化 JobSpec/密码/Token。
- [ ] REST 错误没有直接暴露内部对象。

## 兼容

- [ ] Breaking REST/API 变更明确写进 PR 描述。
- [ ] 已有 Connector identifier 不变。
- [ ] JobSpec 字段语义没有偷偷改变。
- [ ] 老 Worker state / Event Journal 有必要的兼容读取测试。

## 验证

```bash
mvn --batch-mode clean verify
```

至少检查：

- [ ] 新行为有测试。
- [ ] 架构守卫仍通过。
- [ ] 没有残留误导为实时/断点恢复的平台化预留。
- [ ] 文档只描述当前实现。
