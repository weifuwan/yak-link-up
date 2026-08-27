# Review

提交 PR 前按这份清单过一遍。

## 架构驱动的问题处理

遇到问题时，不从“哪里能改”开始，而是先把现象放回当前架构。

> **先问“这个问题属于谁”，再问“这个问题怎么解决”。**

### 先定位责任边界

```text
Connector       外部系统协议、SQL、数据类型、分页、Split、写入语义
Executor        并行执行、Channel、批处理、背压、线程和资源使用
Planner         Schema、Mapping、Split 规划、Capability、JobGraph
State           Job / Attempt 生命周期、Cancel、Retry、Worker State Persistence
Server          REST、Worker 控制面、注册与协议适配
Business Config batchSize、并行度、超时、表配置、字段映射等用户配置
```

问题定位以 `ARCHITECTURE.md`、`DOMAIN.md`、`DEPENDENCIES.md`、`REQUIREMENTS.md` 和本文件定义的当前边界为准。

### 修改代码前回答七个问题

1. **这个问题属于哪一层？**
2. **是层内部问题，还是层与层之间的契约问题？**
3. **当前行为违反了哪个已有架构约束？** 如果没有，是否真的缺少一个当前离线同步需要的能力？
4. **这是局部问题还是公共问题？** 只在一个 Connector / 场景出现，还是多个真实场景反复出现？
5. **最小修改点在哪里？** 哪个模块、角色、类或接口真正拥有这个问题？
6. **这次明确不应该改什么？** 把非目标写出来，防止局部问题扩大成 Runtime 重构。
7. **怎么证明改对了？** 明确单测、集成测试、故障注入、Metrics、日志或真实数据验证中的最小证据链。

输出顺序固定为：

```text
现象
  -> 责任层
  -> 层内问题 / 层间契约问题
  -> 证据
  -> 最小修改边界
  -> 明确非目标
  -> 验证
  -> 多个真实场景重复出现后再抽象
```

处理原则：

- [ ] 优先在问题所属的最小边界内解决，不因为一个具体问题扩大公共抽象。
- [ ] Connector 或业务配置能解决的问题，不先修改 Runtime。
- [ ] 只在一个 Connector 中出现的问题，不把复杂度提升到 Framework。
- [ ] 没有证据前，不先设计新的 Scheduler、State、Event 或 Capability 抽象。
- [ ] 只有多个真实场景反复出现同一种问题时，才考虑抽象成公共能力。

### AI 协作模板

向 AI 提问题时，优先让它在现有架构约束中定位，而不是只说“帮我分析一下”：

```text
基于 ARCHITECTURE.md、DOMAIN.md、DEPENDENCIES.md、REQUIREMENTS.md 和 REVIEW.md，
先不要改代码，回答：

1. 问题属于哪一层？
2. 是层内问题还是层间契约问题？
3. 当前行为违反了哪个已有约束？
4. 哪些证据能验证这个判断？
5. 最小修改边界是什么？这次明确不应该改什么？
6. 什么情况下才值得提升为 Framework 公共能力？
7. 最小验证方案是什么？

确认责任边界和证据后，再给出实现方案。
```

**实战驱动优化，问题就地解决，重复出现再抽象。**

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
