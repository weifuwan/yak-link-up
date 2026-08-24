# Code Style

## 命名先表达角色

优先使用这些后缀：

- `Factory`：构造扩展对象。
- `Registry`：发现和索引实现。
- `Compiler`：协议/模型转换。
- `Planner`：生成不可变计划。
- `Graph`：某一生命周期阶段的图。
- `Coordinator`：协调多个参与者。
- `Scheduler`：决定何时允许执行。
- `Executor`：执行已经选定的工作。
- `Enumerator`：发现 Source Split。
- `Reader` / `Writer`：读写外部系统。
- `Repository`：持久化，不负责调度。
- `Gateway`：跨进程/系统边界。

`Manager` 只留给真正的顶层生命周期拥有者。

## Package

按职责分包，不按“工具类型”分包。

好的例子：

```text
source / sink / catalog / client / converter
application / domain / infrastructure
planner / execution / coordinator
```

避免：

```text
common / utils / helper / misc
```

## Java

- 优先不可变对象和 `final` 字段。
- public API 参数尽早校验。
- 状态转换集中管理，不在多个类里随手 `setStatus`。
- 不吞异常；确实 best-effort 的关闭/清理路径要写明原因。
- 不用异常做正常流程控制。
- 线程、Future、Semaphore 只能出现在明确的 runtime/infrastructure 边界。
- 不把 `Throwable`、线程对象、框架运行对象直接暴露到 REST DTO。

## 日志

- 日志写 ID、状态、耗时，不写密码、Token、完整 options。
- 多线程执行边界要维护 MDC / runId。
- 异常日志保留堆栈；返回给 REST 的错误消息要做长度和换行控制。

## 测试

测试名称写行为：

```text
shouldRejectRetryWhenPreviousAttemptCommittedData
shouldRecoverRunningCheckpointAsLost
```

重点测试：

- 状态机；
- 幂等；
- Retry 安全判定；
- 持久化 reopen/recovery；
- 架构边界；
- Connector split/转换/读写生命周期。

不要为了覆盖率给 getter 写无意义测试。

## 改动尺度

架构 PR 要小步：先移动职责，再优化算法。不要在同一 PR 里同时“换架构 + 改协议 + 改执行语义”。
