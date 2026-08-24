# Link-Up Code Style & Engineering Guide
> Link-Up 的代码风格不是格式化工具说明，而是一套工程约束。它吸收 Flink 的运行时角色与显式状态、Hadoop 的 API/兼容纪律、Spark 的简洁与克制，并以 Link-Up 自己的架构为最终标准。

## 1. Link-Up Style
核心只有四个词：**清晰、笔直、显式、克制**。
- **清晰**：类名、方法名、包名直接表达职责。
- **笔直**：主执行路径从上到下阅读，少嵌套、少跳转。
- **显式**：状态、生命周期、失败、所有权、边界不靠猜。
- **克制**：不提前抽象，不制造无必要层级，不隐藏副作用。
Review 一段代码时先问：谁拥有状态？谁决定何时执行？谁真正执行？谁跨越边界？失败后状态是什么？主方法能否直接看懂完整流程？
如果一个类同时回答多个问题，通常需要继续拆分。
规则等级：**必须**=新代码和重构代码都遵守；**应该**=默认遵守，偏离时说明理由；**建议**=为了可读性，不做机械限制。

## 2. Formatting & Class Layout
必须：4 spaces、禁止 Tab、K&R braces、一行一个 statement、文件以 newline 结束、禁止 wildcard import、删除未使用 import。120 characters 作为软行宽。
不要制造 vertical noise。
Bad:
```java
this.registry =
        Objects.requireNonNull(
                registry,
                "registry must not be null");
```
Good:
```java
this.registry = Objects.requireNonNull(registry, "registry must not be null");
```
调用需要换行时只缩进一层：
```java
JobResult result = executor.execute(
        definition,
        executionListener);
```
复杂匿名对象先命名再调用。
同一类按固定顺序组织：
```text
static constants -> logger -> immutable dependencies -> mutable runtime state
-> constructors -> public API -> lifecycle methods -> package-private methods
-> private workflow methods -> validation/helper methods -> nested classes
```
字段优先 `final`。构造器完成后对象应立即可用，不依赖隐藏的 `init/setup/prepareBeforeUse`。简单 getter 不要打断核心 workflow。

## 3. Naming & Roles
优先按**角色**命名，而不是按模糊动作命名。

| Role | 负责 | 不负责 |
| --- | --- | --- |
| `Factory` | 构造扩展对象 | 调度、执行 |
| `Registry` | 发现、索引、解析实现 | Runtime 生命周期 |
| `Compiler` | Model → Model | IO |
| `Planner` | 生成不可变计划 | Thread、Reader、Channel |
| `Graph` | 描述某生命周期阶段 | 隐藏副作用 |
| `Coordinator` | 协调参与者和结果 | 线程池细节 |
| `Scheduler` | 并发、何时允许执行 | Connector IO |
| `Executor` | 执行已选定工作 | 制定调度策略 |
| `Enumerator` | 发现 Source Split | 读取数据 |
| `Reader` / `Writer` | 外部数据读写 | Job 生命周期 |
| `Repository` | 保存/读取状态 | 调度 |
| `Gateway` | 跨系统/进程边界 | 领域决策 |

`Manager` 只留给真正的顶层生命周期拥有者。
Bad: `DataUtils / CommonHelper / JobManager2 / TaskProcessor / FactoryUtil`
Good: `JobPlanner / SourceCoordinator / JobRuntimeScheduler / JobRetryPolicy / JobLogFileName`
Boolean 使用判断语义：`isTerminal()`、`hasCommittedData()`、`canTransition(...)`、`isRetryEligible()`。

## 4. Package & Module
按职责分包，不按“工具类型”分包。
推荐：
```text
application / domain / infrastructure
planner / execution / source / connector
source / sink / catalog / client / config / converter / internal
```
禁止新增：`common / utils / helper / misc / core`。
历史兼容包可暂存，但不得扩张；触碰时逐步迁出。
模块方向：
```text
launcher/server -> framework + connectors
framework       -> api
connectors      -> api
```
禁止：`api -> framework`、`framework -> concrete connector`、`connector -> framework`、`domain -> infrastructure`、`HTTP -> concrete infrastructure adapter`。
`link-up-api` 只放稳定扩展契约；`framework` 只实现引擎；Connector 只实现 API；`server/launcher` 负责 composition root。

## 5. Control Flow & Method Shape
主路径必须靠左，优先 early return。
Bad:
```java
if (job != null) {
    if (!job.isTerminal()) {
        if (policy.canRetry(job)) {
            retry(job);
        }
    }
}
```
Good:
```java
if (job == null || job.isTerminal()) {
    return;
}
if (!policy.canRetry(job)) {
    return;
}
retry(job);
```
建议最大嵌套层级不超过 3；超过时优先 early return、private method、显式状态对象、policy/strategy、职责拆分。
核心方法应像流程图：
```java
public JobResult execute(JobDefinition definition) throws Exception {
    validate(definition);
    PreparedJob prepared = connectorPreparer.prepare(definition);
    JobGraph graph = jobPlanner.plan(prepared);
    ExecutionGraph execution = new ExecutionGraph(graph);
    return coordinator.execute(execution);
}
```
Review 软阈值：
```text
method > 50 lines       -> 是否包含多个步骤？
class > 500 lines       -> 是否包含多个角色？
parameters > 5          -> 是否缺少领域对象？
nesting > 3             -> 是否可以 early return？
boolean parameters > 1  -> 是否应该 enum/options？
```
Bad: `execute(job, true, false, true)`；Good: `execute(job, ExecutionMode.RETRY)`。

## 6. Abstraction & Immutability
不要提前抽象。不要因为“未来可能有第二种实现”立即创建 `AbstractXxx/DefaultXxx/XxxFactory/XxxProvider`。
原则：**先有职责，再有抽象；先有第二个实现，再考虑多态。**
但架构边界即使只有一个实现，也应该使用 Port/Interface：
```java
interface JobRepository
interface JobExecutor
interface JobRuntimeScheduler
```
这里接口的意义是保护依赖方向。
计划和定义模型优先不可变：`JobDefinition / PreparedJob / JobGraph / PipelineGraph / SourceTaskPlan`。
必须：字段优先 `final`、constructor 建出合法对象、collection 对外 immutable、不提供无约束 setter、plan 不持有 runtime mutable object。
Bad:
```java
class JobGraph {
    ExecutorService executor;
    SplitProvider splitProvider;
}
```
Good:
```java
class JobGraph {
    private final List<PipelineGraph> pipelines;
}
```
可变状态必须有明确 owner，例如 `ExecutionGraph`、`JobExecutionState`、`JobExecutionAttempt`、`LocalJobRuntimeScheduler`。

## 7. API, Compatibility & Validation
Public API、跨模块入口、扩展点参数必须尽早校验：
```java
this.registry = Objects.requireNonNull(registry, "registry must not be null");
```
字符串 ID 统一校验 blank，不让非法值流入深层 Runtime。
构造完成对象应立即可用；如果生命周期必须显式启动，类型和 API 要表达出来，例如 `start/close`。
稳定 API 修改优先：`additive field`、`default method`、`compatibility adapter`、`deprecated -> migrate -> remove`、`versioned persisted format`。
修改这些内容前必须明确兼容策略：public API、Worker protocol、Connector contract、checkpoint format、Job lifecycle。
内部废路径如果已经无人使用，直接删除，不保留假的 compatibility layer。
例如 canonical 路径已经是：
```text
FactoryRegistry -> ConnectorPreparer
```
就不应重新创建第二套 `FactoryUtil`。

## 8. Runtime Ownership & Concurrency
这是 Link-Up 最重要的专属规则。
出现 `Thread / Future / Semaphore / ExecutorService / CancellationToken / SplitProvider / Channel / JobExecution / Metrics` 时必须明确 owner。
Review 必须回答：谁创建？谁关闭？谁取消？属于 Job/Pipeline/Task 哪个生命周期？是否跨层泄漏？
典型所有权：
```text
JobGraph          -> immutable physical plan
ExecutionGraph    -> one runtime attempt state
JobCoordinator    -> job lifecycle/result
PipelineScheduler -> pipeline concurrency
PipelineExecutor  -> one selected pipeline
PipelineExecution -> channels/split queues/tasks
TaskExecutor      -> task execution
```
禁止 Planner 创建 runtime resource。
禁止 Domain 持有 `Thread/Future/Semaphore/ExecutorService/framework JobExecution`。
并发代码必须显式、局部、可关闭：
- `ExecutorService` 有关闭路径。
- `Thread` 有 owner。
- interrupt 不被吞。
- cancellation 可传播。
- terminal state 防止晚到事件覆盖。
- 不用 `sleep` 充当同步机制。
正确处理中断：
```java
try {
    thread.join(timeout);
} catch (InterruptedException interrupted) {
    Thread.currentThread().interrupt();
    break;
}
```
`volatile/synchronized/Atomic*` 出现时，应能解释具体并发语义。

## 9. Error Handling & Logging
异常边界：`validation -> application/domain error -> runtime failure -> REST error mapping`。
必须：不吞异常、不用异常做正常流程、保留 cause、不重复 wrap 五层相同信息、cleanup best-effort 写明原因、REST 不直接暴露 Throwable。
Good:
```java
catch (IOException failure) {
    throw new IllegalStateException(
            "Could not persist Worker checkpoint for " + jobId,
            failure);
}
```
错误消息应说明 `what failed / which identity / which boundary`，不要带密码、Token、完整 Connector options。
日志优先记录 `jobId / attemptId / runId / pipelineId / taskId / status / duration / count`。
禁止记录 `password / token / 完整 secret URL / 完整 Connector options / 整份 JobSpec`。
使用参数化日志；多线程执行边界维护 MDC/runId；异常日志保留 stack trace。
```java
LOG.info(
        "Job started: jobId={}, attemptId={}, runId={}",
        jobId,
        attemptId,
        runId);
```

## 10. Connector, Collections & Comments
Runtime 热路径、状态变更优先普通 `for`；Stream 适合简单 `map/filter/collect` 和非热路径声明式转换。
不要用复杂 Stream 隐藏状态变化、异常处理、资源生命周期和副作用。Reflection 只用于扩展发现等明确框架场景。
Connector 推荐：
```text
jdbc/
├── source
├── sink
├── catalog
├── client
├── config
├── converter
├── dialect
├── split
└── internal
```
Source：`SourceFactory -> Source -> SourceSplitEnumerator -> SourceReader`
Sink：`SinkFactory -> SinkPreparer -> SinkWriter`
Connector 禁止：import framework internals、创建 Job/Pipeline runtime、决定 Worker retry、持有全局 scheduler、把驱动 SDK 类型泄漏到 API。
注释解释 **Why**，不要重复 **What**。
Bad:
```java
// Get job.
Job job = getJob(jobId);
```
Good:
```java
// LOST cannot prove that the sink committed nothing.
// Keep retry conservative until commit evidence is available.
if (status == ServerJobStatus.LOST) {
    return RetryDecision.denied(LOST_OUTCOME_UNKNOWN);
}
```
必须写有价值 Javadoc：public extension API、跨模块 contract、lifecycle、状态机、并发所有权、安全规则、compatibility behavior。
简单 getter、明显 constructor、简单 private helper 不写模板化废话。

## 11. Testing
测试名称描述行为：
```text
shouldRejectRetryWhenPreviousAttemptCommittedData
shouldRecoverRunningCheckpointAsLost
shouldPreserveSplitEnumerationOrder
```
优先测试：状态机、Job/Attempt 生命周期、幂等、Retry safety、checkpoint reopen/recovery、scheduler concurrency、cancellation、architecture boundary、Connector split/read/write/convert、compatibility adapter。
不要为了覆盖率给 getter 写测试。
重要架构边界要用测试锁住：
```text
Planner 不持有 ExecutorService
Domain 不持有 Thread/Future
Connector 不依赖 framework
HTTP 不依赖 infrastructure implementation
```
能自动测试的规则，不只写在文档里。一个测试只验证一个主要行为；失败原因应能从测试名和断言看懂。

## 12. Refactoring & Review
架构 PR 必须小步。一个 PR 尽量只做一种变化：移动职责、修改协议、修改执行语义、性能优化，四者不要混在一起。
推荐顺序：
```text
1. 建立新边界
2. 迁移调用方
3. 加 architecture guard
4. 删除旧路径
5. 再优化算法
```
修改旧类时，只整理当前改动附近；除非 PR 目标就是 style cleanup，否则不要全文件格式化。
Review Checklist：
- **Responsibility**：类是否只有一个主要角色？Planner/Scheduler/Executor/Coordinator 是否越界？是否新增第二套执行路径？是否塞进 `Utils/Helper/Manager`？
- **Readability**：主路径是否从上到下可读？是否超过 3 层嵌套？超长方法是否包含多个阶段？boolean 参数是否难懂？
- **State**：mutable state 的 owner 是谁？状态转换是否集中？terminal state 是否会被晚到事件覆盖？plan 与 runtime state 是否分开？
- **Runtime**：Thread/Future/Executor/Semaphore 谁创建、谁关闭？cancellation 是否传播？classloader scope 是否正确？
- **Safety**：retry 是否基于 commit evidence？LOST 是否被错误当成 FAILED？日志/异常是否泄漏 secret？persisted format 是否兼容旧版本？
- **Tests**：测试是否覆盖真正风险？是否需要 architecture guard？是否保留 compatibility test？

## 13. Canonical Example
目标不是把代码写短，而是让代码读起来就是系统本身。
Bad:
```java
public void process(Job job, boolean retry, boolean force) {
    if (job != null) {
        if (retry && !job.isRunning()) {
            if (force || check(job)) {
                doIt(job);
            }
        }
    }
}
```
Good:
```java
public JobSnapshot retry(
        String jobId,
        JobSubmission submission) {

    JobExecutionState state = requireRetryableJob(jobId);
    validateRetrySubmission(state, submission);

    JobRetryDecision decision = retryPolicy.evaluate(state);
    decision.requireEligible();

    state.startRetryAttempt();
    checkpoint(state);

    schedule(state);
    return snapshot(state);
}
```
它直接表达：`找到状态 -> 校验请求 -> 判断安全 -> 创建 Attempt -> 持久化 -> 调度 -> 返回快照`。
这就是 Link-Up Style。

## 14. 最终标准
两种写法都正确时，选择：
- 更容易读懂的；
- 更容易测试的；
- 更容易定位 state owner 的；
- 更少隐藏副作用的；
- 更少提前抽象的；
- 更接近 Link-Up 当前架构角色的。

> **让代码像执行流程一样清楚，让状态像领域模型一样显式，让抽象只出现在真正的边界上。**
