# Review

提交 PR 前按这份清单过一遍。

## 架构

- [ ] `api` 没有依赖 framework/server/具体 connector。
- [ ] framework 没有依赖具体 connector。
- [ ] connector 没有 import `com.link.up.framework.*`。
- [ ] domain 没有 Thread/Future/Semaphore/framework `JobExecution`。
- [ ] HTTP 没有直接依赖 infrastructure。
- [ ] Planner 没有创建线程、Reader、Channel、Split Queue。
- [ ] 新类的角色能用一句话说清。

## Job / Attempt / Retry

- [ ] 普通终态不会被普通状态转换复活。
- [ ] Retry 创建新 Attempt，不覆盖旧 Attempt。
- [ ] Retry 请求校验原 Job digest/版本/幂等标识。
- [ ] LOST/CANCELED 默认不可 Retry。
- [ ] 有 committed data 或 unknown state 时不可 Retry。
- [ ] 缺少 commit evidence 时不可猜测安全。
- [ ] checkpointVersion 单调递增，旧写入不能覆盖新状态。

## Connector

- [ ] 新 Source 使用 `createEnumerator(...)`。
- [ ] Split ID / dataSetId 合法且稳定。
- [ ] Reader/Writer 自己拥有外部资源。
- [ ] package 使用明确角色名。
- [ ] 没有新增 `common/utils/helper/misc`。

## 安全

- [ ] 日志没有密码、Token、完整 Connector options。
- [ ] checkpoint 没有持久化 JobSpec/密码/Token。
- [ ] REST 错误没有直接暴露内部对象。
- [ ] 文件路径由 Worker 自己生成，不接受任意路径读取。

## 兼容

- [ ] REST 改动是 additive，或明确说明 breaking change。
- [ ] 已有 connector identifier 不变。
- [ ] JobSpec 字段语义不被偷偷改变。
- [ ] 持久化格式升级能读取上一版本。

## 验证

```bash
mvn --batch-mode clean verify
```

至少检查：

- [ ] 新行为有测试。
- [ ] 架构守卫仍通过。
- [ ] 没有残留旧包/旧类 import。
- [ ] 文档只描述当前实现，不写过期计划。
- [ ] PR 描述清楚兼容性、非目标和未执行的验证。

如果一项需要长篇解释才能勾上，通常说明代码边界还不够清楚。
