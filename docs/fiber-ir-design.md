# Fiber IR 定义

**Status:** design record, not implemented. 建立在 `docs/fiber-runtime-design.md`（Fiber / Handle / Continuation 收敛稿）之上；与 `docs/asl-ir-design.md`（Scope/Group + 21 指令旧稿）的差异见 §11。

范围：程序与帧的表示、指令集及其语义、挂起与提交协议、`SPAWN` 语义、ASL → IR 的 lowering。不包含：调度器实现、存储布局、对外查询模型、编译器前端。

本文件中的每一条都是可从 `docs/examples/spica-asl-full-example.json` 的用例上验证的——§10 给出该文件的逐状态展开。

---

## 1. 不变量

1. **确定性**：给定（已提交帧, 事件）目标指令唯一。程序里没有 `$now`/`$random` 之外的不可控输入；它们只在**未提交区间**内被求值，所以不影响重跑的可观测结果（§8.4）。
2. **状态即全部**：Fiber 的全部执行状态 = 一个 `Frame` 值 + 它的 `Inbox` + 它的 `Handle` 集合。没有调用栈、没有运行时回调闭包、没有内存中的注册表。
3. **算子集封闭**：新增 ASL 特性必须 lowering 成已有指令的组合（宏），不得给运行时加 `switch` 分支。
4. **提交原子性**：一次提交 = 一个批次，批次内包含帧快照、出站请求、已消费事件、handle 状态变更、子 Fiber 创建意图。**对外部世界的请求只能通过批次出去**。
5. **单写者**：任一时刻一个 Fiber 至多一个运行时实例在写；写入带 `epoch` 围栏。

---

## 2. 程序表示

```
Program {
  program_id, version,
  consts: [Value],            // 编译期常量/模板，只读
  blocks: [Instruction],      // 扁平指令数组
  block_table: {label -> pc}, // 编译期符号，运行时不可见
  entry: pc,                  // StartAt 状态入口
}
```

- 一个 `Block` 是**基本块标签**，编译产物里解析为绝对 `pc`。Continuation 持久化的是 `(program_id, version, label)`，运行时按 Fiber 钉住的版本解析；版本不重编译即不改语义。
- Fiber 创建时钉住 `version`；旧 Fiber 永远在旧程序上跑完。

### 寄存器与变量

| | 形态 | 名字解析 | 用途 |
|---|---|---|---|
| **Registers** | 带类型标签的值（`null/bool/int/float/str/obj/arr/err/handle/event`） | 编译期确定 | `%in`（状态输入）、`%res`、`%err`、`%ctx`、各指令的临时值 |
| **Variables** | 一个 JSON 对象（ASL 的 `Assign` 变量表） | 运行期动态路径 | 后续状态可读的变量 |

动态索引只发生在**值的内部**（`CALC %acc, merge(%acc, [$i, %v])`），永不发生在寄存器名字上——寄存器名保持编译期可知。

`$states.input/result/errorOutput/context` 分别绑定到 `%in/%res/%err/%ctx`，`EVAL_*` 的表达式在这个固定绑定上求值。

---

## 3. Fiber 与 Frame

```
Frame {
  fiber_id, parent_fiber_id, program_id, version,
  state,            // 当前 ASL 状态名
  pc,               // 下一条待执行指令
  exit,             // 本状态的 epilogue 块（ENTER_STATE 写入）
  regs,             // 带标签寄存器表
  vars,             // ASL 变量对象
  visit,            // 进入该状态的次数（循环下稳定，幂等键的一半）
  attempt,          // 当前状态内的尝试计数
  status,           // RUNNABLE | RUNNING | PARKED | EXITING | TERMINAL
  epoch,            // 围栏
  children,         // 子 Fiber 的 child_key 集合（退出时用）
}
```

**没有 wait-set 字段**：一个 Fiber 在等什么，由"它拥有的 handle 的未决状态"决定，不需要另存一份（§8.2）。

Fiber 状态机：

```
RUNNABLE --dispatch--> RUNNING --PARK--> PARKED
                               --COMMIT_STATE(末状态)--> EXITING? 
PARKED --事件到达且拿到 epoch--> RUNNABLE
RUNNING/ PARKED --取消或正常结束--> EXITING --子 Fiber 全终态 + finalizer 跑完--> TERMINAL
```

`EXITING` 不是礼节性状态：**父不能带着活子 Fiber 退出**。进入 `EXITING` 后：`CANCEL_CHILDREN` → 等在途子 Fiber 全部 `TERMINAL`（或策略超时）→ `RELEASE` 所有 owned handle 并执行 finalizer → `TERMINAL`。这是旧稿 `SCOPE_CLOSE` 的 `NeedAck`，只是 ack 集合简化为"所有活子 + 所有待跑 finalizer"。

---

## 4. Handle

```
Handle {
  handle_id,          // 稳定逻辑 ID，非指针
  owner_fiber_id,
  kind,               // timer | task | child
  state,              // PENDING | ACTIVE | DONE_OK | DONE_ERR | CANCELLED | RELEASED
  routes,             // 事件 → 基本块（Continuation）
  op_id,              // 外部操作 ID，幂等键
  payload,            // 恢复所需：deadline / 参数摘要 / child_key
  finalizer,          // on_release 块（可空）
  finalizer_done,
}
```

**Continuation 就是 `Handle.routes`**：一张 `事件种类 → 基本块` 的路由表，在 `START_*` 时登记、随批次持久化。它不是独立对象，也不是 CPS 续延——没有"返回地址"这个概念，处理器块结束于 `RETURN`，回到 `Frame.exit`（§8.3）。

生命周期约束：

- `RELEASE(live)` = `CANCEL` → 等终态 → 跑 `finalizer` → `RELEASED`。对 `ACTIVE` 的 handle 直接 `RELEASE` 而跳过取消会留下无人拥有的在飞操作。
- `CANCELLED` 的 handle **不再产生事件**，其 `routes` 随之失效——这是 Race 语义成立的前提（§10.3）。
- 关闭的 kind 表：

| kind | 事件 | 默认 route | 清理 |
|---|---|---|---|
| `timer` | `fired` | `on_fire` | 无 |
| `task` | `ok` / `err` | `on_success` / `on_failure` | `CANCEL_TASK` |
| `child` | `exited(ok/err)` | `on_success` / `on_failure` | `SIGNAL_CANCEL` |

---

## 5. 事件与 Inbox

```
Event {
  seq,            // 每个 Fiber 单调递增，append 时分配
  event_id,       // 去重键，由生产者推导
  handle_id,      // 来源
  kind,           // fired | ok | err | exited
  payload,        // 结果或错误
}
```

- **顺序**：按 `seq` 处理。同一时刻多个事件就绪时，"谁先"由**日志顺序**决定——回放看到的就是同一个顺序，因此不把非确定性漏进语义层。
- **去重**：`event_id = f(handle_id, kind, generation)`；inbox 以 `event_id` 唯一。
- **投递原子性**：事件的生产者与 append 必须在同一批次内（子 Fiber 终态 + 向父 inbox 投递；handle 状态变更 + 事件 append）。
- **孤儿事件**：owner 已 `TERMINAL` 时到达的事件进入死信，不阻塞、不重试。

---

## 6. 指令集

19 条指令。`控制` 列：`seq` 顺序、`susp` 挂起、`term` 终止。

### 6.1 控制流

| 指令 | 操作数 | 语义 | 控制 | 提交 |
|---|---|---|---|---|
| `ENTER_STATE` | `"State"` | 状态边界：`state←`，`visit++`，`attempt←0`，清 `%res/%err`，`exit←epilogue` | seq | **是** |
| `COMMIT_STATE` | `→entry` | 状态出口：变量表生效、输出定稿，跳下一状态入口；无后继则进入 `EXITING` | seq | **是** |
| `JUMP` | `label` | 无条件跳转 | seq | |
| `BRANCH` | `%c, T, F` | 条件跳转 | seq | |
| `RETURN` | — | 跳到 `frame.exit` | seq | |
| `FAIL` | `%err` | Fiber 失败，进入 `EXITING` | term | **是** |
| `SUCCEED` | `%out` | Fiber 成功，进入 `EXITING` | term | **是** |

### 6.2 数据

| 指令 | 操作数 | 语义 | 控制 |
|---|---|---|---|
| `EVAL_JSONATA` | `%d, expr, !B?` | 在固定绑定上求值用户 JSONata；失败走词法边 `!B`，无 `!B` 则 `FAIL` | seq |
| `EVAL_TEMPLATE` | `%d, tmpl, !B?` | 对象模板（内嵌 `{% %}`）展开 | seq |
| `CALC` | `%d, expr` | 编译器生成的纯运算：取字段、索引、算术、`merge`、error 构造。**不可失败、不可挂起、不可触外** | seq |
| `ASSIGN` | `$var, %src` | 写 ASL 变量 | seq |

`CALC` 吸收全部"形状"逻辑：下标累加、`Fork` 的 join 计数、retry 退避算术、容差比较。它编译器生成，用户看不到。

### 6.3 异步操作

| 指令 | 操作数 | 语义 | 控制 |
|---|---|---|---|
| `START_TIMER` | `%h, %deadline, on_fire=@B, on_release=@B?` | 建 timer handle；**出站请求随下一次提交发出** | seq |
| `START_TASK` | `%h, %args, on_success=@B, on_failure=@B, on_release=@B?` | 建 task handle，同上 | seq |
| `SPAWN` | `%h, @entry, %input, key=%key` | 建子 Fiber + child handle（§9） | seq |

### 6.4 事件处理

| 指令 | 操作数 | 语义 | 控制 |
|---|---|---|---|
| `PARK` | — | 挂起。恢复目标是 `Handle.routes` 解析出的事件处理块；**PARK 之后的指令不可达** | susp |
| `CURRENT_EVENT` | `%ev` | 取出正在处理的当前事件（含 payload） | seq |

### 6.5 生命周期

| 指令 | 操作数 | 语义 |
|---|---|---|
| `CANCEL` | `%h` | 请求取消；handle 转入 `CANCELLED` 后不再产事件 |
| `RELEASE` | `%h` | 释放：必要时先取消，再执行 finalizer |
| `CANCEL_CHILDREN` | — | 向所有活子 Fiber 发取消；父进入 `EXITING` 时自动执行 |

### 6.6 宏（编译器展开，非指令）

| 宏 | 展开 |
|---|---|
| `RETRY_LOOP` | `CALC` 退避 + `START_TIMER` + `PARK` + `JUMP @attempt` |
| `CATCH` | 在 `on_failure` 块里 `CALC` 匹配错误名 + `BRANCH` 到 catcher 块 |
| `TIMEOUT` / `HEARTBEAT` | `START_TIMER`（同一次 `PARK` 之外并行） |
| `RACE` | 每个 `on_*` 块开头 `CANCEL` 其余 handle |
| `FANOUT` | 计数器 + `SPAWN` 循环 + `PARK`（§10.4） |

---

## 7. 状态边界与错误边

- 每个状态编译成 **entry 块 + 一个 epilogue 块 + 若干 handler 块**。`ENTER_STATE` 写下 `exit`，所有 handler 以 `RETURN` 回到同一个 epilogue，因此 `Assign`/`Output` 只写一次。
- **错误边是词法的、逐点的**：每条可能失败的 `EVAL_*` 自带 `!B`。这恰好表达了 ASL 的一条硬规则——**Catcher 自己的 `Output`/`Assign` 求值失败不可捕获**：epilogue 里的求值不挂 `!B`，直接 `FAIL`。
- `Retry` 与 `Catch` 不是运行时概念：`Retry` 是"`attempt` 计数 + 回退跳转 + 一次 `START_TIMER/PARK`"，`Catch` 是"在 `on_failure` 块里分支"。运行时对二者一无所知。

---

## 8. 挂起、分发与提交

### 8.1 三种挂起之外的指令都不挂起

只有 `PARK` 会挂起。这样"一个 Fiber 的挂起状态"只有一种表示：`status=PARKED` + 当前 `pc`。

### 8.2 分发路径

事件到达 → 按 `handle_id` 找 `Handle` → `owner_fiber_id` → `Handle.routes[kind]` → 该块成为 `pc`，`status←RUNNABLE`。**不需要 wait-set**：handle 上已有路由，fiber 只需要被唤醒。

`PARK` 时若既无未决 handle、inbox 也为空，即为**死锁**，运行时直接 `FAIL`（这是可以被静态检查拦掉大半的）。

### 8.3 处理器块没有返回地址

`on_*` 块不是回调，是普通基本块。它们读 `CURRENT_EVENT`，做自己的事，最后 `RETURN` 回到 `frame.exit`。这就是此前悬空的"续延点"——它被固定为**状态出口**，而不是每次调用时传递。

### 8.4 提交点 = 状态边界 + 挂起

批次内容（原子）：

```
帧快照(pc, regs, vars, visit, attempt, state)
+ 消费掉的 inbox 事件（seq ≤ n）
+ 本批产生的出站请求（START_TIMER/START_TASK/SPAWN 的真正派发）
+ handle 状态变更
+ 子 Fiber 创建意图
```

由此得出两条推论：

1. **`START_*` 不立即派发**，只登记意图；派发随提交发生。崩在提交前 ⇒ 外部世界什么都没发生。
2. **不需要 Recorder**。旧稿要求记录 `$now/$random` 以便逐字节重放，是因为它允许"检查点之间已有可观测副作用"。这里每条指令区间的副作用都随检查点提交，重跑永远从已提交帧开始，**未提交区间内的非确定性不可观测**。

### 8.5 恢复

对每个非终态 Fiber：

1. 载入 `Frame`（`pc` 就在提交点上）。
2. `RUNNING` 视为 `PARKED`（写入者已死），抢 `epoch`。
3. **对账**：timer 比 `deadline` 与当前时间；task 查操作结果/重新租约；child 读子 Fiber 终态。对账产生的事件与状态变更走同一批次。
4. 从 `PARKED` 继续分发。

规则：**永不因为"没收到事件"而假设事件不会来**——事件可能已发生而未 append，一律以 handle 的持久状态 + 时钟为准。

---

## 9. `SPAWN`

```
SPAWN %h, @entry, %input, key=%key
```

1. **key 必须推导**：`child_key = hash(parent_fiber_id, spawn_site_label, ordinal)`。`spawn_site_label` 是编译期稳定符号（不是 pc）；`ordinal` 来自循环/项序号。嵌套 fan-out 必须带上完整祖先路径，否则不同层级的子会撞 key。
2. **child_fiber_id 由 key 推导**，不使用随机 UUID。恢复时按 key 查：存在则复用，不重复创建。
3. **无 Group、无批量 SPAWN**：一次 `SPAWN` 只建一个子 Fiber，对应一条 intent。Map 的并发由"游标 + 边跑边补"实现（§10.4），永不一次性 spawn N 个。
4. **游标推进与 spawn intent 同批提交**：同批写入父帧的 `launched` 计数与 intent，否则恢复时重放会产生不同 `ordinal`。
5. **子可见性与父的 `children` 集合同批**，否则父可能在子还活着时退出。
6. **子完成 → 父 inbox**：子 `TERMINAL` 与向父 inbox 的 `Event` append 同批（outbox）。
7. **输入快照**：`%input` 作为值写入子 Fiber 的初始帧，不复用父的寄存器引用。

---

## 10. ASL lowering

以 `docs/examples/spica-asl-full-example.json` 为准。

### 10.1 骨架

```
Init:     ENTER_STATE "Init"
          EVAL_TEMPLATE %out, {"orderId":"{% $states.input.orderId %}","phase":"init"}
          EVAL_JSONATA  %v_orderId, "$states.input.orderId"
          ASSIGN $orderId, %v_orderId
          CALC %v_attempts, 0 ;  ASSIGN $attempts, %v_attempts
          COMMIT_STATE -> RouteOrder
```

`Assign` 在 epilogue 里求值、随 `COMMIT_STATE` 生效（ASL 语义：变量对**后继状态**可见）。

### 10.2 Wait（相对与绝对）

```
WaitForStock:
  ENTER_STATE "WaitForStock"
  EVAL_JSONATA %secs, "$states.input.pollSeconds" !@fail
  CALC     %deadline, now() + %secs*1000
  START_TIMER %t, %deadline, on_fire=@fired
  PARK
fired:
  CURRENT_EVENT %ev
  RETURN
epilogue:
  EVAL_TEMPLATE %out, {"waitedFor":"stock"}
  ASSIGN $waitKind, "seconds-expr"
  COMMIT_STATE -> ChargeCard
```

绝对时间（`WaitUntilDeadline`）只差第一步：`CALC %deadline, ts("2026-11-01T00:00:00Z")`。**等待的语义完全由 `deadline` 决定**，不含"已等多久"——时间是可推导的，不需要断点。

### 10.3 Task（Retry + Catch + Timeout + Heartbeat）

`ChargeCard` 是这套设计里最复杂的状态，展开后能说明 Race、Retry、Catch 都只是控制流：

```
entry:
  ENTER_STATE "ChargeCard"
  EVAL_JSONATA %args, '{ "FunctionName": ..., "Payload": { "orderId": "$orderId", ... } }' !@fail
  EVAL_JSONATA %to, "$states.input.chargeTimeout" !@fail
  CALC %hb, 30000
  JMP @attempt

attempt:
  START_TASK  %task, %args,
              on_success=@ok, on_failure=@err, on_release=@on_rel
  START_TIMER %deadline_handle, now()+%to, on_fire=@timeout
  START_TIMER %hb_handle, now()+%hb, on_fire=@heartbeat_lost
  PARK

ok:                                  ; Race 胜者：取消其余
  CURRENT_EVENT %ev
  CALC %res, %ev.payload
  CANCEL %deadline_handle
  CANCEL %hb_handle
  RETURN

timeout:                             ; Race 胜者：取消 task
  CANCEL %task
  CALC %err, make_error("States.Timeout", "...")
  JMP @err

heartbeat_lost:
  CANCEL %task
  CANCEL %deadline_handle
  CALC %err, make_error("States.HeartbeatTimeout", "...")
  JMP @err

err:
  CALC %done, %attempt + 1
  CALC %retryable, match(%err, ["States.TaskFailed"]) && %done <= 4
  BRANCH %retryable, @backoff, @catch

backoff:
  CALC %delay, min(2000 * pow(2, %attempt), 30000)   ; JitterStrategy 在此并入
  CALC %dl, now() + %delay
  START_TIMER %b, %dl, on_fire=@retry
  PARK
retry:
  CALC %attempt, %attempt + 1
  JMP @attempt

catch:
  CALC %eo, error_output(%err)
  EVAL_TEMPLATE %out, {"error": "{% $states.errorOutput %}"}   ; 无 !B：不可捕获
  CALC %v_paymentError, %eo ;  ASSIGN $paymentError, %v_paymentError
  COMMIT_STATE -> FailOrder

epilogue:                            ; 正常路径
  CALC %v_charge, %res.Payload
  ASSIGN $charge, %v_charge
  EVAL_TEMPLATE %out, {"chargeId": "{% $states.result.Payload.id %}"}
  COMMIT_STATE -> FanOut
```

被取消的 handle 不再产出事件，所以"输掉的那条分支"不会在后续 `PARK` 里冒出来——这是 `CANCELLED ⇒ 静默` 的直接用途。

### 10.4 Map：游标 + 边跑边补

无 Group，join 状态全在父帧的寄存器里：

```
FanOut:
  ENTER_STATE "FanOut"
  EVAL_JSONATA %items, "$states.input.items" !@fail
  CALC %n, len(%items)
  EVAL_JSONATA %maxc, "$states.input.maxParallel" !@fail
  CALC %launched, 0 ; CALC %done, 0 ; CALC %failed, 0
  CALC %results, []
  JMP @launch

launch:                              ; 补位：只在有空位且还有未启动项时
  CALC %inflight, %launched - %done - %failed
  CALC %can, %inflight < %maxc && %launched < %n
  BRANCH %can, @spawn_one, @wait

spawn_one:
  EVAL_JSONATA %item, "$states.context.Map.Item.Value"      ; ItemSelector
  EVAL_JSONATA %idx,  "$states.context.Map.Item.Index"
  EVAL_TEMPLATE %child_input, { "item": ..., "index": ..., "orderId": ... }
  SPAWN %c, @PickItem, %child_input, key=%idx              ; 一条 intent，一个子
  CALC %launched, %launched + 1                            ; 与 intent 同批提交
  JMP @launch

wait:
  PARK

child_done:                          ; 任一子事件
  CURRENT_EVENT %ev
  CALC %idx, %ev.payload.index
  CALC %ok, %ev.kind == "exited_ok"
  CALC %done,   %done   + (%ok ? 1 : 0)
  CALC %failed, %failed + (%ok ? 0 : 1)
  CALC %results, merge(%results, [%idx, %ev.payload.result])
  CALC %tol_fail, %failed > 1                            ; ToleratedFailureCount
  CALC %tol_pct,  %failed * 100 > %n * 10                ; ToleratedFailurePercentage
  BRANCH %tol_fail || %tol_pct, @exceeded, @continue
exceeded:
  CANCEL_CHILDREN
  CALC %err, make_error("States.ExceededTolerance", ...)
  JMP @catch
continue:
  CALC %all, %done + %failed >= %n
  BRANCH %all, @epilogue, @launch       ; 未完成则回补位
```

要点：`launch` 是**每激活最多补一个**（也可放宽到补满空位，但那会让一次提交带多条 intent——按"entry 大小可控"的约束，默认一次一个，由编译器选择批量）。

### 10.5 Parallel：同构，`n` 编译期已知

`ParallelShip` 走同一个形状，差别仅在于：`%n` 是常量、`ordinal` 是分支下标、`ItemSelector` 由 `Arguments` 取代、无容差。

```
  START_TIMER/SPAWN 两个子 ... 或直接 SPAWN 两次（静态 2 条 intent）
  PARK
```

这正是旧稿 `GROUP_NEW` 的作用面：**Map 与 Parallel 共用同一段 lowering 模板**，只在编译期常量与 selector 上不同。

### 10.6 其余状态

| State | lowering |
|---|---|
| `Choice` | `EVAL_JSONATA` 依序求值条件 + `BRANCH`；每条规则的 `Output`/`Assign` 各自一个 mini-epilogue |
| `Succeed` | epilogue 后 `SUCCEED %out` |
| `Fail` | `EVAL_*` 求 `Error`/`Cause` → `FAIL %err` |
| `Pass` | 只有 epilogue |

---

## 11. 与旧稿（`docs/asl-ir-design.md`）的差异

| 项 | 旧稿 | 本稿 | 理由 |
|---|---|---|---|
| 挂起 | `WAIT %who, %msg, srcs...` 返回"哪个源" | `PARK` + handle 路由 | 挂起状态只有一种表示；"哪个源"由分发路径隐含 |
| 回调 | 词法 `!L` 错误边 + `WAIT` 返回值 | `on_*` 路由块 + `RETURN` 到 epilogue | 消除 per-site 的 Catch 特例；续延点固定为状态出口 |
| 并发 | `GROUP_NEW` 一等值 | 父子 Fiber + 计数器 | 少一个值类型；join 状态落在寄存器 |
| 清理 | `Scope` 栈 + `SCOPE_CLOSE` + `NeedAck` | owned handle + `EXITING` 等子全终态 | ack 集合简化为"活子 + 待跑 finalizer"，但结构化并发必须保留 |
| 非确定性 | `Recorder` 逐字节重放 | 无需记录 | 提交点覆盖所有副作用（§8.4） |
| `SELECT` | 保留 | 砍掉 | Race 由编译器在胜者块里 `CANCEL` 其余 handle 展开；无跨构造的多源等待 |

---

## 12. 开放问题

- **O1 Heartbeat handle 形状**：`HEARTBEAT` 需要周期性重置。是可变 handle（`RESET %h, deadline`）还是每拍 `START_TIMER` 一个新 handle（句柄数随时间增长）？
- **O2 静默取消的边界**：`CANCEL` 之后若事件已在 inbox 里（竞态），是丢弃、还是按原 route 分发？本稿取"丢弃 + 事件标记 superseded"。
- **O3 `COMMIT_STATE` 与 `SUCCEED/FAIL` 的重合**：末状态是否可只走 `COMMIT_STATE`（由 `exit` 块尾决定终态），从而少两条指令？
- **O4 块引用跨版本**：本稿取"Fiber 钉住版本 + `(version, label)` 寻址"。若将来要跨版本迁移在飞 Fiber，需要块语义稳定化，成本另计。
- **O5 Map 被取消项的结果槽**：`null` 还是"不写"？（旧稿 OPEN-6 的剩余部分）
- **O6 `Entry` 大小上限**：`launch` 一次补一个 vs 补满空位，取决于单批次 intent 上限；需要与存储层定一个数字。
- **O7 错误对象的寄存器编码**：`err` 需要携带 `Error`/`Cause` 两个字段且可被 `CALC` 匹配；是否直接借用 JSON 对象 + 一个标签位。

---

## 13. 最小样例推导（进行中）

以 `docs/examples/minimal-pass-timeout.json` 为当前讨论基线：machine 级 `TimeoutSeconds = 5`，`StartAt = "Only"`，`Only` 是一个 `Pass + End`。

> 本节记录最近一轮讨论的收敛结果；在这个最小样例上，**本节优先于**前文仍保留的旧写法（尤其是 `PARK` / `CURRENT_EVENT` / `SELECT` 的用法）。待方案最终稳定后，再统一回填 §6 / §8 / §10。

### 13.1 当前已收敛的结果

1. **根 Fiber 与状态执行 Fiber 分离**
   - 根 Fiber 负责 machine 级 timeout、监督 child、收尾。
   - `StartAt = "Only"` 不表示根 Fiber 直接跳到 `Only.entry`，而是根 Fiber 启动一个 child Fiber，由 child Fiber 执行真正的状态机逻辑。

2. **根 Fiber 的启动阶段至少包含四步**
   - `ADD`：计算 machine timeout 的绝对 deadline。
   - `START_TIMER`：登记 machine timeout。
   - `SPAWN`：启动执行 `Only` 的 child Fiber。
   - `WAIT`：显式进入事件驱动阶段。

3. **`WAIT` 是显式指令，且没有顺序后继**
   - `WAIT` 的语义是：等待一个已登记事件；事件到来后，直接跳到该事件对应的 continuation。
   - `WAIT` 不是“暂停一下再执行下一条”；`WAIT` 后的顺序指令在语义上不可达。

4. **不引入 `CURRENT_EVENT`**
   - continuation 不再显式读取“当前事件对象”。
   - 更自然的做法是：事件恢复 continuation 时，运行时按 continuation ABI 直接填好标准寄存器（如 `%res` / `%err`）；程序只处理这些寄存器，不处理事件对象本身。

5. **不接受 `SELECT` 的隐式输家取消语义**
   - `WAIT` 只负责等待与恢复，不负责自动取消其他 timer / task / child。
   - “取消谁、何时取消”必须显式体现在 IR 中，不能藏在运行时里。

6. **`KILL` 的语义已初步确定**
   - `KILL %c` 针对单个 child ref 发终止请求。
   - `KILL` 是**异步**且**幂等**的：它只保证“该 child 已被请求终止”，不保证 child 已同步消失。
   - child 最终仍需自己走退出流程，并以 `exited` 事件回应父 Fiber。

7. **many-children 的清理要用通用 loop 形状表达**
   - 不引入 `SELECT` 自动收尾，也不引入专门的 `ITER_CHILDREN` / `NEXT_CHILD` / `SNAPSHOT_CHILDREN` 指令。
   - 方向上使用通用的 `Loop + BRANCH + JUMP` 来表达“对所有 child 依次执行 `KILL`”。

8. **`children` 列表由运行时维护，不由 IR 自己构造**
   - `SPAWN` 之后，运行时维护当前 Fiber 的 `children` 集合。
   - 程序可以读取该集合并据此循环，但不需要为它设计专门的 `PUSH` 指令。

### 13.2 当前仍未收敛的问题

当前真正卡住的是 **machine timeout 触发后的 draining / 收尾控制流**：

1. 根 Fiber 收到 `timer fired` 后，需要对所有 child 发 `KILL`。
2. 但 `KILL` 只是异步请求；父 Fiber 还必须继续等待，直到所有 child 都真正 `exited`。
3. 同时，`WAIT` 不会“返回到下一条指令”，而是会直接跳到某个 continuation；因此不能写成“`KILL all` → `WAIT` → 醒来后顺序检查”。
4. 此外，同一个 `child exited` 事件在不同阶段的含义不同：
   - 正常阶段：可能意味着 machine 成功或失败。
   - timeout draining 阶段：只意味着“又少了一个活 child”，不应再走正常完成路径。

### 13.3 当前最小骨架

根 Fiber 当前最小骨架可先记为：

```text
root.prologue:
  ADD
  START_TIMER
  SPAWN
  WAIT
```

这四步已经基本稳定；后续重点是把 timeout 之后的 draining 逻辑压成一套足够简单、又不把复杂度藏进运行时的 IR 形状。
