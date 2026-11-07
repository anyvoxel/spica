# 基于 Fiber、Handle 和 Continuation 的持久化事件驱动虚拟机

**Status:** design record, not implemented. 与 `docs/asl-ir-design.md`（ASL → IR 的 21 条指令集，Scope/Group 方案）是同一执行半边的两种收敛形态；本文为较新的一稿。

目前我们的设计已经基本收敛为一个基于 Fiber、Handle 和 Continuation 的持久化事件驱动虚拟机。

整体思路如下。

### 核心执行模型

Fiber 是执行单元。

每个 Fiber 有自己的 PC、Registers、Variables 和执行上下文，可以独立运行、暂停、持久化和恢复。

Fiber 之间可以形成父子关系。父 Fiber 可以通过 `SPAWN` 动态创建子 Fiber，不需要在编译期知道子 Fiber 的数量。

Handle 是异步操作的引用。

`START_TIMER`、`START_TASK`、`SPAWN` 等原生指令创建异步操作或执行实体，并返回 Handle。

Handle 是可持久化的逻辑 ID，而不是内存指针。

Fiber Frame 保存 Handle 引用，运行时负责维护 Handle 对应操作的生命周期和状态。

Continuation 负责事件分发和控制流。

创建 Handle 时可以注册 Continuation，指定不同事件发生后应该进入哪个 Basic Block：

```
%timer = START_TIMER(
    deadline = %deadline,
    on_fire = @TimeoutHandler
)

%task = START_TASK(
    arguments = %args,
    on_success = @SuccessHandler,
    on_failure = @FailureHandler
)
```

Continuation 是持久化的 Basic Block 引用，而不是内存中的回调函数。

### 并发与 Fiber 之间的通信

不引入 Group 对象，直接通过父子 Fiber 关系管理动态执行实体。

```
Parent Fiber
  ├── Child Fiber A
  ├── Child Fiber B
  └── Child Fiber C
```

`SPAWN` 创建子 Fiber，并建立父子关系。子 Fiber 完成或失败时，向父 Fiber 的持久化 Inbox 投递事件。

例如：

```
ChildEvent {
    event_id
    child_id
    child_key
    kind
    result
}
```

运行时根据子 Fiber 注册的 Continuation，唤醒父 Fiber，进入对应的 Basic Block。

这里有一个重要约束：同一个 Fiber 不能被多个事件并发唤醒并同时执行。

多个事件到达时，先进入 Inbox，再由运行时串行调度。

父 Fiber 可以使用：

```
PARK()
```

暂停执行。当 Inbox 有待处理事件时恢复执行；否则继续等待。

### 资源生命周期与清理

不引入 Scope，资源清理直接与 Handle 关联。

```
%timer = START_TIMER(
    deadline = %deadline,
    on_fire = @TimerFired,
    on_release = @CancelTimer
)
```

Handle 可以关联 Continuation 和 Finalizer。

每个 Fiber 维护动态的 Owned Handles 集合，默认拥有自己创建的 Handle。

- `RELEASE(handle)`：主动释放资源，执行清理逻辑。
- Fiber 退出：释放尚未释放的 Owned Handles。
- `CANCEL_CHILDREN()`：向所有子 Fiber 发送取消请求。
- 子 Fiber 自己负责执行清理，完成后进入终态。

另外，Subscription 的取消和 Handle 的释放是不同的操作，不能混淆。

### 持久化与恢复

运行时负责持久化保证，主要涉及：

- Fiber Frame：PC、Registers、状态等。
- Handle：操作 ID、操作状态和必要的恢复信息。
- Continuation：事件订阅与目标 Basic Block。
- Parent-Child 关系：动态子 Fiber 集合。
- Inbox：尚未处理的事件。
- 清理状态：已经完成和仍待执行的 Finalizer。

`SPAWN` 必须有稳定的 Child ID，保证恢复时不会重复创建子 Fiber。

子 Fiber 完成和父 Fiber 事件投递需要可靠关联，例如通过事务或 WAL/Outbox 机制，避免事件丢失。

外部 Task 则需要稳定的 Operation ID 和幂等机制，处理崩溃恢复时的重复派发问题。

### 当前确定的基本 IR 指令

| 类别   | 指令                                      |
| ---- | --------------------------------------- |
| 控制流  | `JUMP`、`BRANCH`、`RETURN`、`FAIL`         |
| 数据处理 | `EVAL_JSONATA`、`EVAL_TEMPLATE`、`ASSIGN` |
| 状态转换 | `ENTER_STATE`、`COMMIT_STATE`            |
| 异步操作 | `START_TIMER`、`START_TASK`、`SPAWN`      |
| 事件处理 | Continuation、`CURRENT_EVENT`、`PARK`     |
| 生命周期 | `CANCEL`、`RELEASE`、`CANCEL_CHILDREN`    |

`SELECT` 暂时不需要作为所有异步操作的必要指令，因为大部分等待逻辑可以通过 Continuation 表达。不过，对于需要同时监听多个事件源的场景，仍可以保留它。

最终的设计原则：

IR 描述执行逻辑和控制流；Fiber 是执行单元；Handle 引用异步操作；Continuation 描述事件触发后的执行路径；父子关系管理动态并发；运行时负责调度、持久化和恢复。

我认为下一步应该重点完善两个方面：`SPAWN` 的精确语义，以及 Continuation 的注册、触发、消费和恢复协议。这两部分是整个运行时正确性的基础。
