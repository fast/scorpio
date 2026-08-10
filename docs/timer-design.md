# Timer context design

This document records the ownership, scheduling, and benchmark decisions behind Scorpio's timer context. The public contract remains the API and module documentation.

## Ownership model

`IoContext` is an immutable, cloneable capability bundle, not a runtime. Applications explicitly construct each source, keep its single-writer driver in their reactor, and install only the task-facing contexts they need. An empty `IoContext` allocates no source and starts nothing.

`TimerDriver::new` returns a `TimerDriver` and `TimerContext` pair. The driver owns advancement and the timing wheel; the cheap-to-clone context owns only submission and clock-observation capability. Installing the latter with `IoContext::new().with_timer(timer)` never transfers or hides driver ownership.

This split deliberately borrows Asio's explicit association between asynchronous objects and an execution context while avoiding an opaque `run` loop or service registry. It also leaves room for future network and filesystem capabilities without forcing applications that only need timers to construct them.

There is no process-global fallback, thread-local current handle, executor lookup, or crate-owned helper thread. Progress happens only when the application turns the driver.

## Timing wheel

The wheel uses six radix levels with 64 slots each and a one-millisecond base tick. It covers `2^36` milliseconds (about 795 days) before an ordered overflow tier is needed. Future deadlines are rounded up, never down, and upper-level entries cascade toward level zero before firing.

Every slot is an intrusive doubly linked list backed by a slab, so insertion and cancellation are constant-time once an entry's slot is known. A 64-bit occupancy bitmap per level makes empty-slot discovery cheap, and a selected slot remains cached while a bounded turn drains it.

Registrations and cancellations arrive over a multi-producer channel. `TurnBudget` bounds both operation messages and wheel entries per turn, and alternation between immediate and non-immediate entries prevents either class from monopolizing a reactor. Wakers are always invoked outside the wheel's mutation path.

The task-facing context is one shared pointer. Its shared allocation contains the submission endpoint, clock observation, close state, and reactor wake handshake, so cloning a context performs one reference-count update. Timer and reactor wake slots use an inline `AtomicWaker`; polling does not allocate a box merely to publish a waker, and repeated equivalent registrations reuse the stored waker.

## Source comparisons

- [Linux timer wheel](https://github.com/torvalds/linux/blob/db2ddb87143519e20a95aa36c60b36107b736a58/kernel/time/timer.c) contributes the intrusive bucket, occupancy bitmap, rounded-up deadline, and timeout-oriented batching ideas. The current kernel wheel deliberately avoids cascading and accepts coarser upper levels because most kernel timeout timers are cancelled before expiry. Scorpio does not copy that semantic tradeoff: general-purpose delays and intervals retain one-millisecond, never-early scheduling across the wheel.
- [Netty `HashedWheelTimer`](https://github.com/netty/netty/blob/540c9781f900b21c06e957bceb3d490daa3f0759/common/src/main/java/io/netty/util/HashedWheelTimer.java) contributes the producer queue, delayed cancellation, intrusive buckets, and bounded transfer pattern. Scorpio rejects Netty's dedicated worker thread and recommendation to share a small number of timer singletons; the application reactor owns every Scorpio driver explicitly.
- [Boost.Asio `io_context`](https://github.com/boostorg/asio/blob/4fa4abee89a62fdeeccac2585caece625f40647e/include/boost/asio/io_context.hpp), [`basic_waitable_timer`](https://github.com/boostorg/asio/blob/4fa4abee89a62fdeeccac2585caece625f40647e/include/boost/asio/basic_waitable_timer.hpp), and [`timer_queue`](https://github.com/boostorg/asio/blob/4fa4abee89a62fdeeccac2585caece625f40647e/include/boost/asio/detail/timer_queue.hpp) motivate explicit context association, optional services, earliest-deadline reactor interruption, and deterministic shutdown. Scorpio keeps source drivers outside `IoContext`, uses a hierarchical wheel rather than a binary heap, and returns futures rather than dispatching callbacks from a context-owned run loop.
- [Tokio's timer wheel](https://github.com/tokio-rs/tokio/tree/75fef53d0a8590c2d1dbb63672aa7b7d1ef51155/tokio/src/runtime/time) validates the six-level radix geometry, intrusive entries, and careful racing-reset protocol. Scorpio does not store or discover a scheduler handle in timer futures; every `Delay` is created from a caller-supplied context.
- [`async-io`](https://github.com/smol-rs/async-io/blob/576b470ca3cadefdec8b169279df23c9a0a63495/src/lib.rs) and [`futures-timer`](https://github.com/async-rs/futures-timer/blob/e8d2e877147bcdbe5b64c0fbf5df1f2dce1c4253/src/native/timer.rs) provide useful public-API benchmark comparisons, but both use a global fallback and lazily start a helper thread. Those ownership choices are intentionally outside Scorpio's contract.

## Performance regression suite

Run `cargo x bench --quick` for an isolated-process smoke measurement. Without a filter, the xtask launches each implementation family in a fresh process so async-io and futures-timer helper threads cannot leak into another implementation's samples. Add a name filter such as `cargo x bench --quick expire_registered` only for focused diagnosis where that isolation is not required. For a statistically sampled before/after comparison, run `cargo x bench --save-baseline main` on the reference revision and `cargo x bench --baseline main` after the change. Keep the machine, power mode, toolchain, and background load fixed.

The `timer/frontend_lifecycle` group measures the relative-delay create, first-poll, and drop boundary for Scorpio, Tokio, async-io, and futures-timer at 1, 64, and 1,024 timers. It does not claim that background or driver-side work is settled. Scorpio periodically drains cancelled submissions outside the measured intervals, verifies the queue is drained, and keeps that explicit driver work outside this API-side measurement.

The `timer/scorpio_driver` group separately measures registration and cancellation queue draining, which is work that Scorpio deliberately exposes to the application-owned reactor. Keeping it separate prevents a cross-implementation benchmark from mixing caller latency with asynchronously deferred backend work.

The `timer/expire_registered` group excludes setup and measures two deliberately named ownership paths: Scorpio's direct driver turn plus terminal polling, and Tokio's paused-runtime `advance` orchestration plus terminal polling. They are useful implementation-specific regression signals, not a direct subtraction of equivalent primitives. A Scorpio distribution spanning every selected wheel level covers cascade work. Each heavyweight driver or runtime input is constructed, measured, and dropped alone; Tokio's paused clock and Scorpio's driven clock avoid wall-clock sleeping.

These results are regression and tradeoff evidence, not a universal ranking. The compared crates make different ownership, executor, precision, and background-thread choices; benchmark names keep those paths separate rather than claiming identical semantics.
