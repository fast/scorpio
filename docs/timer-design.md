# Timer context design

This document records the ownership, scheduling, and benchmark decisions behind Scorpio's timer context. The public contract remains the API and module documentation.

## Ownership model

`TimerDriver::new` returns a `TimerDriver` and `TimerContext` pair. The driver owns advancement and the timing wheel; the cheap-to-clone context owns only submission and clock-observation capability. Applications keep the single-writer driver in their reactor and pass context clones through task boundaries.

Scorpio does not currently expose an aggregate I/O context. With timer as the only concrete capability, a bundle would add an empty state, a builder, and optional access without removing any caller responsibility. Tokio's aggregate driver handle is an internal runtime mechanism: its optional time handle supports a runtime builder that can dynamically disable timers, while ordinary time operations require the capability rather than returning an optional handle. Scorpio instead keeps its public ownership explicit and direct. If multiple independently enabled capabilities emerge, an aggregate can be reconsidered with feature-selected, non-optional fields so a constructed context always contains every compiled capability.

The explicit driver/context split borrows Asio's association between asynchronous objects and an execution context while avoiding an opaque `run` loop or service registry. It leaves room for future network and filesystem capabilities without designing their construction or ownership before those requirements exist.

There is no process-global fallback, thread-local current handle, executor lookup, or crate-owned helper thread. Progress happens only when the application turns the driver.

## Timing wheel

The wheel uses six radix levels with 64 slots each and a one-millisecond base tick. It covers `2^36` milliseconds (about 795 days) before an ordered overflow tier is needed. Future deadlines are rounded up, never down, and upper-level entries cascade toward level zero before firing.

Every slot is an intrusive doubly linked list backed by a slab, so insertion and cancellation are constant-time once an entry's slot is known. A 64-bit occupancy bitmap per level makes empty-slot discovery cheap, and a selected slot remains cached while a bounded turn drains it.

Registrations and cancellations arrive over a multi-producer channel. `TurnBudget` bounds both operation messages and wheel entries per turn, and alternation between immediate and non-immediate entries prevents either class from monopolizing a reactor. Wakers are always invoked outside the wheel's mutation path.

The task-facing context is one shared pointer. Its shared allocation contains the submission endpoint, clock observation, close state, and reactor wake handshake, so cloning a context performs one reference-count update. The wake slots use `mea::atomicbox::AtomicOptionBox`; publishing a new waker allocates one box, while repeated polls with an equivalent task waker reuse the existing registration. Scorpio keeps these synchronization primitives behind the existing `mea` dependency rather than adding direct channel or atomic-waker dependencies.

## Source comparisons

- [Linux timer wheel](https://github.com/torvalds/linux/blob/db2ddb87143519e20a95aa36c60b36107b736a58/kernel/time/timer.c) contributes the intrusive bucket, occupancy bitmap, rounded-up deadline, and timeout-oriented batching ideas. The current kernel wheel deliberately avoids cascading and accepts coarser upper levels because most kernel timeout timers are cancelled before expiry. Scorpio does not copy that semantic tradeoff: general-purpose delays and intervals retain one-millisecond, never-early scheduling across the wheel.
- [Netty `HashedWheelTimer`](https://github.com/netty/netty/blob/540c9781f900b21c06e957bceb3d490daa3f0759/common/src/main/java/io/netty/util/HashedWheelTimer.java) contributes the producer queue, delayed cancellation, intrusive buckets, and bounded transfer pattern. Scorpio rejects Netty's dedicated worker thread and recommendation to share a small number of timer singletons; the application reactor owns every Scorpio driver explicitly.
- [Boost.Asio `io_context`](https://github.com/boostorg/asio/blob/4fa4abee89a62fdeeccac2585caece625f40647e/include/boost/asio/io_context.hpp), [`basic_waitable_timer`](https://github.com/boostorg/asio/blob/4fa4abee89a62fdeeccac2585caece625f40647e/include/boost/asio/basic_waitable_timer.hpp), and [`timer_queue`](https://github.com/boostorg/asio/blob/4fa4abee89a62fdeeccac2585caece625f40647e/include/boost/asio/detail/timer_queue.hpp) motivate explicit context association, earliest-deadline reactor interruption, and deterministic shutdown. Scorpio exposes its timer driver and task-facing context directly, uses a hierarchical wheel rather than a binary heap, and returns futures rather than dispatching callbacks from a context-owned run loop.
- [Tokio's runtime driver](https://github.com/tokio-rs/tokio/blob/tokio-1.53.1/tokio/src/runtime/driver.rs) and [timer wheel](https://github.com/tokio-rs/tokio/tree/tokio-1.53.1/tokio/src/runtime/time) validate the internal driver/handle split, six-level radix geometry, intrusive entries, and careful racing-reset protocol. Tokio aggregates handles because it owns the full runtime and discovers the current scheduler implicitly. Scorpio does not store or discover a scheduler handle in timer futures; every `Delay` is created from a caller-supplied `TimerContext`.
- [`async-io`](https://github.com/smol-rs/async-io/blob/576b470ca3cadefdec8b169279df23c9a0a63495/src/lib.rs) and [`futures-timer`](https://github.com/async-rs/futures-timer/blob/e8d2e877147bcdbe5b64c0fbf5df1f2dce1c4253/src/native/timer.rs) provide useful public-API benchmark comparisons, but both use a global fallback and lazily start a helper thread. Those ownership choices are intentionally outside Scorpio's contract.

## Performance regression suite

Run `cargo x bench --quick` for an isolated-process, single-iteration smoke check. Without a filter, the xtask launches each implementation family in a fresh process so async-io and futures-timer helper threads cannot leak into another implementation's samples. Add a name filter such as `cargo x bench expire_registered` for focused Divan measurements where that isolation is not required. Run the same filtered command on both revisions for before/after comparisons, keeping the machine, power mode, toolchain, and background load fixed.

The `timer/frontend_lifecycle` group measures the relative-delay create, first-poll, and drop boundary for Scorpio, Tokio, async-io, and futures-timer at 1, 64, and 1,024 timers. It does not claim that background or driver-side work is part of that boundary. Divan creates a fresh Scorpio driver/context pair outside each measured iteration; deferred output destruction then drains a count-bounded operation batch and verifies that no deadline remains, also outside the measured interval.

The `timer/scorpio_driver` group separately measures registration and cancellation queue draining, which is work that Scorpio deliberately exposes to the application-owned reactor. Deferred validators prove that every declared item was registered or cancelled and that no deadline remains, without adding validation work to the measured interval. Keeping this group separate prevents a cross-implementation benchmark from mixing caller latency with asynchronously deferred backend work.

The `timer/expire_registered` group excludes setup and measures Scorpio's direct driver turn plus terminal polling. Same-deadline cases cover bucket draining, while a distribution spanning every selected wheel level covers cascade work. Each driver input is constructed, measured, and dropped alone, and the driven clock avoids wall-clock sleeping.

These results are regression and tradeoff evidence, not a universal ranking. The compared crates make different ownership, executor, precision, and background-thread choices; benchmark names keep those paths separate rather than claiming identical semantics.
