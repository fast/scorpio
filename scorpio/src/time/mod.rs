// Copyright 2025 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Explicitly driven timer primitives for application-owned reactors.
//!
//! [`TimerService`] owns time advancement. It starts no thread and opens no I/O resource; an event
//! loop calls [`TimerService::turn`] and [`TimerService::prepare_wait`]. Tasks receive a cloneable
//! [`TimerHandle`] and create lazy [`Delay`] futures from it.
//!
//! # Reactor contract
//!
//! After each timer turn, the integrating event loop calls [`TimerService::prepare_wait`] once and
//! applies the returned [`WaitPlan`] to its I/O poll. [`WaitPlan::Immediate`] means the poll must
//! not block. After dispatching ready I/O, the loop calls `turn` again. `prepare_wait` registers
//! the reactor waker and chooses the timer deadline as one operation, so callers cannot introduce a
//! lost-wakeup window by performing those steps in the wrong order.
//!
//! # Operation backlog and cancellation
//!
//! Registrations and cancellations use an unbounded operation queue. One turn processes at most
//! [`TurnBudget::max_operations`] messages. When producers outpace the service, operations remain
//! queued instead of being rejected for capacity, so [`TimerService::prepare_wait`] returns
//! [`WaitPlan::Immediate`] while a backlog remains.
//!
//! Dropping a submitted delay prevents its registration from becoming active in the wheel.
//! Dropping an already registered delay marks it cancelled immediately, but queues its removal
//! from the wheel; the entry can therefore remain linked and consume turn budget until a later
//! turn processes that cancellation.
//!
//! # Resolution
//!
//! The service uses a 1 ms scheduling resolution. Future deadlines are rounded up to the next tick,
//! so a timer never fires early because of wheel rounding, but may become ready up to one tick
//! after its requested deadline in addition to any reactor wake-up delay.
//!
//! # Examples
//!
//! ```
//! use std::future::Future;
//! use std::pin::pin;
//! use std::task::Context;
//! use std::task::Poll;
//! use std::task::Waker;
//! use std::time::Duration;
//! use std::time::Instant;
//!
//! use scorpio::time::TimerService;
//! use scorpio::time::TurnBudget;
//! use scorpio::time::WaitPlan;
//!
//! let start = Instant::now();
//! let deadline = start + Duration::from_millis(10);
//! let mut service = TimerService::new_at(start);
//! let timer = service.handle();
//! let mut delay = pin!(timer.delay_until(deadline));
//! let mut cx = Context::from_waker(Waker::noop());
//!
//! assert!(delay.as_mut().poll(&mut cx).is_pending());
//! // The first poll queued a registration. One turn moves it into the timing wheel.
//! service.turn(start, TurnBudget::default());
//!
//! // Preparing to wait atomically arms the reactor and returns the timer deadline.
//! assert_eq!(
//!     service.prepare_wait(Waker::noop()),
//!     WaitPlan::Until(deadline)
//! );
//! service.turn(start + Duration::from_millis(10), TurnBudget::default());
//! assert!(matches!(delay.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
//! ```

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::ops::AsyncFnMut;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use crate::time::wheel::Deadline;
use crate::time::wheel::Step;
use crate::time::wheel::Wheel;

#[cfg(test)]
mod tests;
mod wheel;

/// The delay submitted a registration, but the service has not published it as registered.
const STATE_SUBMITTED: u8 = 0;
/// The service has linked the timer into its wheel.
const STATE_REGISTERED: u8 = 1;
/// The service reached the deadline and published the completion observation.
const STATE_FIRED: u8 = 2;
/// The delay was dropped before it completed.
const STATE_CANCELLED: u8 = 3;
/// The backing service closed before it completed.
const STATE_CLOSED: u8 = 4;
/// Sentinel indicating that a timer state has no live wheel entry.
const NO_SLOT: usize = usize::MAX;

/// Work admitted during one [`TimerService::turn`].
///
/// The default admits up to 1,024 operation-queue messages and 4,096 timer entries per turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnBudget {
    max_operations: NonZeroUsize,
    max_timer_entries: NonZeroUsize,
}

impl TurnBudget {
    /// Creates a turn budget from non-zero operation and timer-entry limits.
    pub const fn new(max_operations: NonZeroUsize, max_timer_entries: NonZeroUsize) -> Self {
        Self {
            max_operations,
            max_timer_entries,
        }
    }

    /// Returns the maximum number of operation-queue messages processed by one turn.
    pub const fn max_operations(self) -> NonZeroUsize {
        self.max_operations
    }

    /// Returns the maximum number of wheel entries examined by one turn.
    pub const fn max_timer_entries(self) -> NonZeroUsize {
        self.max_timer_entries
    }
}

impl Default for TurnBudget {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(1024).unwrap(),
            NonZeroUsize::new(4096).unwrap(),
        )
    }
}

/// How the reactor should wait before its next timer turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "the reactor must apply the returned wait plan"]
pub enum WaitPlan {
    /// Poll I/O without blocking, then turn the timer service again.
    Immediate,
    /// Wait until this deadline unless I/O or the registered waker interrupts the reactor first.
    Until(Instant),
    /// Wait indefinitely unless I/O or the registered waker interrupts the reactor.
    Indefinite,
}

/// Error returned when the service backing a timer handle has been dropped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimerClosed;

impl fmt::Display for TimerClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("timer service is closed")
    }
}

impl std::error::Error for TimerClosed {}

#[derive(Clone, Copy)]
enum ClockMode {
    System,
    Driven,
}

struct OperationQueue {
    inner: Mutex<OperationQueueInner>,
}

struct OperationQueueInner {
    accepting: bool,
    operations: VecDeque<Operation>,
    reactor_waker: Option<Waker>,
}

impl OperationQueue {
    fn new() -> Self {
        Self {
            inner: Mutex::new(OperationQueueInner {
                accepting: true,
                operations: VecDeque::new(),
                reactor_waker: None,
            }),
        }
    }

    fn send(&self, operation: Operation) -> Result<(), Operation> {
        let waker = {
            let mut inner = self.inner.lock().unwrap();
            if !inner.accepting {
                return Err(operation);
            }
            let first = inner.operations.is_empty();
            inner.operations.push_back(operation);
            first.then(|| inner.reactor_waker.take()).flatten()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    fn arm(&self, waker: &Waker) -> bool {
        // Clone before locking because a custom RawWaker vtable may panic. Replaced wakers are also
        // dropped after unlocking so no user-provided vtable code runs in the queue critical path.
        let mut replacement = Some(waker.clone());
        let (armed, previous) = {
            let mut inner = self.inner.lock().unwrap();
            if !inner.operations.is_empty() || !inner.accepting {
                (false, inner.reactor_waker.take())
            } else if inner
                .reactor_waker
                .as_ref()
                .is_some_and(|registered| registered.will_wake(waker))
            {
                (true, None)
            } else {
                (
                    true,
                    inner
                        .reactor_waker
                        .replace(replacement.take().expect("replacement waker must exist")),
                )
            }
        };
        drop(previous);
        drop(replacement);
        armed
    }

    fn drain_into(&self, limit: usize, target: &mut VecDeque<Operation>) {
        debug_assert!(target.is_empty());
        let mut inner = self.inner.lock().unwrap();
        let count = limit.min(inner.operations.len());
        if count == inner.operations.len() {
            std::mem::swap(target, &mut inner.operations);
        } else {
            target.extend(inner.operations.drain(..count));
        }
    }

    fn close(&self, closed: &AtomicBool) -> (VecDeque<Operation>, Option<Waker>) {
        let mut inner = self.inner.lock().unwrap();
        inner.accepting = false;
        closed.store(true, Ordering::Release);
        (
            std::mem::take(&mut inner.operations),
            inner.reactor_waker.take(),
        )
    }

    #[cfg(test)]
    fn disconnect(&self) {
        self.inner.lock().unwrap().accepting = false;
    }
}

/// A single-writer observation stored as nanoseconds since the service's genesis.
struct AtomicObservation {
    genesis: Instant,
    nanos: AtomicU64,
}

impl AtomicObservation {
    fn new(genesis: Instant) -> Self {
        Self {
            genesis,
            nanos: AtomicU64::new(0),
        }
    }

    fn load(&self) -> Instant {
        // ORDERING: The location publishes no other state, so per-location coherence alone gives
        // every reader a monotonically non-decreasing observation.
        let nanos = self.nanos.load(Ordering::Relaxed);
        self.decode(nanos)
    }

    fn store(&self, observation: Instant) {
        // ORDERING: Only the service stores here, and readers synchronize with timer completions
        // through `TimerState`, never through this value.
        self.nanos
            .store(self.encode(observation), Ordering::Relaxed);
    }

    fn encode(&self, observation: Instant) -> u64 {
        encode_observation_nanos(observation.duration_since(self.genesis))
    }

    fn decode(&self, nanos: u64) -> Instant {
        self.genesis
            .checked_add(Duration::from_nanos(nanos))
            .expect("published observation must remain representable")
    }
}

fn encode_observation_nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).expect("timer observation exceeds the u64 nanosecond range")
}

struct Shared {
    clock_mode: ClockMode,
    closed: AtomicBool,
    observed: AtomicObservation,
    operations: OperationQueue,
}

impl Shared {
    fn observation(&self) -> Instant {
        let published = self.observed.load();
        match self.clock_mode {
            ClockMode::System => Instant::now().max(published),
            ClockMode::Driven => published,
        }
    }
}

const WAKER_READY: u8 = 0;
const WAKER_REGISTERING: u8 = 1;
// Terminal is absorbing. A READY predecessor transfers the waker to the publisher; a REGISTERING
// predecessor delegates cleanup to the polling task that already owns the slot.
const WAKER_TERMINAL: u8 = 2;

struct DelayWakeSlot {
    state: AtomicU8,
    waker: UnsafeCell<Option<Waker>>,
}

#[allow(
    unsafe_code,
    reason = "the slot state transfers exclusive inline-waker access between threads"
)]
unsafe impl Sync for DelayWakeSlot {}

// RawWaker clone and drop operations run only before acquiring or after releasing the slot state,
// so unwinding cannot expose a partially updated inline waker through a shared reference.
impl std::panic::RefUnwindSafe for DelayWakeSlot {}

impl DelayWakeSlot {
    fn new(waker: Waker) -> Self {
        Self {
            state: AtomicU8::new(WAKER_READY),
            waker: UnsafeCell::new(Some(waker)),
        }
    }

    fn register_and_load(
        &self,
        waker: &Waker,
        lifecycle: &AtomicU8,
    ) -> (u8, Option<WakerIdentity>) {
        self.register_and_load_with(waker, lifecycle, || {})
    }

    #[allow(
        unsafe_code,
        reason = "WAKER_REGISTERING grants exclusive access to replace the inline waker"
    )]
    fn register_and_load_with<F>(
        &self,
        waker: &Waker,
        lifecycle: &AtomicU8,
        after_claim: F,
    ) -> (u8, Option<WakerIdentity>)
    where
        F: FnOnce(),
    {
        // Clone before claiming the slot because a custom RawWaker vtable may panic.
        let registered = waker.clone();
        let identity = WakerIdentity::new(&registered);
        // ORDERING: acquire takes ownership from the preceding READY publication. A failed acquire
        // observes a terminal publisher's state transition, which follows its lifecycle update.
        if self
            .state
            .compare_exchange(
                WAKER_READY,
                WAKER_REGISTERING,
                Ordering::Acquire,
                Ordering::Acquire,
            )
            .is_err()
        {
            drop(registered);
            return (lifecycle.load(Ordering::Acquire), None);
        }

        // Tests pause here to force terminal publication through the REGISTERING branch. The
        // production caller passes an empty closure, which optimizes away.
        after_claim();

        // SAFETY: only the thread that changed READY to REGISTERING may write the slot.
        let previous = unsafe { (&mut *self.waker.get()).replace(registered) };
        // ORDERING: release publishes the replacement before READY. On failure, acquire observes
        // the terminal transition and therefore the lifecycle update that preceded it.
        if let Err(state) = self.state.compare_exchange(
            WAKER_REGISTERING,
            WAKER_READY,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            assert_eq!(
                state, WAKER_TERMINAL,
                "invalid waker state after registration"
            );
            // SAFETY: a terminal publisher changed REGISTERING to TERMINAL and transferred cleanup
            // to this polling thread.
            let current = unsafe { (&mut *self.waker.get()).take() };
            drop(current);
        }
        drop(previous);
        (lifecycle.load(Ordering::Acquire), Some(identity))
    }

    fn clear(&self) {
        drop(self.take());
    }

    #[allow(
        unsafe_code,
        reason = "READY-to-TERMINAL grants exclusive access to take the inline waker"
    )]
    fn take(&self) -> Option<Waker> {
        // ORDERING: acquire observes a replacement published before READY. Release makes a prior
        // lifecycle transition visible to a polling task that currently owns REGISTERING.
        match self.state.swap(WAKER_TERMINAL, Ordering::AcqRel) {
            WAKER_READY => {
                // SAFETY: only the thread that changed READY to TERMINAL may read the slot.
                unsafe { (&mut *self.waker.get()).take() }
            }
            WAKER_REGISTERING | WAKER_TERMINAL => None,
            state => panic!("invalid waker state before take: {state}"),
        }
    }
}

// The slot owns the live Waker. This non-owning identity avoids retaining a second clone solely
// for the raw-identity repoll check; equal data and vtable pointers are the fast path used by
// `Waker::will_wake`.
#[derive(Clone, Copy, Eq, PartialEq)]
struct WakerIdentity {
    data: usize,
    vtable: usize,
}

impl WakerIdentity {
    fn new(waker: &Waker) -> Self {
        Self {
            data: waker.data() as usize,
            vtable: waker.vtable() as *const _ as usize,
        }
    }

    fn matches(self, waker: &Waker) -> bool {
        self == Self::new(waker)
    }
}

struct TimerState {
    // Written before publishing `STATE_FIRED` and read only after observing it.
    fired_nanos: AtomicU64,
    lifecycle: AtomicU8,
    // Only the service accesses this reclamation hint. Atomic interior mutability keeps the shared
    // state `Sync`; the service validates a non-sentinel value with `Arc::ptr_eq` before using it.
    slot: AtomicUsize,
    waker: DelayWakeSlot,
}

impl TimerState {
    fn new(waker: Waker) -> Self {
        Self {
            fired_nanos: AtomicU64::new(0),
            lifecycle: AtomicU8::new(STATE_SUBMITTED),
            slot: AtomicUsize::new(NO_SLOT),
            waker: DelayWakeSlot::new(waker),
        }
    }

    fn publish_terminal(&self, from: u8, to: u8) -> Option<Waker> {
        // ORDERING: The release half publishes terminal payload such as `fired_nanos` to a polling
        // task's acquire lifecycle load. Waker publication and transfer are ordered separately by
        // `DelayWakeSlot`'s ownership-state transitions.
        if self
            .lifecycle
            .compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.waker.take()
        } else {
            None
        }
    }
}

struct RegisterOp {
    deadline: Deadline,
    // Service processing takes this value. If the operation is dropped while it still owns the
    // state, `Drop` closes the submitted delay instead of leaving it pending forever.
    state: Option<Arc<TimerState>>,
}

impl RegisterOp {
    fn into_state(mut self) -> Arc<TimerState> {
        self.state.take().expect("register op must own its state")
    }
}

impl Drop for RegisterOp {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        if let Some(waker) = state.publish_terminal(STATE_SUBMITTED, STATE_CLOSED) {
            waker.wake();
        }
    }
}

enum Operation {
    Register(RegisterOp),
    Cancel(Arc<TimerState>),
}

/// A cheap-to-clone handle for creating timers from tasks.
///
/// See the [module level documentation](self) for the service and reactor integration model.
#[derive(Clone)]
pub struct TimerHandle {
    shared: Arc<Shared>,
}

impl fmt::Debug for TimerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerHandle").finish_non_exhaustive()
    }
}

impl TimerHandle {
    /// Returns this timer capability's current clock observation.
    ///
    /// A system-clock handle returns the later of [`Instant::now`] and the service's last
    /// observation. A handle created by [`TimerService::new_at`] advances only when its service is
    /// turned.
    pub fn now(&self) -> Instant {
        self.shared.observation()
    }

    /// Creates a lazy delay that completes at or after `deadline`.
    pub fn delay_until(&self, deadline: Instant) -> Delay {
        self.delay_to(Deadline::At(deadline))
    }

    /// Creates a lazy delay for `duration` from the handle's current observation.
    ///
    /// An unrepresentable addition creates a delay that can complete only when its service closes.
    pub fn delay(&self, duration: Duration) -> Delay {
        self.delay_to(Deadline::checked_add(self.now(), duration))
    }

    /// Runs `future` until it completes or `duration` elapses.
    ///
    /// The guarded future wins when both branches become ready in the same poll. The returned
    /// future owns a timer handle and can therefore cross a `'static` task boundary when `future`
    /// can.
    pub fn timeout<F>(
        &self,
        duration: Duration,
        future: F,
    ) -> impl Future<Output = Result<F::Output, TimeoutError>> + use<F>
    where
        F: Future,
    {
        timeout_with_delay(self.delay(duration), future)
    }

    /// Runs `future` until it completes or `deadline` is reached.
    ///
    /// The guarded future wins when both branches become ready in the same poll.
    pub fn timeout_at<F>(
        &self,
        deadline: Instant,
        future: F,
    ) -> impl Future<Output = Result<F::Output, TimeoutError>> + use<F>
    where
        F: Future,
    {
        timeout_with_delay(self.delay_until(deadline), future)
    }

    /// Creates an interval with an immediate first tick.
    ///
    /// # Panics
    ///
    /// Panics when `period` is zero.
    pub fn interval(&self, period: Duration) -> Interval {
        interval_from_deadline(self, Deadline::At(self.now()), period)
    }

    /// Creates an interval whose first tick is scheduled at `start`.
    ///
    /// # Panics
    ///
    /// Panics when `period` is zero.
    pub fn interval_at(&self, start: Instant, period: Duration) -> Interval {
        interval_from_deadline(self, Deadline::At(start), period)
    }

    /// Repeatedly runs `task`, waiting `delay` after each completion.
    ///
    /// `None` starts the first task immediately; `Some(duration)` delays the first invocation.
    /// This future otherwise runs until it is dropped, returning [`TimerClosed`] only if the
    /// service closes.
    ///
    /// # Panics
    ///
    /// Panics when first polled if `delay` is zero.
    pub fn schedule_with_fixed_delay<F>(
        &self,
        initial_delay: Option<Duration>,
        delay: Duration,
        task: F,
    ) -> impl Future<Output = Result<(), TimerClosed>> + use<F>
    where
        F: AsyncFnMut(),
    {
        schedule_with_fixed_delay(self.clone(), initial_delay, delay, task)
    }

    /// Repeatedly runs `task` on a fixed grid, skipping missed invocations without overlap.
    ///
    /// `None` starts the first task immediately; `Some(duration)` delays the first invocation. A
    /// task that outruns its period does not cause following invocations to run back to back.
    ///
    /// # Panics
    ///
    /// Panics when first polled if `period` is zero.
    pub fn schedule_at_fixed_rate<F>(
        &self,
        initial_delay: Option<Duration>,
        period: Duration,
        task: F,
    ) -> impl Future<Output = Result<(), TimerClosed>> + use<F>
    where
        F: AsyncFnMut(),
    {
        schedule_at_fixed_rate(self.clone(), initial_delay, period, task)
    }

    /// Repeatedly runs `task`, using each returned instant as the next deadline.
    ///
    /// `None` starts the first task immediately; `Some(duration)` delays the first invocation.
    /// Returning an elapsed instant repeatedly creates an intentionally busy loop.
    pub fn schedule_with_arbitrary_delay<F>(
        &self,
        initial_delay: Option<Duration>,
        task: F,
    ) -> impl Future<Output = Result<(), TimerClosed>> + use<F>
    where
        F: AsyncFnMut() -> Instant,
    {
        schedule_with_arbitrary_delay(self.clone(), initial_delay, task)
    }

    fn delay_to(&self, deadline: Deadline) -> Delay {
        Delay {
            handle: self.clone(),
            deadline,
            closed: false,
            fired_at: None,
            registered_waker: None,
            state: None,
        }
    }

    fn send(&self, operation: Operation) -> Result<(), Operation> {
        self.shared.operations.send(operation)
    }
}

/// Reactor-owned timer service.
///
/// The service starts no thread and performs no I/O. A reactor advances it with
/// [`turn`](Self::turn), then obtains an atomic parking decision through
/// [`prepare_wait`](Self::prepare_wait).
///
/// See the [module level documentation](self) for the complete reactor contract.
pub struct TimerService {
    last_now: Instant,
    operation_batch: VecDeque<Operation>,
    prefer_non_immediate: bool,
    shared: Arc<Shared>,
    wheel: Wheel<Arc<TimerState>>,
}

impl fmt::Debug for TimerService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerService")
            .field("last_now", &self.last_now)
            .finish_non_exhaustive()
    }
}

impl TimerService {
    /// Constructs a system-clock service.
    ///
    /// Handles obtained from [`handle`](Self::handle) observe the later of the system clock and the
    /// service's last published observation, even between turns. Registered timers still complete
    /// only when the reactor calls [`turn`](Self::turn).
    pub fn new() -> Self {
        let genesis = Instant::now();
        Self::with_clock(genesis, ClockMode::System)
    }

    /// Constructs a deterministic service whose handle clock advances only through
    /// [`turn`](Self::turn).
    ///
    /// This constructor is useful for reactor tests and simulations. It does not read the system
    /// clock after construction.
    pub fn new_at(genesis: Instant) -> Self {
        Self::with_clock(genesis, ClockMode::Driven)
    }

    fn with_clock(genesis: Instant, clock_mode: ClockMode) -> Self {
        let shared = Arc::new(Shared {
            clock_mode,
            closed: AtomicBool::new(false),
            observed: AtomicObservation::new(genesis),
            operations: OperationQueue::new(),
        });
        Self {
            last_now: genesis,
            operation_batch: VecDeque::new(),
            prefer_non_immediate: true,
            shared,
            wheel: Wheel::new(genesis),
        }
    }

    /// Returns a cloneable task-side timer handle.
    pub fn handle(&self) -> TimerHandle {
        TimerHandle {
            shared: self.shared.clone(),
        }
    }

    /// Atomically prepares the reactor to wait after a timer turn.
    ///
    /// This method combines the operation-queue wake handshake with the next timer deadline so the
    /// reactor cannot observe an empty queue before registering its wake. The reactor should poll
    /// I/O once using the returned plan, dispatch ready I/O, and then call [`turn`](Self::turn)
    /// again.
    pub fn prepare_wait(&self, waker: &Waker) -> WaitPlan {
        let next = self.wheel.next_poll_at(self.last_now);
        if next.is_some_and(|deadline| deadline <= self.last_now) {
            return WaitPlan::Immediate;
        }

        if !self.shared.operations.arm(waker) {
            return WaitPlan::Immediate;
        }

        match next {
            Some(deadline) => WaitPlan::Until(deadline),
            None => WaitPlan::Indefinite,
        }
    }

    /// Applies bounded operations and advances timers through `now`.
    ///
    /// A release build clamps a backwards `now` to the previous observation.
    ///
    /// # Panics
    ///
    /// Panics in debug builds when `now` is earlier than the previous observation. Panics in all
    /// builds when `now` is more than `u64::MAX` nanoseconds after the service's genesis.
    pub fn turn(&mut self, now: Instant, budget: TurnBudget) {
        debug_assert!(now >= self.last_now, "timer service cannot move backwards");
        let now = now.max(self.last_now);
        self.last_now = now;
        self.shared.observed.store(now);

        self.drain_operations(now, budget.max_operations.get());

        let mut entries = 0;
        // Alternate at entry granularity so neither continuously arriving immediate timers nor
        // previously registered wheel work can consume every bounded turn.
        // Once empty, non-immediate work stays empty for this turn: `now` is fixed and subsequent
        // steps can only remove immediate entries.
        let mut non_immediate_empty = false;
        while entries < budget.max_timer_entries.get() {
            let step = if self.prefer_non_immediate {
                if non_immediate_empty {
                    self.wheel.step_immediate()
                } else {
                    match self.wheel.step_non_immediate(now) {
                        Some(step) => Some(step),
                        None => {
                            non_immediate_empty = true;
                            self.wheel.step_immediate()
                        }
                    }
                }
            } else {
                self.wheel.step_immediate().or_else(|| {
                    if non_immediate_empty {
                        None
                    } else {
                        let step = self.wheel.step_non_immediate(now);
                        non_immediate_empty = step.is_none();
                        step
                    }
                })
            };
            let Some(step) = step else {
                break;
            };
            entries += 1;
            self.prefer_non_immediate = !self.prefer_non_immediate;
            self.apply_step(step, now);
        }

        if !self.wheel.has_due(now) {
            self.wheel.settle(now);
        }
    }

    fn apply_step(&mut self, step: Step, now: Instant) {
        if let Step::Fire(id) = step {
            self.fire(id, now);
        }
    }

    fn drain_operations(&mut self, now: Instant, limit: usize) {
        self.shared
            .operations
            .drain_into(limit, &mut self.operation_batch);
        while let Some(operation) = self.operation_batch.pop_front() {
            self.apply_operation(operation, now);
        }
    }

    fn apply_operation(&mut self, operation: Operation, now: Instant) {
        match operation {
            Operation::Register(operation) => self.register(operation, now),
            Operation::Cancel(state) => self.cancel(&state),
        }
    }

    fn register(&mut self, operation: RegisterOp, now: Instant) {
        let deadline = operation.deadline;
        let state = operation.into_state();
        if state.lifecycle.load(Ordering::Acquire) != STATE_SUBMITTED {
            return;
        }

        let id = self.wheel.insert(deadline, now, state);
        let registered = self
            .wheel
            .get(id)
            .expect("inserted timer must remain linked in the wheel");
        registered.slot.store(id, Ordering::Relaxed);
        let cancelled = registered
            .lifecycle
            .compare_exchange(
                STATE_SUBMITTED,
                STATE_REGISTERED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err();
        if cancelled {
            registered.slot.store(NO_SLOT, Ordering::Relaxed);
            self.wheel.remove(id);
        }
    }

    fn cancel(&mut self, state: &Arc<TimerState>) {
        let id = state.slot.load(Ordering::Relaxed);
        if id == NO_SLOT {
            return;
        }
        let Some(found) = self.wheel.get(id) else {
            return;
        };
        if !Arc::ptr_eq(found, state) {
            return;
        }
        state.slot.store(NO_SLOT, Ordering::Relaxed);
        self.wheel.remove(id);
    }

    fn fire(&mut self, id: usize, now: Instant) {
        let state = self
            .wheel
            .remove(id)
            .expect("ready timer must remain linked in the wheel");
        state
            .fired_nanos
            .store(self.shared.observed.encode(now), Ordering::Relaxed);
        let waker = state.publish_terminal(STATE_REGISTERED, STATE_FIRED);
        state.slot.store(NO_SLOT, Ordering::Relaxed);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Default for TimerService {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TimerService {
    fn drop(&mut self) {
        let (queued, reactor_waker) = self.shared.operations.close(&self.shared.closed);

        let mut wakers = Vec::new();
        for state in self.wheel.drain() {
            state.slot.store(NO_SLOT, Ordering::Relaxed);
            if let Some(waker) = state.publish_terminal(STATE_REGISTERED, STATE_CLOSED) {
                wakers.push(waker);
            }
        }
        for waker in wakers {
            waker.wake();
        }

        // Dropping queued RegisterOps closes timers still in STATE_SUBMITTED.
        drop(queued);
        // User-provided waker code may unwind. Run it only after every service-owned timer has
        // reached a durable terminal state.
        drop(reactor_waker);
    }
}

/// A lazy future that completes at or after its deadline.
///
/// Registration happens on the first poll. Dropping the backing service closes delays that still
/// require service processing, including due timers that have not yet been fired. A delay polled
/// for the first time after its deadline completes without registering, even if the service has
/// already closed. Subsequent polls repeat the same terminal result.
///
/// # Cancel safety
///
/// Dropping an incomplete delay marks it cancelled immediately. If the service has already linked
/// it into the wheel, unlinking is queued and happens during a later service turn.
///
/// See the [module level documentation](self) for timer resolution and driving requirements.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Delay {
    handle: TimerHandle,
    deadline: Deadline,
    closed: bool,
    fired_at: Option<Instant>,
    registered_waker: Option<WakerIdentity>,
    state: Option<Arc<TimerState>>,
}

impl fmt::Debug for Delay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delay")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl Delay {
    fn completion_observation(&self) -> Instant {
        self.fired_at
            .expect("completed delay must have a completion observation")
    }

    fn is_elapsed_at(&self, now: Instant) -> bool {
        self.deadline
            .as_instant()
            .is_some_and(|deadline| deadline <= now)
    }

    fn poll_lifecycle(&mut self, lifecycle: u8) -> Poll<Result<(), TimerClosed>> {
        match lifecycle {
            STATE_FIRED => {
                let state = self.state.take().expect("polled delay must have state");
                state.waker.clear();
                let nanos = state.fired_nanos.load(Ordering::Relaxed);
                self.fired_at = Some(self.handle.shared.observed.decode(nanos));
                self.registered_waker = None;
                Poll::Ready(Ok(()))
            }
            STATE_CLOSED => {
                let state = self.state.take().expect("polled delay must have state");
                state.waker.clear();
                self.closed = true;
                self.registered_waker = None;
                Poll::Ready(Err(TimerClosed))
            }
            STATE_SUBMITTED | STATE_REGISTERED => Poll::Pending,
            STATE_CANCELLED => unreachable!(
                "only Delay::drop writes STATE_CANCELLED, so a live Delay cannot observe it"
            ),
            state => panic!("invalid timer state {state}"),
        }
    }
}

impl Future for Delay {
    type Output = Result<(), TimerClosed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.fired_at.is_some() {
            return Poll::Ready(Ok(()));
        }
        if this.closed {
            return Poll::Ready(Err(TimerClosed));
        }
        if this.state.is_none() {
            let now = this.handle.now();
            if this.is_elapsed_at(now) {
                this.fired_at = Some(now);
                return Poll::Ready(Ok(()));
            }
            if this.handle.shared.closed.load(Ordering::Acquire) {
                this.closed = true;
                return Poll::Ready(Err(TimerClosed));
            }

            // The state is still private, so its first waker can be initialized inline without a
            // second allocation or synchronization.
            let registered = cx.waker().clone();
            let registered_waker = WakerIdentity::new(&registered);
            let state = Arc::new(TimerState::new(registered));
            this.registered_waker = Some(registered_waker);
            this.state = Some(state.clone());
            let operation = Operation::Register(RegisterOp {
                deadline: this.deadline,
                state: Some(state),
            });
            if let Err(operation) = this.handle.send(operation) {
                let Operation::Register(operation) = operation else {
                    unreachable!("a failed send must return the submitted operation")
                };
                let state = operation.into_state();
                state.lifecycle.store(STATE_CLOSED, Ordering::Release);
                return this.poll_lifecycle(STATE_CLOSED);
            }

            let lifecycle = this
                .state
                .as_ref()
                .expect("submitted delay must have state")
                .lifecycle
                .load(Ordering::Acquire);
            return this.poll_lifecycle(lifecycle);
        }

        let state = this.state.as_ref().expect("polled delay must have state");
        let lifecycle = if this
            .registered_waker
            .is_some_and(|registered| registered.matches(cx.waker()))
        {
            // ORDERING: The shared slot still holds an equivalent waker from an earlier poll, so
            // there is no registration update to synchronize. A non-terminal lifecycle implies
            // the slot is still armed: the service empties it only after terminal publication,
            // which happens after the terminal transition, and `poll_lifecycle` clears it only on
            // a terminal state. Skipping the update avoids cloning and replacing the same waker on
            // every repoll.
            state.lifecycle.load(Ordering::Acquire)
        } else {
            this.registered_waker = None;
            let (lifecycle, registered_waker) =
                state.waker.register_and_load(cx.waker(), &state.lifecycle);
            if let Some(registered_waker) = registered_waker {
                this.registered_waker = Some(registered_waker);
            }
            lifecycle
        };
        this.poll_lifecycle(lifecycle)
    }
}

impl Drop for Delay {
    fn drop(&mut self) {
        let Some(state) = self.state.as_ref() else {
            return;
        };
        loop {
            match state.lifecycle.load(Ordering::Acquire) {
                STATE_SUBMITTED => {
                    if state
                        .lifecycle
                        .compare_exchange(
                            STATE_SUBMITTED,
                            STATE_CANCELLED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        state.waker.clear();
                        return;
                    }
                }
                STATE_REGISTERED => {
                    if state
                        .lifecycle
                        .compare_exchange(
                            STATE_REGISTERED,
                            STATE_CANCELLED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        let _ = self.handle.send(Operation::Cancel(state.clone()));
                        // Queue reclamation before running the user-provided waker destructor so a
                        // panic cannot leave the cancelled entry linked indefinitely.
                        state.waker.clear();
                        return;
                    }
                }
                STATE_FIRED | STATE_CANCELLED | STATE_CLOSED => return,
                state => panic!("invalid timer state {state}"),
            }
        }
    }
}

/// Why a timed operation did not produce a value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutError {
    /// The timeout timer fired before the guarded future completed.
    Elapsed,
    /// The backing timer service closed before the timeout timer fired.
    Closed,
}

impl fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Elapsed => f.write_str("operation timed out"),
            Self::Closed => f.write_str("timer service is closed"),
        }
    }
}

impl std::error::Error for TimeoutError {}

async fn timeout_with_delay<F>(delay: Delay, future: F) -> Result<F::Output, TimeoutError>
where
    F: Future,
{
    let mut future = std::pin::pin!(future);
    let mut delay = std::pin::pin!(delay);
    poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        match delay.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Err(TimeoutError::Elapsed)),
            Poll::Ready(Err(TimerClosed)) => Poll::Ready(Err(TimeoutError::Closed)),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

/// How an [`Interval`] responds when one or more ticks were missed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MissedTickBehavior {
    /// Preserve every scheduled deadline and catch up with immediately ready ticks.
    #[default]
    Burst,
    /// Schedule the next tick one period after the completed tick was observed.
    Delay,
    /// Stay on the original grid while discarding deadlines at or before the observation.
    Skip,
}

/// A periodic timer built from sequential [`Delay`] futures.
///
/// See the [module level documentation](self) for timer resolution and driving requirements.
#[derive(Debug)]
#[must_use = "an interval advances only when tick is awaited"]
pub struct Interval {
    behavior: MissedTickBehavior,
    delay: Delay,
    period: Duration,
}

impl Interval {
    /// Waits for and consumes the next tick, returning its scheduled deadline.
    ///
    /// # Cancel safety
    ///
    /// Cancelling this future before completion does not consume the tick.
    pub async fn tick(&mut self) -> Result<Instant, TimerClosed> {
        (&mut self.delay).await?;
        let observation = self.delay.completion_observation();
        let scheduled = self
            .delay
            .deadline
            .as_instant()
            .expect("a never deadline cannot complete successfully");
        let next_deadline = self.next_deadline(scheduled, observation);
        self.delay = self.delay.handle.delay_to(next_deadline);
        Ok(scheduled)
    }

    /// Returns the current missed-tick behavior.
    pub const fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.behavior
    }

    /// Changes the missed-tick behavior used after the next completed tick.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }

    fn next_deadline(&self, scheduled: Instant, observation: Instant) -> Deadline {
        match self.behavior {
            MissedTickBehavior::Burst => Deadline::checked_add(scheduled, self.period),
            MissedTickBehavior::Delay => Deadline::checked_add(observation, self.period),
            MissedTickBehavior::Skip => skip_deadline(scheduled, self.period, observation),
        }
    }
}

fn interval_from_deadline(handle: &TimerHandle, deadline: Deadline, period: Duration) -> Interval {
    assert!(!period.is_zero(), "interval period must be non-zero");
    Interval {
        behavior: MissedTickBehavior::Burst,
        delay: handle.delay_to(deadline),
        period,
    }
}

fn skip_deadline(scheduled: Instant, period: Duration, observation: Instant) -> Deadline {
    let Some(elapsed) = observation.checked_duration_since(scheduled) else {
        return Deadline::checked_add(scheduled, period);
    };
    let elapsed_nanos = elapsed.as_nanos();
    let period_nanos = period.as_nanos();
    let periods = elapsed_nanos / period_nanos + 1;
    let Some(total_nanos) = period_nanos.checked_mul(periods) else {
        return Deadline::Never;
    };
    let seconds = total_nanos / 1_000_000_000;
    let Ok(seconds) = u64::try_from(seconds) else {
        return Deadline::Never;
    };
    let nanos = (total_nanos % 1_000_000_000) as u32;
    Deadline::checked_add(scheduled, Duration::new(seconds, nanos))
}

async fn schedule_with_fixed_delay<F>(
    timer: TimerHandle,
    initial_delay: Option<Duration>,
    delay: Duration,
    mut task: F,
) -> Result<(), TimerClosed>
where
    F: AsyncFnMut(),
{
    assert!(!delay.is_zero(), "fixed delay must be non-zero");
    if let Some(initial_delay) = initial_delay {
        timer.delay(initial_delay).await?;
    }
    loop {
        task().await;
        timer.delay(delay).await?;
    }
}

async fn schedule_at_fixed_rate<F>(
    timer: TimerHandle,
    initial_delay: Option<Duration>,
    period: Duration,
    mut task: F,
) -> Result<(), TimerClosed>
where
    F: AsyncFnMut(),
{
    assert!(!period.is_zero(), "fixed rate must be non-zero");
    let now = timer.now();
    let mut scheduled = match initial_delay {
        Some(delay) => Deadline::checked_add(now, delay),
        None => Deadline::At(now),
    };
    loop {
        timer.delay_to(scheduled).await?;
        let completed = scheduled
            .as_instant()
            .expect("a never deadline cannot complete successfully");
        task().await;
        // The observation is read *after* the task returns. Deriving the next grid point from
        // the tick's own completion instead would let an overrunning task collapse the schedule
        // into back-to-back invocations, because every subsequent deadline would already be in
        // the past by the time it was awaited.
        scheduled = skip_deadline(completed, period, timer.now());
    }
}

async fn schedule_with_arbitrary_delay<F>(
    timer: TimerHandle,
    initial_delay: Option<Duration>,
    mut task: F,
) -> Result<(), TimerClosed>
where
    F: AsyncFnMut() -> Instant,
{
    if let Some(initial_delay) = initial_delay {
        timer.delay(initial_delay).await?;
    }
    loop {
        let next = task().await;
        timer.delay_until(next).await?;
    }
}
