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

use std::future::Future;
use std::future::pending;
use std::future::ready;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use super::*;

fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    let waker = Waker::noop();
    poll_with_waker(future, waker)
}

fn poll_with_waker<T>(future: Pin<&mut impl Future<Output = T>>, waker: &Waker) -> Poll<T> {
    let mut cx = Context::from_waker(waker);
    future.poll(&mut cx)
}

fn budget() -> TurnBudget {
    TurnBudget::new(
        NonZeroUsize::new(64).unwrap(),
        NonZeroUsize::new(64).unwrap(),
    )
}

fn tiny_budget() -> TurnBudget {
    TurnBudget::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(1).unwrap())
}

fn drive(service: &mut TimerService, now: Instant) {
    service.turn(now, budget());
}

fn drive_with_tiny_budget(service: &mut TimerService, now: Instant) {
    service.turn(now, tiny_budget());
}

fn wait_plan(service: &TimerService) -> WaitPlan {
    service.prepare_wait(Waker::noop())
}

fn assert_schedule_waits_for_initial_delay(
    service: &mut TimerService,
    genesis: Instant,
    mut schedule: Pin<&mut impl Future<Output = Result<(), TimerClosed>>>,
    count: &AtomicUsize,
) {
    assert!(poll(schedule.as_mut()).is_pending());
    assert_eq!(count.load(Ordering::Relaxed), 0);
    drive(service, genesis);
    drive(service, genesis + Duration::from_millis(4));
    assert!(poll(schedule.as_mut()).is_pending());
    assert_eq!(count.load(Ordering::Relaxed), 0);
    drive(service, genesis + Duration::from_millis(5));
    assert!(poll(schedule.as_mut()).is_pending());
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[allow(
    unsafe_code,
    reason = "the test needs a RawWaker whose drop callback can unwind"
)]
mod panic_drop_waker {
    use std::sync::Mutex;
    use std::sync::MutexGuard;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Waker;

    struct State {
        panic_on_drop: AtomicBool,
        wakes: AtomicUsize,
    }

    static STATE: State = State {
        panic_on_drop: AtomicBool::new(false),
        wakes: AtomicUsize::new(0),
    };
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    pub(super) fn serial() -> MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap()
    }

    pub(super) fn new() -> Waker {
        STATE.panic_on_drop.store(true, Ordering::Relaxed);
        STATE.wakes.store(0, Ordering::Relaxed);
        let raw = RawWaker::new((&STATE as *const State).cast(), &VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    pub(super) fn wakes() -> usize {
        STATE.wakes.load(Ordering::Relaxed)
    }

    unsafe fn clone(data: *const ()) -> RawWaker {
        RawWaker::new(data, &VTABLE)
    }

    unsafe fn wake(data: *const ()) {
        unsafe { &*data.cast::<State>() }
            .wakes
            .fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn wake_by_ref(data: *const ()) {
        unsafe { wake(data) };
    }

    unsafe fn drop(data: *const ()) {
        let state = unsafe { &*data.cast::<State>() };
        if state.panic_on_drop.swap(false, Ordering::Relaxed) && !std::thread::panicking() {
            panic!("intentional RawWaker drop panic");
        }
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
}

#[test]
fn default_turn_budget_matches_documented_limits() {
    let budget = TurnBudget::default();
    assert_eq!(budget.max_operations().get(), 1_024);
    assert_eq!(budget.max_timer_entries().get(), 4_096);
}

#[test]
fn atomic_observation_preserves_nanoseconds() {
    let genesis = Instant::now();
    let observation = AtomicObservation::new(genesis);
    let published = genesis + Duration::from_nanos(500_123);

    observation.store(published);
    assert_eq!(observation.load(), published);
}

#[test]
fn observation_encoding_accepts_the_full_u64_range() {
    assert_eq!(
        encode_observation_nanos(Duration::from_nanos(u64::MAX - 1)),
        u64::MAX - 1
    );
    assert_eq!(
        encode_observation_nanos(Duration::from_nanos(u64::MAX)),
        u64::MAX
    );
}

#[test]
#[should_panic(expected = "timer observation exceeds the u64 nanosecond range")]
fn atomic_observation_rejects_offsets_larger_than_u64_nanos() {
    let first_unrepresentable = Duration::from_nanos(u64::MAX)
        .checked_add(Duration::from_nanos(1))
        .unwrap();
    let _ = encode_observation_nanos(first_unrepresentable);
}

#[test]
fn concurrent_observation_reads_never_move_backwards() {
    const READERS: usize = 4;
    const UPDATES: u64 = 1_000;

    let genesis = Instant::now();
    let final_observation = genesis + Duration::from_micros(UPDATES);
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let ready = Arc::new(Barrier::new(READERS + 1));

    std::thread::scope(|scope| {
        for _ in 0..READERS {
            let timer = timer.clone();
            let ready = ready.clone();
            scope.spawn(move || {
                let mut previous = timer.now();
                assert_eq!(previous, genesis);
                let mut spins = 0usize;
                ready.wait();
                while previous != final_observation {
                    let observed = timer.now();
                    assert!(observed >= previous);
                    assert!(observed <= final_observation);
                    previous = observed;
                    // More readers than cores must not starve the single service thread.
                    if spins % 64 == 0 {
                        std::thread::yield_now();
                    } else {
                        std::hint::spin_loop();
                    }
                    spins += 1;
                }
            });
        }

        ready.wait();
        for micros in 1..=UPDATES {
            drive(&mut service, genesis + Duration::from_micros(micros));
        }
    });
}

#[test]
fn system_clock_handle_uses_wall_clock_and_published_observation() {
    let mut service = TimerService::new();
    let timer = service.handle();
    let duration = Duration::from_millis(10);

    let before = Instant::now();
    let wall_clock_delay = timer.delay(duration);
    assert!(
        wall_clock_delay.deadline.as_instant().unwrap() >= before.checked_add(duration).unwrap()
    );

    let published = Instant::now()
        .checked_add(Duration::from_secs(86_400))
        .unwrap();
    drive(&mut service, published);
    let published_delay = timer.delay(duration);
    assert_eq!(
        published_delay.deadline,
        Deadline::At(published.checked_add(duration).unwrap())
    );
}

#[test]
fn prepare_wait_reports_pending_operations_and_the_earliest_deadline() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let later_deadline = genesis + Duration::from_millis(20);
    let earlier_deadline = genesis + Duration::from_millis(10);

    let mut later = Box::pin(timer.delay_until(later_deadline));
    assert!(poll(later.as_mut()).is_pending());
    assert_eq!(wait_plan(&service), WaitPlan::Immediate);
    drive(&mut service, genesis);
    assert_eq!(wait_plan(&service), WaitPlan::Until(later_deadline));

    let mut earlier = Box::pin(timer.delay_until(earlier_deadline));
    assert!(poll(earlier.as_mut()).is_pending());
    assert_eq!(wait_plan(&service), WaitPlan::Immediate);
    drive(&mut service, genesis);
    assert_eq!(wait_plan(&service), WaitPlan::Until(earlier_deadline));
}

#[test]
fn service_promotes_overflow_timer_and_fires_at_deadline() {
    let genesis = Instant::now();
    let promotion = genesis + Duration::from_millis(1);
    let deadline = genesis + Duration::from_millis(wheel::HORIZON);
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut delay = Box::pin(timer.delay_until(deadline));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis);
    assert_eq!(wait_plan(&service), WaitPlan::Until(promotion));

    drive(&mut service, promotion);
    assert!(poll(delay.as_mut()).is_pending());
    assert_eq!(wait_plan(&service), WaitPlan::Until(deadline));

    drive(&mut service, deadline - Duration::from_millis(1));
    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, deadline);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "timer service cannot move backwards")]
fn turn_rejects_a_backwards_clock_in_debug_builds() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let _timer = service.handle();
    drive(&mut service, genesis + Duration::from_millis(1));
    drive(&mut service, genesis);
}

#[test]
fn dropping_service_releases_its_registered_reactor_waker() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());

    assert_eq!(service.prepare_wait(&waker), WaitPlan::Indefinite);
    drop(waker);
    assert_eq!(Arc::strong_count(&counter), 2);

    drop(service);
    assert_eq!(Arc::strong_count(&counter), 1);
    drop(timer);
}

#[test]
fn completed_delay_releases_its_registered_waker() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll_with_waker(delay.as_mut(), &waker).is_pending());
    drive(&mut service, genesis);
    drive(&mut service, genesis + Duration::from_millis(1));
    assert_eq!(poll_with_waker(delay.as_mut(), &waker), Poll::Ready(Ok(())));
    assert!(delay.as_ref().get_ref().state.is_none());

    drop(waker);
    assert_eq!(Arc::strong_count(&counter), 1);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn cancelled_delays_release_wakers_before_the_service_drains() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();

    let submitted_counter = Arc::new(WakeCount::default());
    let submitted_waker = Waker::from(submitted_counter.clone());
    let mut submitted = Box::pin(timer.delay(Duration::from_secs(1)));
    assert!(poll_with_waker(submitted.as_mut(), &submitted_waker).is_pending());
    drop(submitted_waker);
    drop(submitted);
    assert_eq!(Arc::strong_count(&submitted_counter), 1);

    drive(&mut service, genesis);

    let registered_counter = Arc::new(WakeCount::default());
    let registered_waker = Waker::from(registered_counter.clone());
    let mut registered = Box::pin(timer.delay(Duration::from_secs(1)));
    assert!(poll_with_waker(registered.as_mut(), &registered_waker).is_pending());
    drive(&mut service, genesis);
    drop(registered_waker);
    drop(registered);
    assert_eq!(Arc::strong_count(&registered_counter), 1);

    drive(&mut service, genesis);
    assert_eq!(service.wheel.len(), 0);
}

#[test]
fn delay_is_lazy_and_uses_driven_time() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut delay = std::pin::pin!(timer.delay(Duration::from_millis(10)));

    assert_eq!(service.wheel.len(), 0);
    assert!(poll(delay.as_mut()).is_pending());
    assert_eq!(service.wheel.len(), 0);

    service.turn(genesis, budget());
    assert_eq!(
        wait_plan(&service),
        WaitPlan::Until(genesis + Duration::from_millis(10))
    );
    assert_eq!(service.wheel.len(), 1);
    assert!(poll(delay.as_mut()).is_pending());

    drive(&mut service, genesis + Duration::from_millis(9));
    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis + Duration::from_millis(10));
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn dropping_service_closes_registered_delay() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut delay = std::pin::pin!(timer.delay(Duration::from_secs(1)));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis);
    drop(service);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
    assert!(delay.as_ref().get_ref().state.is_none());
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn service_drop_closes_due_timer_that_budget_has_not_fired() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let deadline = genesis + Duration::from_millis(1);
    let mut fired = Box::pin(timer.delay_until(deadline));
    let mut still_due = Box::pin(timer.delay_until(deadline));

    assert!(poll(fired.as_mut()).is_pending());
    assert!(poll(still_due.as_mut()).is_pending());
    service.turn(deadline, tiny_budget());
    assert_eq!(wait_plan(&service), WaitPlan::Immediate);
    assert_eq!(poll(fired.as_mut()), Poll::Ready(Ok(())));
    assert!(poll(still_due.as_mut()).is_pending());

    drop(service);
    assert_eq!(poll(still_due.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn dropping_service_closes_queued_delay() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut delay = std::pin::pin!(timer.delay(Duration::from_secs(1)));

    assert!(poll(delay.as_mut()).is_pending());
    drop(service);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
    assert!(delay.as_ref().get_ref().state.is_none());
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn failed_registration_send_returns_closed_without_self_wake() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    service.shared.operations.disconnect();
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_secs(1)));

    assert_eq!(
        poll_with_waker(delay.as_mut(), &waker),
        Poll::Ready(Err(TimerClosed))
    );
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    assert!(delay.as_ref().get_ref().state.is_none());
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn fresh_delay_after_close_is_synchronously_closed_and_fused() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    drop(service);
    let mut delay = std::pin::pin!(timer.delay_until(genesis + Duration::from_secs(1)));

    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn elapsed_deadline_wins_over_closed_service() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    drop(service);
    let mut delay = std::pin::pin!(timer.delay_until(genesis));

    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn cancellation_before_and_after_registration_reclaims_entries() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();

    let mut submitted = Box::pin(timer.delay(Duration::from_secs(1)));
    assert!(poll(submitted.as_mut()).is_pending());
    drop(submitted);
    drive(&mut service, genesis);
    assert_eq!(service.wheel.len(), 0);

    let mut registered = Box::pin(timer.delay(Duration::from_secs(1)));
    assert!(poll(registered.as_mut()).is_pending());
    drive(&mut service, genesis);
    assert_eq!(service.wheel.len(), 1);
    drop(registered);
    drive(&mut service, genesis);
    assert_eq!(service.wheel.len(), 0);
}

#[test]
fn operation_and_entry_budgets_bound_each_turn() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut delays: Vec<_> = (0..5)
        .map(|_| Box::pin(timer.delay_until(genesis + Duration::from_millis(1))))
        .collect();
    for delay in &mut delays {
        assert!(poll(delay.as_mut()).is_pending());
    }

    service.turn(genesis, tiny_budget());
    assert_eq!(service.wheel.len(), 2);
    assert_eq!(wait_plan(&service), WaitPlan::Immediate);
    service.turn(genesis, tiny_budget());
    assert_eq!(service.wheel.len(), 4);
    assert_eq!(wait_plan(&service), WaitPlan::Immediate);
    drive_with_tiny_budget(&mut service, genesis);
    assert_eq!(service.wheel.len(), 5);
    assert_eq!(
        wait_plan(&service),
        WaitPlan::Until(genesis + Duration::from_millis(1))
    );

    let due = genesis + Duration::from_millis(1);
    for expected in 1..=5 {
        service.turn(due, tiny_budget());
        let mut now_completed = 0;
        for delay in &mut delays {
            if poll(delay.as_mut()).is_ready() {
                now_completed += 1;
            }
        }
        assert_eq!(now_completed, expected);
        assert_eq!(
            wait_plan(&service),
            if expected == 5 {
                WaitPlan::Indefinite
            } else {
                WaitPlan::Immediate
            }
        );
    }
}

#[test]
fn immediate_and_wheel_timers_both_progress_under_continuous_registrations() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut existing = Box::pin(timer.delay_until(genesis + Duration::from_millis(1)));
    assert!(poll(existing.as_mut()).is_pending());
    drive(&mut service, genesis);

    let mut immediate = Vec::new();
    for millis in 1..=2 {
        let now = genesis + Duration::from_millis(millis);
        for _ in 0..2 {
            let mut delay = Box::pin(timer.delay_until(now));
            assert!(poll(delay.as_mut()).is_pending());
            immediate.push(delay);
        }
        service.turn(now, tiny_budget());
    }

    assert_eq!(poll(existing.as_mut()), Poll::Ready(Ok(())));
    assert!(
        immediate
            .iter_mut()
            .any(|delay| poll(delay.as_mut()).is_ready())
    );
}

#[test]
fn immediate_and_wheel_timers_share_the_entry_budget() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let due = genesis + Duration::from_millis(1);
    let mut wheel: Vec<_> = (0..8).map(|_| Box::pin(timer.delay_until(due))).collect();
    for delay in &mut wheel {
        assert!(poll(delay.as_mut()).is_pending());
    }
    drive(&mut service, genesis);

    let mut immediate: Vec<_> = (0..8).map(|_| Box::pin(timer.delay_until(due))).collect();
    for delay in &mut immediate {
        assert!(poll(delay.as_mut()).is_pending());
    }

    let budget = TurnBudget::new(
        NonZeroUsize::new(16).unwrap(),
        NonZeroUsize::new(6).unwrap(),
    );
    service.turn(due, budget);
    assert_eq!(wait_plan(&service), WaitPlan::Immediate);
    assert_eq!(
        wheel
            .iter_mut()
            .map(|delay| poll(delay.as_mut()).is_ready())
            .filter(|ready| *ready)
            .count(),
        3
    );
    assert_eq!(
        immediate
            .iter_mut()
            .map(|delay| poll(delay.as_mut()).is_ready())
            .filter(|ready| *ready)
            .count(),
        3
    );
}

#[test]
fn prepare_wait_wake_is_coalesced_replaceable_and_renewable() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    drive(&mut service, genesis);
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    assert_eq!(service.prepare_wait(&waker), WaitPlan::Indefinite);

    let mut first = Box::pin(timer.delay(Duration::from_secs(1)));
    let mut second = Box::pin(timer.delay(Duration::from_secs(2)));
    assert!(poll(first.as_mut()).is_pending());
    assert!(poll(second.as_mut()).is_pending());
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(service.prepare_wait(&waker), WaitPlan::Immediate);

    drive(&mut service, genesis);
    let replacement_counter = Arc::new(WakeCount::default());
    let replacement_waker = Waker::from(replacement_counter.clone());
    assert!(matches!(service.prepare_wait(&waker), WaitPlan::Until(_)));
    assert!(matches!(
        service.prepare_wait(&replacement_waker),
        WaitPlan::Until(_)
    ));
    drop(first);
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(replacement_counter.0.load(Ordering::Relaxed), 1);
    drive(&mut service, genesis);
    assert!(matches!(
        service.prepare_wait(&replacement_waker),
        WaitPlan::Until(_)
    ));
}

#[test]
fn submillisecond_deadline_never_fires_early() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let deadline = genesis + Duration::from_micros(500);
    let mut delay = Box::pin(timer.delay_until(deadline));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis);
    drive(&mut service, genesis + Duration::from_micros(999));
    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis + Duration::from_millis(1));
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn driven_relative_delay_keeps_submillisecond_observation() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    drive(&mut service, genesis + Duration::from_micros(500));
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis + Duration::from_millis(1));
    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis + Duration::from_millis(2));
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn timeout_prefers_guarded_future_on_tie() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    assert_eq!(
        pollster::block_on(timer.timeout_at(genesis, ready(7))),
        Ok(7)
    );
}

#[test]
fn timeout_distinguishes_elapsed_and_closed() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut elapsed =
        Box::pin(timer.timeout_at(genesis + Duration::from_millis(1), pending::<()>()));
    assert!(poll(elapsed.as_mut()).is_pending());
    drive(&mut service, genesis + Duration::from_millis(1));
    assert_eq!(
        poll(elapsed.as_mut()),
        Poll::Ready(Err(TimeoutError::Elapsed))
    );
    drop(elapsed);

    let mut closed = Box::pin(timer.timeout(Duration::from_secs(1), pending::<()>()));
    assert!(poll(closed.as_mut()).is_pending());
    drop(service);
    assert_eq!(
        poll(closed.as_mut()),
        Poll::Ready(Err(TimeoutError::Closed))
    );
}

#[test]
fn high_level_futures_own_their_timer_handle() {
    fn assert_send_static<T: Send + 'static>(_: T) {}

    let service = TimerService::new();
    let timer = service.handle();
    assert_send_static(timer.timeout(Duration::from_secs(1), ready(())));
    assert_send_static(timer.schedule_with_fixed_delay(None, Duration::from_secs(1), async || {}));
    assert_send_static(timer.schedule_at_fixed_rate(None, Duration::from_secs(1), async || {}));
    assert_send_static(timer.schedule_with_arbitrary_delay(None, async || Instant::now()));
}

#[test]
fn never_deadline_only_completes_on_close() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut delay = Box::pin(timer.delay(Duration::MAX));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis + Duration::from_secs(10));
    assert!(poll(delay.as_mut()).is_pending());
    assert_eq!(wait_plan(&service), WaitPlan::Indefinite);
    drop(service);
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn interval_missed_tick_behaviors_have_distinct_schedules() {
    let genesis = Instant::now();
    for (behavior, expected) in [
        (MissedTickBehavior::Burst, 20),
        (MissedTickBehavior::Delay, 45),
        (MissedTickBehavior::Skip, 40),
    ] {
        let mut service = TimerService::new_at(genesis);
        let timer = service.handle();
        let mut interval = timer.interval_at(
            genesis + Duration::from_millis(10),
            Duration::from_millis(10),
        );
        interval.set_missed_tick_behavior(behavior);
        assert_eq!(interval.missed_tick_behavior(), behavior);
        let mut tick = Box::pin(interval.tick());
        assert!(poll(tick.as_mut()).is_pending());
        drive(&mut service, genesis + Duration::from_millis(35));
        assert_eq!(
            poll(tick.as_mut()),
            Poll::Ready(Ok(genesis + Duration::from_millis(10)))
        );
        drop(tick);
        let expected = genesis + Duration::from_millis(expected);
        let mut next_tick = Box::pin(interval.tick());
        if expected <= genesis + Duration::from_millis(35) {
            assert_eq!(poll(next_tick.as_mut()), Poll::Ready(Ok(expected)));
        } else {
            assert!(poll(next_tick.as_mut()).is_pending());
            drive(&mut service, expected);
            assert_eq!(poll(next_tick.as_mut()), Poll::Ready(Ok(expected)));
        }
    }
}

#[test]
fn interval_has_an_immediate_first_tick() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut interval = timer.interval(Duration::from_millis(10));

    assert_eq!(interval.missed_tick_behavior(), MissedTickBehavior::Burst);
    assert_eq!(pollster::block_on(interval.tick()), Ok(genesis));
}

#[test]
#[should_panic(expected = "interval period must be non-zero")]
fn interval_rejects_a_zero_period() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    let _ = timer.interval(Duration::ZERO);
}

#[test]
#[should_panic(expected = "interval period must be non-zero")]
fn interval_at_rejects_a_zero_period() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    let _ = timer.interval_at(genesis, Duration::ZERO);
}

#[test]
fn skip_does_not_discard_a_future_grid_point_within_the_same_tick() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut interval = timer.interval_at(
        genesis + Duration::from_millis(10),
        Duration::from_millis(10),
    );
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut tick = Box::pin(interval.tick());
    assert!(poll(tick.as_mut()).is_pending());

    drive(&mut service, genesis + Duration::from_micros(39_900));
    assert_eq!(
        poll(tick.as_mut()),
        Poll::Ready(Ok(genesis + Duration::from_millis(10)))
    );
    drop(tick);
    let mut next_tick = Box::pin(interval.tick());
    assert!(poll(next_tick.as_mut()).is_pending());
    drive(&mut service, genesis + Duration::from_millis(40));
    assert_eq!(
        poll(next_tick.as_mut()),
        Poll::Ready(Ok(genesis + Duration::from_millis(40)))
    );
}

#[test]
fn interval_tick_is_cancel_safe() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut interval = timer.interval_at(
        genesis + Duration::from_millis(10),
        Duration::from_millis(10),
    );

    let mut first_attempt = Box::pin(interval.tick());
    assert!(poll(first_attempt.as_mut()).is_pending());
    drop(first_attempt);
    drive(&mut service, genesis + Duration::from_millis(10));
    assert_eq!(
        pollster::block_on(interval.tick()),
        Ok(genesis + Duration::from_millis(10))
    );
}

#[test]
fn scheduling_futures_do_not_spawn_and_propagate_close() {
    let genesis = Instant::now();

    let service = TimerService::new_at(genesis);

    let timer = service.handle();
    let fixed_delay_count = Arc::new(AtomicUsize::new(0));
    let count = fixed_delay_count.clone();
    let mut fixed_delay = Box::pin(timer.schedule_with_fixed_delay(
        None,
        Duration::from_millis(1),
        async move || {
            count.fetch_add(1, Ordering::Relaxed);
        },
    ));
    assert!(poll(fixed_delay.as_mut()).is_pending());
    assert_eq!(fixed_delay_count.load(Ordering::Relaxed), 1);
    drop(service);
    assert_eq!(poll(fixed_delay.as_mut()), Poll::Ready(Err(TimerClosed)));

    let service = TimerService::new_at(genesis);

    let timer = service.handle();
    let fixed_rate_count = Arc::new(AtomicUsize::new(0));
    let count = fixed_rate_count.clone();
    let mut fixed_rate =
        Box::pin(
            timer.schedule_at_fixed_rate(None, Duration::from_millis(1), async move || {
                count.fetch_add(1, Ordering::Relaxed);
            }),
        );
    assert!(poll(fixed_rate.as_mut()).is_pending());
    assert_eq!(fixed_rate_count.load(Ordering::Relaxed), 1);
    drop(service);
    assert_eq!(poll(fixed_rate.as_mut()), Poll::Ready(Err(TimerClosed)));

    let service = TimerService::new_at(genesis);

    let timer = service.handle();
    let arbitrary_count = Arc::new(AtomicUsize::new(0));
    let count = arbitrary_count.clone();
    let mut arbitrary = Box::pin(timer.schedule_with_arbitrary_delay(None, async move || {
        count.fetch_add(1, Ordering::Relaxed);
        genesis + Duration::from_millis(1)
    }));
    assert!(poll(arbitrary.as_mut()).is_pending());
    assert_eq!(arbitrary_count.load(Ordering::Relaxed), 1);
    drop(service);
    assert_eq!(poll(arbitrary.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn scheduling_futures_honor_their_initial_delay() {
    let genesis = Instant::now();
    let initial_delay = Some(Duration::from_millis(5));

    let mut service = TimerService::new_at(genesis);

    let timer = service.handle();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let mut schedule = Box::pin(timer.schedule_with_fixed_delay(
        initial_delay,
        Duration::from_millis(1),
        async move || {
            observed.fetch_add(1, Ordering::Relaxed);
        },
    ));
    assert_schedule_waits_for_initial_delay(&mut service, genesis, schedule.as_mut(), &count);

    let mut service = TimerService::new_at(genesis);

    let timer = service.handle();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let mut schedule = Box::pin(timer.schedule_at_fixed_rate(
        initial_delay,
        Duration::from_millis(1),
        async move || {
            observed.fetch_add(1, Ordering::Relaxed);
        },
    ));
    assert_schedule_waits_for_initial_delay(&mut service, genesis, schedule.as_mut(), &count);

    let mut service = TimerService::new_at(genesis);

    let timer = service.handle();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let mut schedule = Box::pin(timer.schedule_with_arbitrary_delay(
        initial_delay,
        async move || {
            observed.fetch_add(1, Ordering::Relaxed);
            genesis + Duration::from_millis(6)
        },
    ));
    assert_schedule_waits_for_initial_delay(&mut service, genesis, schedule.as_mut(), &count);
}

#[test]
#[should_panic(expected = "fixed delay must be non-zero")]
fn fixed_delay_schedule_rejects_a_zero_delay() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    let _ = pollster::block_on(timer.schedule_with_fixed_delay(None, Duration::ZERO, async || {}));
}

#[test]
#[should_panic(expected = "fixed rate must be non-zero")]
fn fixed_rate_schedule_rejects_a_zero_period() {
    let genesis = Instant::now();
    let service = TimerService::new_at(genesis);
    let timer = service.handle();
    let _ = pollster::block_on(timer.schedule_at_fixed_rate(None, Duration::ZERO, async || {}));
}

#[test]
fn concurrent_prepare_wait_and_first_operation_never_both_miss() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();

    for _ in 0..1_000 {
        drive(&mut service, genesis);
        let barrier = Arc::new(Barrier::new(2));
        let sender_barrier = barrier.clone();
        let sender_timer = timer.clone();
        let sender = std::thread::spawn(move || {
            let mut delay = Box::pin(sender_timer.delay(Duration::from_secs(1)));
            sender_barrier.wait();
            assert!(poll(delay.as_mut()).is_pending());
            delay
        });

        let counter = Arc::new(WakeCount::default());
        let waker = Waker::from(counter.clone());
        barrier.wait();
        let plan = service.prepare_wait(&waker);
        let delay = sender.join().unwrap();
        assert!(
            plan == WaitPlan::Immediate || counter.0.load(Ordering::Relaxed) > 0,
            "service prepared to wait but the first producer missed its wake slot"
        );

        drive(&mut service, genesis);
        drop(delay);
        drive(&mut service, genesis);
        assert_eq!(service.wheel.len(), 0);
    }
}

#[test]
fn concurrent_delay_registration_and_fire_never_lose_wake() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut now = genesis;

    for _ in 0..1_000 {
        let deadline = now + Duration::from_millis(1);
        let mut delay = Box::pin(timer.delay_until(deadline));
        assert!(poll(delay.as_mut()).is_pending());
        drive(&mut service, now);

        let barrier = Arc::new(Barrier::new(2));
        let poll_barrier = barrier.clone();
        let counter = Arc::new(WakeCount::default());
        let poll_counter = counter.clone();
        let polling = std::thread::spawn(move || {
            let waker = Waker::from(poll_counter.clone());
            poll_barrier.wait();
            let result = poll_with_waker(delay.as_mut(), &waker);
            (delay, result, poll_counter)
        });

        barrier.wait();
        drive(&mut service, deadline);
        let (mut delay, result, counter) = polling.join().unwrap();
        if result.is_pending() {
            assert!(
                counter.0.load(Ordering::Relaxed) > 0,
                "Delay returned Pending while the terminal publisher missed its waker"
            );
        }
        assert_eq!(poll(delay.as_mut()), Poll::Ready(Ok(())));
        now = deadline;
    }
}

#[test]
fn terminal_publication_during_waker_registration_delegates_cleanup_to_poller() {
    let old_counter = Arc::new(WakeCount::default());
    let old_waker = Waker::from(old_counter.clone());
    let state = Arc::new(TimerState::new(old_waker));
    state.lifecycle.store(STATE_REGISTERED, Ordering::Relaxed);
    let new_counter = Arc::new(WakeCount::default());
    let new_waker = Waker::from(new_counter.clone());
    let claimed = Arc::new(Barrier::new(2));
    let published = Arc::new(Barrier::new(2));

    let poll_state = state.clone();
    let poll_claimed = claimed.clone();
    let poll_published = published.clone();
    let polling = std::thread::spawn(move || {
        poll_state
            .waker
            .register_and_load_with(&new_waker, &poll_state.lifecycle, || {
                poll_claimed.wait();
                poll_published.wait();
            })
    });

    claimed.wait();
    assert_eq!(state.waker.state.load(Ordering::Acquire), WAKER_REGISTERING);
    assert!(
        state
            .publish_terminal(STATE_REGISTERED, STATE_FIRED)
            .is_none()
    );
    published.wait();

    let (observed, identity) = polling.join().unwrap();
    assert_eq!(observed, STATE_FIRED);
    assert!(identity.is_some());
    assert_eq!(state.waker.state.load(Ordering::Acquire), WAKER_TERMINAL);
    assert_eq!(Arc::strong_count(&old_counter), 1);
    assert_eq!(Arc::strong_count(&new_counter), 1);
}

#[test]
fn concurrent_cancel_and_fire_reclaim_exactly_once() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut now = genesis;

    for _ in 0..1_000 {
        let deadline = now + Duration::from_millis(1);
        let mut delay = Box::pin(timer.delay_until(deadline));
        assert!(poll(delay.as_mut()).is_pending());
        drive(&mut service, now);

        let barrier = Arc::new(Barrier::new(2));
        let drop_barrier = barrier.clone();
        let dropping = std::thread::spawn(move || {
            drop_barrier.wait();
            drop(delay);
        });
        barrier.wait();
        drive(&mut service, deadline);
        dropping.join().unwrap();
        drive(&mut service, deadline);
        assert_eq!(service.wheel.len(), 0);
        now = deadline;
    }
}

#[test]
fn fixed_rate_task_longer_than_period_stays_on_the_grid() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let starts = Arc::new(Mutex::new(Vec::new()));
    let observed = starts.clone();
    let clock = timer.clone();

    // Each invocation occupies 35ms of the timeline, so it misses three 10ms grid points. The
    // task has to consume that time itself: advancing the clock only between polls would let the
    // scheduler observe the pre-task instant and would not model an overrun at all.
    let mut schedule =
        Box::pin(
            timer.schedule_at_fixed_rate(None, Duration::from_millis(10), async move || {
                observed.lock().unwrap().push(clock.now() - genesis);
                let _ = clock.delay(Duration::from_millis(35)).await;
            }),
        );

    assert!(poll(schedule.as_mut()).is_pending());
    for millis in [35, 40, 75, 80] {
        drive(&mut service, genesis + Duration::from_millis(millis));
        assert!(poll(schedule.as_mut()).is_pending());
    }

    let starts = starts.lock().unwrap().clone();
    assert_eq!(
        starts,
        vec![
            Duration::ZERO,
            Duration::from_millis(40),
            Duration::from_millis(80),
        ],
        "an overrunning task must skip missed grid points, not run back to back"
    );
}

#[test]
fn concurrent_drain_and_send_never_park_with_queued_operations() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();

    // Exercises operation submission against wake registration on the real queue.
    for _ in 0..1_000 {
        let barrier = Arc::new(Barrier::new(2));
        let sender_barrier = barrier.clone();
        let sender_timer = timer.clone();
        let sender = std::thread::spawn(move || {
            let mut delay = Box::pin(sender_timer.delay(Duration::from_secs(1)));
            sender_barrier.wait();
            assert!(poll(delay.as_mut()).is_pending());
            delay
        });

        barrier.wait();
        service.turn(genesis, budget());
        let delay = sender.join().unwrap();

        let counter = Arc::new(WakeCount::default());
        let waker = Waker::from(counter.clone());
        if service.prepare_wait(&waker) != WaitPlan::Immediate {
            assert_eq!(
                service.wheel.len(),
                1,
                "service parked before the completed registration was linked"
            );
        }

        drive(&mut service, genesis);
        assert_eq!(service.wheel.len(), 1);
        drop(delay);
        drive(&mut service, genesis);
        assert_eq!(service.wheel.len(), 0);
    }
}

#[test]
fn repeated_polls_reuse_the_registered_waker() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll_with_waker(delay.as_mut(), &waker).is_pending());
    drive(&mut service, genesis);
    for _ in 0..16 {
        assert!(poll_with_waker(delay.as_mut(), &waker).is_pending());
    }

    // A repoll with the same waker must not disarm the slot the service is going to take.
    drive(&mut service, genesis + Duration::from_millis(1));
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(poll_with_waker(delay.as_mut(), &waker), Poll::Ready(Ok(())));
}

#[test]
fn a_changed_waker_is_republished() {
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let first = Arc::new(WakeCount::default());
    let second = Arc::new(WakeCount::default());
    let first_waker = Waker::from(first.clone());
    let second_waker = Waker::from(second.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll_with_waker(delay.as_mut(), &first_waker).is_pending());
    assert_eq!(Arc::strong_count(&first), 3);
    drive(&mut service, genesis);
    assert!(poll_with_waker(delay.as_mut(), &second_waker).is_pending());
    assert_eq!(Arc::strong_count(&first), 2);
    assert_eq!(Arc::strong_count(&second), 3);

    drive(&mut service, genesis + Duration::from_millis(1));
    assert_eq!(first.0.load(Ordering::Relaxed), 0);
    assert_eq!(second.0.load(Ordering::Relaxed), 1);
    assert_eq!(Arc::strong_count(&second), 2);
}

#[test]
fn waker_drop_unwind_does_not_leave_a_stale_identity() {
    let _serial = panic_drop_waker::serial();
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let original_waker = panic_drop_waker::new();
    let replacement = Arc::new(WakeCount::default());
    let replacement_waker = Waker::from(replacement.clone());
    let mut delay = Box::pin(timer.delay(Duration::from_millis(1)));

    assert!(poll_with_waker(delay.as_mut(), &original_waker).is_pending());
    drive(&mut service, genesis);

    let replacement_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = poll_with_waker(delay.as_mut(), &replacement_waker);
    }));
    assert!(replacement_result.is_err());

    assert!(poll_with_waker(delay.as_mut(), &original_waker).is_pending());
    drive(&mut service, genesis + Duration::from_millis(1));
    assert_eq!(panic_drop_waker::wakes(), 1);
    assert_eq!(replacement.0.load(Ordering::Relaxed), 0);
    assert!(poll_with_waker(delay.as_mut(), &original_waker).is_ready());
}

#[test]
fn service_drop_closes_delays_before_reactor_waker_drop_unwinds() {
    let _serial = panic_drop_waker::serial();
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let mut delay = Box::pin(timer.delay(Duration::from_secs(1)));

    assert!(poll(delay.as_mut()).is_pending());
    drive(&mut service, genesis);
    let reactor_waker = panic_drop_waker::new();
    assert!(matches!(
        service.prepare_wait(&reactor_waker),
        WaitPlan::Until(_)
    ));

    let drop_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(service)));
    assert!(drop_result.is_err());
    assert_eq!(poll(delay.as_mut()), Poll::Ready(Err(TimerClosed)));
}

#[test]
fn registered_cancellation_survives_task_waker_drop_unwind() {
    let _serial = panic_drop_waker::serial();
    let genesis = Instant::now();
    let mut service = TimerService::new_at(genesis);
    let timer = service.handle();
    let task_waker = panic_drop_waker::new();
    let mut delay = Box::pin(timer.delay(Duration::from_secs(1)));

    assert!(poll_with_waker(delay.as_mut(), &task_waker).is_pending());
    drive(&mut service, genesis);
    assert_eq!(service.wheel.len(), 1);

    let drop_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(delay)));
    assert!(drop_result.is_err());
    drive(&mut service, genesis);
    assert_eq!(service.wheel.len(), 0);
    assert_eq!(wait_plan(&service), WaitPlan::Indefinite);
}
