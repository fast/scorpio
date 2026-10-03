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
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Barrier;
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

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
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
