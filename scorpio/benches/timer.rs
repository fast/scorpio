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
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use divan::Bencher;
use divan::counter::ItemsCount;
use scorpio::time::Delay;
use scorpio::time::TimerContext;
use scorpio::time::TimerService;
use scorpio::time::TurnBudget;

const FAR_FUTURE: Duration = Duration::from_secs(24 * 60 * 60);
const MIXED_OFFSETS_MILLIS: [u64; 8] = [1, 63, 64, 65, 4_095, 4_096, 65_535, 3_600_000];

fn main() {
    divan::main();
}

fn poll_once<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    let mut cx = Context::from_waker(Waker::noop());
    future.poll(&mut cx)
}

fn full_budget() -> TurnBudget {
    let maximum = NonZeroUsize::new(usize::MAX).unwrap();
    TurnBudget::new(maximum, maximum)
}

fn operation_budget(count: usize) -> TurnBudget {
    TurnBudget::new(NonZeroUsize::new(count).unwrap(), NonZeroUsize::MAX)
}

fn verify_exact_operation_batch(
    service: &mut TimerService,
    timer: &TimerContext,
    now: Instant,
    count: usize,
) {
    // Append a sentinel after the expected operations. Exactly `count` operations leave it queued;
    // fewer process it too soon, while extras prevent the one-operation probe from reaching it.
    let sentinel_deadline = now + FAR_FUTURE;
    let mut sentinel = Box::pin(timer.delay_until(sentinel_deadline));
    assert!(poll_once(sentinel.as_mut()).is_pending());
    assert!(service.turn(now, operation_budget(count)).has_more_work());
    assert!(!service.turn(now, operation_budget(1)).has_more_work());
    assert!(service.next_poll_at().is_some_and(|next| next > now));
    drop(sentinel);
    assert!(!service.turn(now, operation_budget(1)).has_more_work());
    assert_eq!(service.next_poll_at(), None);
}

struct FrontendLifecycleResult {
    service: TimerService,
    timer: TimerContext,
    count: usize,
    pending_count: usize,
}

impl Drop for FrontendLifecycleResult {
    fn drop(&mut self) {
        assert_eq!(self.pending_count, self.count);
        verify_exact_operation_batch(&mut self.service, &self.timer, Instant::now(), self.count);
    }
}

struct FrontendPollResult {
    pending_count: usize,
    expected_count: usize,
}

impl Drop for FrontendPollResult {
    fn drop(&mut self) {
        assert_eq!(self.pending_count, self.expected_count);
    }
}

struct RegisteredBatch {
    service: TimerService,
    timer: TimerContext,
    delays: Vec<Pin<Box<Delay>>>,
    now: Instant,
    count: usize,
    turn_result: scorpio::time::TurnResult,
}

impl Drop for RegisteredBatch {
    fn drop(&mut self) {
        assert!(!self.turn_result.has_more_work());
        assert!(self.service.next_poll_at().is_some());
        self.delays.clear();
        verify_exact_operation_batch(&mut self.service, &self.timer, self.now, self.count);
    }
}

struct ExpiredBatch {
    service: TimerService,
    _delays: Vec<Pin<Box<Delay>>>,
    turn_result: scorpio::time::TurnResult,
    ready_count: usize,
    expected_count: usize,
}

impl Drop for ExpiredBatch {
    fn drop(&mut self) {
        assert!(!self.turn_result.has_more_work());
        assert_eq!(self.ready_count, self.expected_count);
        assert_eq!(self.service.next_poll_at(), None);
    }
}

struct CancelledBatch {
    service: TimerService,
    turn_result: scorpio::time::TurnResult,
}

impl Drop for CancelledBatch {
    fn drop(&mut self) {
        assert!(!self.turn_result.has_more_work());
        assert_eq!(self.service.next_poll_at(), None);
    }
}

mod frontend_lifecycle {
    use super::*;

    #[divan::bench(args = [1, 64, 1_024], sample_size = 1)]
    fn scorpio(bencher: Bencher, count: usize) {
        bencher
            .counter(ItemsCount::new(count))
            .with_inputs(TimerService::new)
            .bench_local_values(|(service, timer)| {
                let mut delays = (0..count)
                    .map(|_| Box::pin(timer.delay(FAR_FUTURE)))
                    .collect::<Vec<_>>();
                let pending_count = delays
                    .iter_mut()
                    .map(|delay| usize::from(poll_once(delay.as_mut()).is_pending()))
                    .sum();
                drop(delays);
                FrontendLifecycleResult {
                    service,
                    timer,
                    count,
                    pending_count,
                }
            });
    }

    #[divan::bench(args = [1, 64, 1_024], sample_size = 1)]
    fn tokio(bencher: Bencher, count: usize) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _entered = runtime.enter();

        bencher.counter(ItemsCount::new(count)).bench_local(|| {
            let mut delays = (0..count)
                .map(|_| Box::pin(tokio::time::sleep(FAR_FUTURE)))
                .collect::<Vec<_>>();
            let pending_count = delays
                .iter_mut()
                .map(|delay| usize::from(poll_once(delay.as_mut()).is_pending()))
                .sum();
            drop(delays);
            FrontendPollResult {
                pending_count,
                expected_count: count,
            }
        });
    }

    #[divan::bench(args = [1, 64, 1_024], sample_size = 1)]
    fn async_io(bencher: Bencher, count: usize) {
        bencher.counter(ItemsCount::new(count)).bench_local(|| {
            let mut delays = (0..count)
                .map(|_| Box::pin(async_io::Timer::after(FAR_FUTURE)))
                .collect::<Vec<_>>();
            let pending_count = delays
                .iter_mut()
                .map(|delay| usize::from(poll_once(delay.as_mut()).is_pending()))
                .sum();
            drop(delays);
            FrontendPollResult {
                pending_count,
                expected_count: count,
            }
        });
    }

    #[divan::bench(args = [1, 64, 1_024], sample_size = 1)]
    fn futures_timer(bencher: Bencher, count: usize) {
        bencher.counter(ItemsCount::new(count)).bench_local(|| {
            let mut delays = (0..count)
                .map(|_| Box::pin(futures_timer::Delay::new(FAR_FUTURE)))
                .collect::<Vec<_>>();
            let pending_count = delays
                .iter_mut()
                .map(|delay| usize::from(poll_once(delay.as_mut()).is_pending()))
                .sum();
            drop(delays);
            FrontendPollResult {
                pending_count,
                expected_count: count,
            }
        });
    }
}

fn submitted_scorpio_batch(
    count: usize,
) -> (TimerService, TimerContext, Vec<Pin<Box<Delay>>>, Instant) {
    let genesis = Instant::now();
    let deadline = genesis + FAR_FUTURE;
    let (service, timer) = TimerService::new_at(genesis);
    let mut delays = (0..count)
        .map(|_| Box::pin(timer.delay_until(deadline)))
        .collect::<Vec<_>>();
    for delay in &mut delays {
        assert!(poll_once(delay.as_mut()).is_pending());
    }
    (service, timer, delays, genesis)
}

fn cancellation_scorpio_batch(count: usize) -> (TimerService, Instant) {
    let (mut service, _timer, delays, genesis) = submitted_scorpio_batch(count);
    assert!(!service.turn(genesis, full_budget()).has_more_work());
    assert!(service.next_poll_at().is_some());
    drop(delays);
    (service, genesis)
}

mod scorpio_service {
    use super::*;

    #[divan::bench(args = [64, 1_024], sample_size = 1)]
    fn register(bencher: Bencher, count: usize) {
        bencher
            .counter(ItemsCount::new(count))
            .with_inputs(|| submitted_scorpio_batch(count))
            .bench_local_values(|(mut service, timer, delays, genesis)| {
                let turn_result = service.turn(genesis, full_budget());
                RegisteredBatch {
                    service,
                    timer,
                    delays,
                    now: genesis,
                    count,
                    turn_result,
                }
            });
    }

    #[divan::bench(args = [64, 1_024], sample_size = 1)]
    fn cancel(bencher: Bencher, count: usize) {
        bencher
            .counter(ItemsCount::new(count))
            .with_inputs(|| cancellation_scorpio_batch(count))
            .bench_local_values(|(mut service, genesis)| {
                let turn_result = service.turn(genesis, operation_budget(count));
                CancelledBatch {
                    service,
                    turn_result,
                }
            });
    }
}

fn scorpio_batch(count: usize, mixed: bool) -> (TimerService, Vec<Pin<Box<Delay>>>, Instant) {
    let genesis = Instant::now();
    let (mut service, timer) = TimerService::new_at(genesis);
    let mut delays = (0..count)
        .map(|index| {
            let offset = if mixed {
                Duration::from_millis(MIXED_OFFSETS_MILLIS[index % MIXED_OFFSETS_MILLIS.len()])
            } else {
                Duration::from_millis(1)
            };
            Box::pin(timer.delay_until(genesis + offset))
        })
        .collect::<Vec<_>>();
    for delay in &mut delays {
        assert!(poll_once(delay.as_mut()).is_pending());
    }
    assert!(!service.turn(genesis, full_budget()).has_more_work());

    let deadline = if mixed {
        genesis + Duration::from_millis(*MIXED_OFFSETS_MILLIS.last().unwrap())
    } else {
        genesis + Duration::from_millis(1)
    };
    (service, delays, deadline)
}

mod expire_registered {
    use super::*;

    #[divan::bench(args = [64, 1_024], sample_size = 1)]
    fn scorpio_same_deadline(bencher: Bencher, count: usize) {
        bencher
            .counter(ItemsCount::new(count))
            .with_inputs(|| scorpio_batch(count, false))
            .bench_local_values(|(mut service, mut delays, deadline)| {
                let turn_result = service.turn(deadline, full_budget());
                let ready_count = delays
                    .iter_mut()
                    .map(|delay| usize::from(poll_once(delay.as_mut()).is_ready()))
                    .sum();
                ExpiredBatch {
                    service,
                    _delays: delays,
                    turn_result,
                    ready_count,
                    expected_count: count,
                }
            });
    }

    #[divan::bench(args = [64, 1_024], sample_size = 1)]
    fn scorpio_mixed_levels(bencher: Bencher, count: usize) {
        bencher
            .counter(ItemsCount::new(count))
            .with_inputs(|| scorpio_batch(count, true))
            .bench_local_values(|(mut service, mut delays, deadline)| {
                let turn_result = service.turn(deadline, full_budget());
                let ready_count = delays
                    .iter_mut()
                    .map(|delay| usize::from(poll_once(delay.as_mut()).is_ready()))
                    .sum();
                ExpiredBatch {
                    service,
                    _delays: delays,
                    turn_result,
                    ready_count,
                    expected_count: count,
                }
            });
    }
}
