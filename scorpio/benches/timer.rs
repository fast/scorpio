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
use scorpio::time::TimerDriver;
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

fn verify_exact_operation_batch(driver: &mut TimerDriver, now: Instant, count: usize) {
    // Processing exactly the operation limit leaves the wake handshake pending. Fewer operations
    // would let the driver observe an empty queue and clear it before returning.
    assert!(driver.turn(now, operation_budget(count)).has_more_work());
    // One extra operation would consume the probe budget and leave the handshake pending again.
    assert!(!driver.turn(now, operation_budget(1)).has_more_work());
    assert_eq!(driver.next_poll_at(), None);
}

struct FrontendLifecycleResult {
    driver: TimerDriver,
    _timer: TimerContext,
    count: usize,
}

impl Drop for FrontendLifecycleResult {
    fn drop(&mut self) {
        verify_exact_operation_batch(&mut self.driver, Instant::now(), self.count);
    }
}

struct RegisteredBatch {
    driver: TimerDriver,
    delays: Vec<Pin<Box<Delay>>>,
    now: Instant,
    count: usize,
    turn_result: scorpio::time::TurnResult,
}

impl Drop for RegisteredBatch {
    fn drop(&mut self) {
        assert!(!self.turn_result.has_more_work());
        assert!(self.driver.next_poll_at().is_some());
        self.delays.clear();
        verify_exact_operation_batch(&mut self.driver, self.now, self.count);
    }
}

struct ExpiredBatch {
    driver: TimerDriver,
    _delays: Vec<Pin<Box<Delay>>>,
    turn_result: scorpio::time::TurnResult,
    ready_count: usize,
    expected_count: usize,
}

impl Drop for ExpiredBatch {
    fn drop(&mut self) {
        assert!(!self.turn_result.has_more_work());
        assert_eq!(self.ready_count, self.expected_count);
        assert_eq!(self.driver.next_poll_at(), None);
    }
}

struct CancelledBatch {
    driver: TimerDriver,
    now: Instant,
    turn_result: scorpio::time::TurnResult,
}

impl Drop for CancelledBatch {
    fn drop(&mut self) {
        assert!(self.turn_result.has_more_work());
        assert!(
            !self
                .driver
                .turn(self.now, operation_budget(1))
                .has_more_work()
        );
        assert_eq!(self.driver.next_poll_at(), None);
    }
}

mod frontend_lifecycle {
    use super::*;

    #[divan::bench(args = [1, 64, 1_024], sample_size = 1)]
    fn scorpio(bencher: Bencher, count: usize) {
        bencher
            .counter(ItemsCount::new(count))
            .with_inputs(TimerDriver::new)
            .bench_local_values(|(driver, timer)| {
                let mut delays = (0..count)
                    .map(|_| Box::pin(timer.delay(FAR_FUTURE)))
                    .collect::<Vec<_>>();
                for delay in &mut delays {
                    assert!(poll_once(delay.as_mut()).is_pending());
                }
                drop(delays);
                FrontendLifecycleResult {
                    driver,
                    _timer: timer,
                    count,
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
            for delay in &mut delays {
                assert!(poll_once(delay.as_mut()).is_pending());
            }
            drop(delays);
        });
    }

    #[divan::bench(args = [1, 64, 1_024], sample_size = 1)]
    fn async_io(bencher: Bencher, count: usize) {
        bencher.counter(ItemsCount::new(count)).bench_local(|| {
            let mut delays = (0..count)
                .map(|_| Box::pin(async_io::Timer::after(FAR_FUTURE)))
                .collect::<Vec<_>>();
            for delay in &mut delays {
                assert!(poll_once(delay.as_mut()).is_pending());
            }
            drop(delays);
        });
    }

    #[divan::bench(args = [1, 64, 1_024], sample_size = 1)]
    fn futures_timer(bencher: Bencher, count: usize) {
        bencher.counter(ItemsCount::new(count)).bench_local(|| {
            let mut delays = (0..count)
                .map(|_| Box::pin(futures_timer::Delay::new(FAR_FUTURE)))
                .collect::<Vec<_>>();
            for delay in &mut delays {
                assert!(poll_once(delay.as_mut()).is_pending());
            }
            drop(delays);
        });
    }
}

fn submitted_scorpio_batch(count: usize) -> (TimerDriver, Vec<Pin<Box<Delay>>>, Instant) {
    let genesis = Instant::now();
    let deadline = genesis + FAR_FUTURE;
    let (driver, timer) = TimerDriver::new_at(genesis);
    let mut delays = (0..count)
        .map(|_| Box::pin(timer.delay_until(deadline)))
        .collect::<Vec<_>>();
    for delay in &mut delays {
        assert!(poll_once(delay.as_mut()).is_pending());
    }
    (driver, delays, genesis)
}

fn cancellation_scorpio_batch(count: usize) -> (TimerDriver, Instant) {
    let (mut driver, delays, genesis) = submitted_scorpio_batch(count);
    assert!(!driver.turn(genesis, full_budget()).has_more_work());
    assert!(driver.next_poll_at().is_some());
    drop(delays);
    (driver, genesis)
}

mod scorpio_driver {
    use super::*;

    #[divan::bench(args = [64, 1_024], sample_size = 1)]
    fn register(bencher: Bencher, count: usize) {
        bencher
            .counter(ItemsCount::new(count))
            .with_inputs(|| submitted_scorpio_batch(count))
            .bench_local_values(|(mut driver, delays, genesis)| {
                let turn_result = driver.turn(genesis, full_budget());
                RegisteredBatch {
                    driver,
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
            .bench_local_values(|(mut driver, genesis)| {
                let turn_result = driver.turn(genesis, operation_budget(count));
                CancelledBatch {
                    driver,
                    now: genesis,
                    turn_result,
                }
            });
    }
}

fn scorpio_batch(count: usize, mixed: bool) -> (TimerDriver, Vec<Pin<Box<Delay>>>, Instant) {
    let genesis = Instant::now();
    let (mut driver, timer) = TimerDriver::new_at(genesis);
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
    assert!(!driver.turn(genesis, full_budget()).has_more_work());

    let deadline = if mixed {
        genesis + Duration::from_millis(*MIXED_OFFSETS_MILLIS.last().unwrap())
    } else {
        genesis + Duration::from_millis(1)
    };
    (driver, delays, deadline)
}

mod expire_registered {
    use super::*;

    #[divan::bench(args = [64, 1_024], sample_size = 1)]
    fn scorpio_same_deadline(bencher: Bencher, count: usize) {
        bencher
            .counter(ItemsCount::new(count))
            .with_inputs(|| scorpio_batch(count, false))
            .bench_local_values(|(mut driver, mut delays, deadline)| {
                let turn_result = driver.turn(deadline, full_budget());
                let ready_count = delays
                    .iter_mut()
                    .map(|delay| usize::from(poll_once(delay.as_mut()).is_ready()))
                    .sum();
                ExpiredBatch {
                    driver,
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
            .bench_local_values(|(mut driver, mut delays, deadline)| {
                let turn_result = driver.turn(deadline, full_budget());
                let ready_count = delays
                    .iter_mut()
                    .map(|delay| usize::from(poll_once(delay.as_mut()).is_ready()))
                    .sum();
                ExpiredBatch {
                    driver,
                    _delays: delays,
                    turn_result,
                    ready_count,
                    expected_count: count,
                }
            });
    }
}
