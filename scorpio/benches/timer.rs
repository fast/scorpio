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

use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use scorpio::IoContext;
use scorpio::time::Delay;
use scorpio::time::TimerDriver;
use scorpio::time::TurnBudget;

const COUNTS: [usize; 3] = [1, 64, 1_024];
const DRIVER_COUNTS: [usize; 2] = [64, 1_024];
const FAR_FUTURE: Duration = Duration::from_secs(24 * 60 * 60);
const MIXED_OFFSETS_MILLIS: [u64; 8] = [1, 63, 64, 65, 4_095, 4_096, 65_535, 3_600_000];
const MAX_QUEUED_TIMERS: usize = 4_096;

fn poll_once<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    let mut cx = Context::from_waker(Waker::noop());
    future.poll(&mut cx)
}

fn full_budget() -> TurnBudget {
    let maximum = NonZeroUsize::new(usize::MAX).unwrap();
    TurnBudget::new(maximum, maximum)
}

fn frontend_lifecycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer/frontend_lifecycle");

    for count in COUNTS {
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::new("scorpio", count), &count, |b, &count| {
            let (mut driver, timer) = TimerDriver::new();
            let io = IoContext::new().with_timer(timer);
            let timer = io.timer().unwrap();

            b.iter_custom(|iterations| {
                let iterations_per_chunk = (MAX_QUEUED_TIMERS / count).max(1) as u64;
                let mut measured = Duration::ZERO;
                let mut remaining = iterations;

                while remaining > 0 {
                    let chunk = remaining.min(iterations_per_chunk);
                    let started = Instant::now();
                    for _ in 0..chunk {
                        let mut delays = (0..count)
                            .map(|_| Box::pin(timer.delay(FAR_FUTURE)))
                            .collect::<Vec<_>>();
                        for delay in &mut delays {
                            assert!(poll_once(delay.as_mut()).is_pending());
                        }
                        drop(delays);
                    }
                    measured += started.elapsed();

                    // Reclaim cancelled submissions without charging driver work to the
                    // API-side measurement or letting the queue grow with Criterion's sample.
                    assert!(!driver.turn(Instant::now(), full_budget()).has_more_work());
                    remaining -= chunk;
                }

                measured
            });
        });

        group.bench_with_input(BenchmarkId::new("tokio", count), &count, |b, &count| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let _entered = runtime.enter();

            b.iter(|| {
                let mut delays = (0..count)
                    .map(|_| Box::pin(tokio::time::sleep(FAR_FUTURE)))
                    .collect::<Vec<_>>();
                for delay in &mut delays {
                    assert!(poll_once(delay.as_mut()).is_pending());
                }
                drop(delays);
            });
        });

        group.bench_with_input(BenchmarkId::new("async_io", count), &count, |b, &count| {
            b.iter(|| {
                let mut delays = (0..count)
                    .map(|_| Box::pin(async_io::Timer::after(FAR_FUTURE)))
                    .collect::<Vec<_>>();
                for delay in &mut delays {
                    assert!(poll_once(delay.as_mut()).is_pending());
                }
                drop(delays);
            });
        });

        group.bench_with_input(
            BenchmarkId::new("futures_timer", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let mut delays = (0..count)
                        .map(|_| Box::pin(futures_timer::Delay::new(FAR_FUTURE)))
                        .collect::<Vec<_>>();
                    for delay in &mut delays {
                        assert!(poll_once(delay.as_mut()).is_pending());
                    }
                    drop(delays);
                });
            },
        );
    }

    group.finish();
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

fn scorpio_driver(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer/scorpio_driver");

    for count in DRIVER_COUNTS {
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(BenchmarkId::new("register", count), &count, |b, &count| {
            b.iter_custom(|iterations| {
                let mut measured = Duration::ZERO;
                for _ in 0..iterations {
                    let (mut driver, _delays, genesis) = submitted_scorpio_batch(count);
                    let started = Instant::now();
                    let result = driver.turn(genesis, full_budget());
                    measured += started.elapsed();

                    assert!(!result.has_more_work());
                    assert!(driver.next_poll_at().is_some());
                }
                measured
            });
        });

        group.bench_with_input(BenchmarkId::new("cancel", count), &count, |b, &count| {
            b.iter_custom(|iterations| {
                let mut measured = Duration::ZERO;
                for _ in 0..iterations {
                    let (mut driver, genesis) = cancellation_scorpio_batch(count);
                    let started = Instant::now();
                    let result = driver.turn(genesis, full_budget());
                    measured += started.elapsed();

                    assert!(!result.has_more_work());
                    assert_eq!(driver.next_poll_at(), None);
                }
                measured
            });
        });
    }

    group.finish();
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

fn expire_registered(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer/expire_registered");

    for count in DRIVER_COUNTS {
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(
            BenchmarkId::new("scorpio_driver_same_deadline", count),
            &count,
            |b, &count| {
                b.iter_batched_ref(
                    || scorpio_batch(count, false),
                    |batch| {
                        let (driver, delays, deadline) = batch;
                        assert!(!driver.turn(*deadline, full_budget()).has_more_work());
                        for delay in delays {
                            assert!(poll_once(delay.as_mut()).is_ready());
                        }
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("tokio_runtime_same_deadline", count),
            &count,
            |b, &count| {
                b.iter_batched_ref(
                    || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_time()
                            .start_paused(true)
                            .build()
                            .unwrap();
                        let delays = {
                            let _entered = runtime.enter();
                            let deadline = tokio::time::Instant::now() + Duration::from_millis(1);
                            let mut delays = (0..count)
                                .map(|_| Box::pin(tokio::time::sleep_until(deadline)))
                                .collect::<Vec<_>>();
                            for delay in &mut delays {
                                assert!(poll_once(delay.as_mut()).is_pending());
                            }
                            delays
                        };
                        (runtime, delays)
                    },
                    |batch| {
                        let (runtime, delays) = batch;
                        runtime.block_on(tokio::time::advance(Duration::from_millis(1)));
                        for delay in delays {
                            assert!(poll_once(delay.as_mut()).is_ready());
                        }
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    for count in DRIVER_COUNTS {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("scorpio_mixed_levels", count),
            &count,
            |b, &count| {
                b.iter_batched_ref(
                    || scorpio_batch(count, true),
                    |batch| {
                        let (driver, delays, deadline) = batch;
                        assert!(!driver.turn(*deadline, full_budget()).has_more_work());
                        for delay in delays {
                            assert!(poll_once(delay.as_mut()).is_ready());
                        }
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    frontend_lifecycle,
    scorpio_driver,
    expire_registered
);
criterion_main!(benches);
