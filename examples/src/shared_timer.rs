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

//! Share one application-owned timer thread between independent library clients.
//!
//! Keep the owner in `main` or application state and inject handle clones into libraries. This can
//! serve the entire process without a static singleton: tests can own separate instances, and the
//! owner's drop stops and joins its thread. A retained handle does not prevent shutdown.

use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use scorpio::time::TimerClosed;
use scorpio::time::TimerHandle;
use scorpio::time::TimerService;
use scorpio::time::TurnBudget;
use scorpio::time::WaitPlan;

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

struct ApplicationTimers {
    timer: TimerHandle,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ApplicationTimers {
    fn new() -> io::Result<Self> {
        let mut service = TimerService::new();
        let timer = service.handle();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let thread = thread::Builder::new()
            .name("application-timer".into())
            .spawn(move || {
                let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
                while !stopping.load(Ordering::Acquire) {
                    service.turn(Instant::now(), TurnBudget::default());
                    match service.prepare_wait(&waker) {
                        WaitPlan::Immediate => {}
                        WaitPlan::Until(deadline) => {
                            thread::park_timeout(
                                deadline.saturating_duration_since(Instant::now()),
                            );
                        }
                        WaitPlan::Indefinite => thread::park(),
                    }
                }
            })?;
        Ok(Self {
            timer,
            stop,
            thread: Some(thread),
        })
    }

    fn handle(&self) -> TimerHandle {
        self.timer.clone()
    }
}

impl Drop for ApplicationTimers {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let thread = self.thread.take().unwrap();
        thread.thread().unpark();
        // Also join during unwinding. Pending library timers observe service closure if the
        // driver panics; propagating a second panic here could abort the process.
        let _ = thread.join();
    }
}

struct Client {
    timer: TimerHandle,
}

impl Client {
    fn new(timer: TimerHandle) -> Self {
        Self { timer }
    }

    async fn retry_after(&self, duration: Duration) -> Result<(), TimerClosed> {
        self.timer.delay(duration).await
    }
}

fn main() -> io::Result<()> {
    let timers = ApplicationTimers::new()?;
    thread::scope(|scope| {
        for _ in 0..2 {
            let client = Client::new(timers.handle());
            scope.spawn(move || {
                pollster::block_on(client.retry_after(Duration::from_millis(10))).unwrap();
            });
        }
    });

    let surviving_delay = timers.handle().delay(Duration::MAX);
    drop(timers);
    assert_eq!(pollster::block_on(surviving_delay), Err(TimerClosed));
    Ok(())
}
