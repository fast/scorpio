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

//! Drive a root future and timers on one application-owned reactor thread.
//!
//! An I/O reactor uses its own poller in place of parking below. Its notification must be latched:
//! a wake delivered between `prepare_wait` and the blocking wait must make that wait return.

use std::future::Future;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
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

async fn application_task(timer: TimerHandle) -> Result<(), TimerClosed> {
    timer.delay(Duration::from_millis(10)).await
}

fn main() -> Result<(), TimerClosed> {
    let mut service = TimerService::new();
    let mut task = std::pin::pin!(application_task(service.handle()));
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut cx = Context::from_waker(&waker);

    loop {
        service.turn(Instant::now(), TurnBudget::default());
        if let Poll::Ready(result) = task.as_mut().poll(&mut cx) {
            return result;
        }
        match service.prepare_wait(&waker) {
            WaitPlan::Immediate => {}
            WaitPlan::Until(deadline) => {
                thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
            }
            WaitPlan::Indefinite => thread::park(),
        }
    }
}
