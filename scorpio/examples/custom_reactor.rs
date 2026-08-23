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

use std::sync::Arc;
use std::sync::mpsc;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use scorpio::time::TimerHandle;
use scorpio::time::TimerService;
use scorpio::time::TurnBudget;
use scorpio::time::WaitPlan;

#[derive(Clone)]
struct AppContext {
    timer: TimerHandle,
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

async fn application_task(context: AppContext) {
    context
        .timer
        .delay(Duration::from_millis(10))
        .await
        .expect("the reactor keeps the timer service alive");
}

fn main() {
    let service = TimerService::new();
    let context = AppContext {
        timer: service.handle(),
    };
    let (stop_tx, stop_rx) = mpsc::channel();

    let reactor = thread::spawn(move || {
        let mut service = service;
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
        loop {
            service.turn(Instant::now(), TurnBudget::default());
            if stop_rx.try_recv().is_ok() {
                break;
            }
            match service.prepare_wait(&waker) {
                WaitPlan::Immediate => thread::yield_now(),
                WaitPlan::Until(deadline) => {
                    thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
                }
                WaitPlan::Indefinite => thread::park(),
            }
        }
    });

    pollster::block_on(application_task(context));
    stop_tx.send(()).unwrap();
    reactor.thread().unpark();
    reactor.join().unwrap();
}
