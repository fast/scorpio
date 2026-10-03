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

use scorpio::time::Delay;
use scorpio::time::Interval;
use scorpio::time::TimerHandle;
use scorpio::time::TimerService;

#[test]
fn assert_send_and_sync() {
    fn do_assert_send_and_sync<T: Send + Sync>() {}
    do_assert_send_and_sync::<TimerHandle>();
    do_assert_send_and_sync::<TimerService>();
    do_assert_send_and_sync::<Delay>();
    do_assert_send_and_sync::<Interval>();
}

#[test]
fn assert_unpin() {
    fn do_assert_unpin<T: Unpin>() {}
    do_assert_unpin::<Delay>();
    do_assert_unpin::<Interval>();
}

#[test]
fn assert_unwind_safe() {
    fn do_assert_unwind_safe<T: std::panic::RefUnwindSafe + std::panic::UnwindSafe>() {}
    do_assert_unwind_safe::<TimerHandle>();
    do_assert_unwind_safe::<TimerService>();
    do_assert_unwind_safe::<Delay>();
    do_assert_unwind_safe::<Interval>();
}
