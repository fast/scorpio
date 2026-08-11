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

#![deny(missing_docs)]

//! `scorpio` provides scheduler-independent asynchronous contexts. Each capability is constructed
//! explicitly and passed through application-owned task boundaries. Scorpio starts no default
//! thread and never installs a process-global or thread-local handle.
//!
//! # Capabilities
//!
//! * [`time`]: Explicitly driven delays, timeouts, intervals, and scheduled actions.

pub mod time;

#[cfg(test)]
mod tests {
    use crate::time::Delay;
    use crate::time::Interval;
    use crate::time::TimerContext;
    use crate::time::TimerService;

    #[test]
    fn assert_send_and_sync() {
        fn do_assert_send_and_sync<T: Send + Sync>() {}
        do_assert_send_and_sync::<TimerContext>();
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
        do_assert_unwind_safe::<TimerContext>();
        do_assert_unwind_safe::<TimerService>();
        do_assert_unwind_safe::<Delay>();
        do_assert_unwind_safe::<Interval>();
    }
}
