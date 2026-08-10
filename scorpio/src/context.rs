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

use std::fmt;

use crate::time::TimerContext;

/// An explicitly constructed bundle of asynchronous I/O capabilities.
///
/// `IoContext` is not a runtime or an executor. It starts no thread, owns no reactor driver, and
/// is never installed in process-global or thread-local state. Applications pass a clone through
/// their own task and service boundaries.
///
/// Capabilities are optional. Their drivers stay with the integrating reactor, making ownership
/// and progress explicit while allowing an application to include only the sources it uses.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use scorpio::IoContext;
/// use scorpio::time::TimerDriver;
///
/// let (timer_driver, timer) = TimerDriver::new();
/// let io = IoContext::new().with_timer(timer);
///
/// let delay = io
///     .timer()
///     .expect("the application installed timer capability")
///     .delay(Duration::from_secs(1));
///
/// // The application reactor owns and drives `timer_driver`.
/// drop((delay, timer_driver));
/// ```
#[derive(Clone, Default)]
pub struct IoContext {
    timer: Option<TimerContext>,
}

impl IoContext {
    /// Creates an empty context.
    pub const fn new() -> Self {
        Self { timer: None }
    }

    /// Installs timer capability.
    ///
    /// Calling this method again replaces the previously installed timer context. The matching
    /// [`TimerDriver`](crate::time::TimerDriver) remains owned by the application reactor.
    #[must_use = "the configured I/O context must be retained"]
    pub fn with_timer(mut self, timer: TimerContext) -> Self {
        self.timer = Some(timer);
        self
    }

    /// Returns the installed timer capability, if any.
    pub const fn timer(&self) -> Option<&TimerContext> {
        self.timer.as_ref()
    }
}

impl fmt::Debug for IoContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoContext")
            .field("timer", &self.timer.is_some())
            .finish_non_exhaustive()
    }
}
