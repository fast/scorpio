# Scorpio

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MSRV 1.85][msrv-badge]](https://www.whatrustisit.com)
[![Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/scorpio.svg
[crates-url]: https://crates.io/crates/scorpio
[docs-badge]: https://img.shields.io/docsrs/scorpio
[docs-url]: https://docs.rs/scorpio
[msrv-badge]: https://img.shields.io/badge/MSRV-1.85-green?logo=rust
[license-badge]: https://img.shields.io/crates/l/scorpio
[license-url]: https://www.apache.org/licenses/LICENSE-2.0
[actions-badge]: https://github.com/fast/scorpio/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/fast/scorpio/actions/workflows/ci.yml

A scheduler-independent set of asynchronous capabilities.

Scorpio does not rely on a process-global runtime, thread-local current handle, or crate-owned default thread. Applications keep each service in their own reactor and pass cloneable handles through task boundaries.

```rust
use std::time::Duration;

use scorpio::time::TimerHandle;
use scorpio::time::TimerService;

#[derive(Clone)]
struct AppContext {
    timer: TimerHandle,
}

let timer_service = TimerService::new();
let context = AppContext {
    timer: timer_service.handle(),
};
let delay = context.timer.delay(Duration::from_secs(1));

// The application reactor owns and drives `timer_service`; `delay` owns a handle clone.
drop((delay, timer_service));
```

Run `cargo run -p scorpio --example custom_reactor` for a complete reactor that advances a timer and parks safely. The ownership and timing-wheel tradeoffs are documented in [Timer service and handle design](https://github.com/fast/scorpio/blob/main/docs/timer-design.md). Run `cargo x bench --quick` for a single-iteration benchmark smoke test, or `cargo x bench [FILTER]` for Divan's statistical measurements.

## Acknowledgements

The initial `time` module is adapted from [fast/mea#137](https://github.com/fast/mea/pull/137), authored by [Orthur](https://github.com/orthur2) (`Orthur <orthur2@gmail.com>`, original commit [`5476e10`](https://github.com/fast/mea/commit/5476e1006fc80729f8e646f70aba1091fde72386)).

## Minimum Rust version policy

This crate's minimum supported `rustc` version is `1.85.0`.

The current policy is that the minimum Rust version required to use this crate can be increased in minor version updates. For example, if `scorpio 1.0` requires Rust 1.85.0, then `scorpio 1.0.z` for all values of `z` will also require Rust 1.85.0 or newer. However, `scorpio 1.y` for `y > 0` may require a newer minimum version of Rust.

## License

This project is licensed under [Apache License, Version 2.0][license-url].
