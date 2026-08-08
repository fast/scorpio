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

A scheduler independent asynchronous context.

## Minimum Rust version policy

This crate's minimum supported `rustc` version is `1.85.0`.

The current policy is that the minimum Rust version required to use this crate can be increased in minor version updates. For example, if `scorpio 1.0` requires Rust 1.85.0, then `scorpio 1.0.z` for all values of `z` will also require Rust 1.85.0 or newer. However, `scorpio 1.y` for `y > 0` may require a newer minimum version of Rust.

## License

This project is licensed under [Apache License, Version 2.0][license-url].
