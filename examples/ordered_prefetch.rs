// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Correctness-only, example-local ordered buffered prefetch. No timing claims.

#[cfg(target_os = "linux")]
#[path = "ordered_prefetch/reader.rs"]
mod reader;

#[cfg(target_os = "linux")]
#[path = "ordered_prefetch/linux.rs"]
mod linux;

fn main() -> std::process::ExitCode {
    #[cfg(target_os = "linux")]
    {
        match linux::run() {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("ordered_prefetch: {error}");
                std::process::ExitCode::FAILURE
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("ordered_prefetch requires Linux and a usable io_uring driver");
        std::process::ExitCode::FAILURE
    }
}
