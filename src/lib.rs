// Copyright 2024 RustFS Team
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

//! Cancel-safe async io_uring read backend for RustFS
//! (rustfs/backlog#894, hardened per the #1048/#1051 audit).
//!
//! This crate proves and enforces the ownership model any production io_uring
//! integration in RustFS must follow:
//!
//! - The read buffer and the file handle are owned by the driver's pending
//!   (orphan) table from SQE submission until the CQE arrives. The kernel may
//!   write into the buffer at any point in that window, so nothing else is
//!   allowed to free or move its heap allocation.
//! - Dropping the caller-side future only abandons the *result*. It never
//!   touches the buffer. Optionally it submits `IORING_OP_ASYNC_CANCEL` to
//!   accelerate the CQE; reclamation still happens only at the CQE.
//! - Driver shutdown cancels all in-flight ops and drains the ring to
//!   `in_flight == 0` (with a bounded escape hatch) before the ring is
//!   unmapped.
//!
//! Status: read path only, Linux only. The driver supports positioned buffered
//! reads, `O_DIRECT` reads with internal alignment, sharded rings, async
//! backpressure, eventfd-driven reaping, graceful restricted-environment
//! detection, and bounded shutdown drain. The write path is intentionally out
//! of scope; see the
//! [design notes](https://github.com/rustfs/uring/blob/0.2.0/docs/DESIGN.md)
//! for the invariant details.

#[cfg(target_os = "linux")]
mod driver;

#[cfg(target_os = "linux")]
pub use driver::{ProbeFailure, StatsSnapshot, UringDriver};
