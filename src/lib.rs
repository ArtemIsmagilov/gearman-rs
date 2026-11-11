//! A complete, ergonomic Rust implementation of the [Gearman] protocol.
//!
//! The crate provides:
//!
//! * [`Client`] — foreground/background job submission, priorities, scheduling,
//!   reduce jobs, status queries, options and echo.
//! * [`Worker`] — function registration, `GRAB_JOB*`, `PRE_SLEEP`, streaming job
//!   updates and a convenient job-processing loop.
//! * [`AdminClient`] — the Gearman text-based administrative protocol.
//! * [`protocol`] — typed packets and a raw binary codec.
//!
//! # Example: client
//!
//! ```no_run
//! use gearman_rs::Client;
//!
//! # fn main() -> Result<(), gearman_rs::Error> {
//! let mut client = Client::connect("127.0.0.1:4730")?;
//! let result = client.execute("reverse", b"hello")?;
//! println!("{}", String::from_utf8_lossy(&result.data));
//! # Ok(())
//! # }
//! ```
//!
//! # Example: worker
//!
//! ```no_run
//! use gearman_rs::Worker;
//!
//! # fn main() -> Result<(), gearman_rs::Error> {
//! let mut worker = Worker::connect("127.0.0.1:4730")?;
//! worker.register("reverse")?;
//! worker.run(|job| {
//!     let mut result = job.workload().to_vec();
//!     result.reverse();
//!     job.complete(result)
//! })?;
//! # Ok(())
//! # }
//! ```
//!
//! # Async API
//!
//! Enable the `tokio` (or `async`) feature to use [`AsyncClient`],
//! [`AsyncWorker`] and [`AsyncAdminClient`].
//!
//! [Gearman]: https://gearman.org/

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod admin;
pub mod client;
pub mod error;
mod net;
pub mod protocol;
#[cfg(feature = "tls")]
pub mod tls;
pub mod worker;

pub use admin::{
    AdminClient, AdminClientBuilder, FunctionStatus, JobInfo, PriorityStatus, WorkerInfo,
};
pub use client::{
    Client, ClientBuilder, JobHandle, JobRequest, JobResult, JobStatus, Priority, Schedule,
};
/// Convenient alias for `std::result::Result<T, Error>`.
pub use error::Result;
pub use error::{Error, JobFailure};
pub use protocol::{Packet, PacketType};
#[cfg(feature = "tls")]
pub use tls::TlsConfig;
pub use worker::{Job, Worker, WorkerBuilder, WorkerJob};

#[cfg(feature = "tokio")]
mod asynchronous;
#[cfg(feature = "tokio")]
pub use asynchronous::{
    AsyncAdminClient, AsyncAdminClientBuilder, AsyncClient, AsyncClientBuilder, AsyncWorker,
    AsyncWorkerBuilder, AsyncWorkerJob,
};

/// Helpers used by the real-server documentation tests.
///
/// This module is an implementation detail and may change at any time.
#[doc(hidden)]
pub mod _test_support {
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use crate::{AdminClient, Error, JobHandle, Worker, WorkerJob};

    /// Builds a unique function name suitable for parallel documentation tests.
    pub fn unique(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before the Unix epoch")
            .as_nanos();
        let prefix = &prefix[..prefix.len().min(32)];
        format!("grs_{prefix}_{:?}_{nanos:x}", thread::current().id())
    }

    /// Returns a Unix timestamp a number of seconds in the future.
    pub fn epoch_in(seconds: u64) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before the Unix epoch")
            .as_secs()
            + seconds
    }

    /// Runs `handler` with a connected, registered worker and signals readiness.
    pub fn spawn_worker<F>(
        address: &'static str,
        function: String,
        handler: F,
    ) -> (mpsc::Receiver<()>, thread::JoinHandle<Result<(), Error>>)
    where
        F: FnOnce(&mut Worker) -> Result<(), Error> + Send + 'static,
    {
        let (ready_tx, ready_rx) = mpsc::channel();
        let join = thread::spawn(move || -> Result<(), Error> {
            let mut worker = Worker::builder()
                .read_timeout(Duration::from_secs(10))
                .connect(address)?;
            worker.register(&function)?;
            ready_tx.send(()).expect("doc test receiver was dropped");
            handler(&mut worker)
        });
        (ready_rx, join)
    }

    /// Runs exactly one job with `handler` and signals readiness.
    pub fn spawn_worker_once<F>(
        address: &'static str,
        function: String,
        mut handler: F,
    ) -> (mpsc::Receiver<()>, thread::JoinHandle<Result<(), Error>>)
    where
        F: FnMut(&mut WorkerJob<'_>) -> Result<(), Error> + Send + 'static,
    {
        spawn_worker(address, function, move |worker| {
            worker.run_once(&mut handler)
        })
    }

    /// Cancels a background job during a documentation test.
    pub fn cancel(address: &str, handle: &JobHandle) -> Result<(), Error> {
        AdminClient::connect(address)?.cancel_job(handle.as_str())
    }

    /// Runs `handler` with a connected, registered asynchronous worker.
    #[cfg(feature = "tokio")]
    pub fn spawn_async_worker<F, Fut>(
        function: String,
        handler: F,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::task::JoinHandle<Result<(), Error>>,
    )
    where
        F: FnOnce(crate::AsyncWorker) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), Error>> + Send,
    {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let mut worker = crate::AsyncWorker::builder()
                .read_timeout(Duration::from_secs(10))
                .connect("127.0.0.1:4730")
                .await?;
            worker.register(&function).await?;
            let _ = ready_tx.send(());
            handler(worker).await
        });
        (ready_rx, join)
    }

    /// Runs exactly one job with an asynchronous worker.
    #[cfg(feature = "tokio")]
    pub fn spawn_async_worker_once<F, Fut>(
        function: String,
        handler: F,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::task::JoinHandle<Result<(), Error>>,
    )
    where
        F: FnOnce(crate::AsyncWorkerJob) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), Error>> + Send,
    {
        spawn_async_worker(function, move |mut worker| async move {
            let job = worker.next_job().await?;
            handler(job).await
        })
    }
}
