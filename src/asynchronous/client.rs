//! Asynchronous Gearman client.

use std::collections::HashMap;
use std::time::Duration;
use tokio::net::ToSocketAddrs;

use crate::client::{
    JobHandle, JobRequest, JobResult, JobStatus, Schedule, parse_status, parse_unique_status,
};
use crate::error::{Error, JobFailure, Result};
use crate::net::Config;
use crate::protocol::{
    self, Packet, PacketType, ensure_no_nul, field_to_string, field_to_string_trim, parse_u32,
    split_fields, trim_trailing_nul,
};

use super::connection::Connection;

#[derive(Debug, Default)]
struct PendingJob {
    streamed_data: Vec<Vec<u8>>,
    warnings: Vec<Vec<u8>>,
    status: Option<JobStatus>,
    exception: Option<Vec<u8>>,
    completed: Option<Vec<u8>>,
    failed: bool,
}

/// Builder for [`Client`].
#[derive(Debug, Clone, Default)]
pub struct ClientBuilder {
    config: Config,
}

impl ClientBuilder {
    /// Creates a builder with default connection settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the timeout for the TCP connect and TLS handshake.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = Some(timeout);
        self
    }

    /// Sets the read timeout used for all asynchronous reads.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.config.read_timeout = Some(timeout);
        self
    }

    /// Sets the write timeout used for all asynchronous writes.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.config.write_timeout = Some(timeout);
        self
    }

    /// Enables or disables `TCP_NODELAY` (enabled by default).
    pub fn nodelay(mut self, nodelay: bool) -> Self {
        self.config.nodelay = nodelay;
        self
    }

    /// Enables TLS using the given configuration.
    #[cfg(feature = "tokio-tls")]
    pub fn tls(mut self, tls: crate::TlsConfig) -> Self {
        self.config.tls = Some(tls);
        self
    }

    /// Sets the maximum accepted packet size.
    pub fn max_packet_size(mut self, max_packet_size: usize) -> Self {
        self.config.max_packet_size = max_packet_size;
        self
    }

    /// Connects to a Gearman server.
    pub async fn connect<A: ToSocketAddrs>(self, address: A) -> Result<Client> {
        Client::with_config(address, self.config).await
    }
}

/// An asynchronous Gearman client.
pub struct Client {
    connection: Connection,
    pending: HashMap<String, PendingJob>,
}

impl Client {
    /// Returns a builder for configuring the connection.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Connects to a Gearman server with default settings.
    pub async fn connect<A: ToSocketAddrs>(address: A) -> Result<Self> {
        Self::with_config(address, Config::default()).await
    }

    async fn with_config<A: ToSocketAddrs>(address: A, config: Config) -> Result<Self> {
        Ok(Self {
            connection: Connection::connect(address, &config).await?,
            pending: HashMap::new(),
        })
    }

    /// Submits a job request and returns its handle.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, JobRequest};
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker_once(
    /// #     function.clone(),
    /// #     |mut job| async move {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let request = JobRequest::new(function.clone(), b"hello").unique("request-1");
    /// let handle = client.submit(&request).await?;
    /// let result = client.wait(&handle).await?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit(&mut self, request: &JobRequest) -> Result<JobHandle> {
        request.validate()?;

        let packet_type = request.packet_type();
        let payload = request.encode();
        self.connection.send(packet_type, &payload).await?;

        let handle = self.read_job_created().await?;
        if request.expects_result() {
            self.pending.entry(handle.to_string()).or_default();
        }
        Ok(handle)
    }

    /// Submits a foreground job and waits for its result.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, JobRequest};
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit_and_wait");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker_once(
    /// #     function.clone(),
    /// #     |mut job| async move {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let request = JobRequest::new(function, b"hello");
    /// let result = client.submit_and_wait(&request).await?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_and_wait(&mut self, request: &JobRequest) -> Result<JobResult> {
        if !request.expects_result() {
            return Err(Error::InvalidArgument(
                "background or scheduled requests cannot be awaited".to_owned(),
            ));
        }
        let handle = self.submit(request).await?;
        self.wait(&handle).await
    }

    /// Submits a normal-priority foreground job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit_job");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker_once(
    /// #     function.clone(),
    /// #     |mut job| async move {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job(function, b"hello").await?;
    /// let result = client.wait(&handle).await?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload))
            .await
    }

    /// Submits a normal-priority foreground job with a unique id.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let function = "reverse";
    /// let unique = "request-1";
    /// # let function = gearman_rs::_test_support::unique("submit_job_unique");
    /// # let unique = gearman_rs::_test_support::unique("submit_job_unique_id");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker_once(
    /// #     function.clone(),
    /// #     |mut job| async move {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_unique(function, unique, b"hello").await?;
    /// let result = client.wait(&handle).await?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_unique(
        &mut self,
        function: impl AsRef<str>,
        unique: impl Into<String>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload).unique(unique))
            .await
    }

    /// Submits a normal-priority background job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("submit_job_bg");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg(function, b"payload").await?;
    /// assert!(!handle.as_str().is_empty());
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_bg(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload).background())
            .await
    }

    /// Submits a normal-priority background job with a unique id.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// let unique = "job-1";
    /// # let function = gearman_rs::_test_support::unique("submit_job_bg_unique");
    /// # let unique = gearman_rs::_test_support::unique("submit_job_bg_unique_id");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg_unique(function, unique.clone(), b"payload").await?;
    /// let status = client.job_status_unique(&unique).await?;
    /// assert!(status.known);
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_bg_unique(
        &mut self,
        function: impl AsRef<str>,
        unique: impl Into<String>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .unique(unique)
                .background(),
        )
        .await
    }

    /// Submits a high-priority foreground job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit_job_high");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker_once(
    /// #     function.clone(),
    /// #     |mut job| async move {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_high(function, b"hello").await?;
    /// assert_eq!(client.wait(&handle).await?.data, b"olleh");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_high(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload).high())
            .await
    }

    /// Submits a high-priority background job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("submit_job_high_bg");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg(function, b"payload").await?;
    /// assert!(!handle.as_str().is_empty());
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_high_bg(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .high()
                .background(),
        )
        .await
    }

    /// Submits a low-priority foreground job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit_job_low");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker_once(
    /// #     function.clone(),
    /// #     |mut job| async move {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_low(function, b"hello").await?;
    /// assert_eq!(client.wait(&handle).await?.data, b"olleh");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_low(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload).low())
            .await
    }

    /// Submits a low-priority background job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("submit_job_low_bg");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg(function, b"payload").await?;
    /// assert!(!handle.as_str().is_empty());
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_low_bg(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .low()
                .background(),
        )
        .await
    }

    /// Submits a scheduled background job.
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, Schedule};
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client
    ///     .submit_job_sched("cleanup", "nightly", Schedule::every(), b"payload")
    ///     .await?;
    /// assert!(!handle.as_str().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_sched(
        &mut self,
        function: impl AsRef<str>,
        unique: impl Into<String>,
        schedule: Schedule,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .unique(unique)
                .schedule(schedule),
        )
        .await
    }

    /// Submits a background job to run at a Unix epoch.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "reminder";
    /// let unique = "reminder-1";
    /// # let function = gearman_rs::_test_support::unique("submit_job_epoch");
    /// # let unique = gearman_rs::_test_support::unique("submit_job_epoch_id");
    /// let epoch = 1_900_000_000;
    /// # let epoch = gearman_rs::_test_support::epoch_in(3_600);
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_epoch(function, unique, epoch, b"payload").await?;
    /// assert!(!handle.as_str().is_empty());
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_job_epoch(
        &mut self,
        function: impl AsRef<str>,
        unique: impl Into<String>,
        epoch: u64,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .unique(unique)
                .run_at(epoch),
        )
        .await
    }

    /// Submits a foreground reduce job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "reduce-sum";
    /// let unique = "reduce-1";
    /// # let function = gearman_rs::_test_support::unique("submit_reduce_job");
    /// # let unique = gearman_rs::_test_support::unique("submit_reduce_job_id");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job_all().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         assert_eq!(job.reducer.as_deref(), Some("sum"));
    /// #         worker.send_work_complete(&job.handle, b"reduced").await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_reduce_job(function, unique, "sum", b"parts").await?;
    /// assert_eq!(client.wait(&handle).await?.data, b"reduced");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_reduce_job(
        &mut self,
        function: impl AsRef<str>,
        unique: impl Into<String>,
        reducer: impl Into<String>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .unique(unique)
                .reducer(reducer),
        )
        .await
    }

    /// Submits a background reduce job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "reduce-sum";
    /// let unique = "reduce-1";
    /// # let function = gearman_rs::_test_support::unique("submit_reduce_job_bg");
    /// # let unique = gearman_rs::_test_support::unique("submit_reduce_job_bg_id");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client
    ///     .submit_reduce_job_bg(function, unique, "sum", b"parts")
    ///     .await?;
    /// assert!(client.job_status(&handle).await?.known);
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn submit_reduce_job_bg(
        &mut self,
        function: impl AsRef<str>,
        unique: impl Into<String>,
        reducer: impl Into<String>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .unique(unique)
                .reducer(reducer)
                .background(),
        )
        .await
    }

    /// Submits a foreground job and waits for the result.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("execute");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker_once(
    /// #     function.clone(),
    /// #     |mut job| async move {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let result = client.execute(function, b"hello").await?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobResult> {
        self.submit_and_wait(&JobRequest::new(function.as_ref(), workload))
            .await
    }

    /// Waits for the result of a foreground job.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("wait");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker_once(
    /// #     function.clone(),
    /// #     |mut job| async move {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job(function, b"hello").await?;
    /// let result = client.wait(&handle).await?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait(&mut self, handle: &JobHandle) -> Result<JobResult> {
        if !self.pending.contains_key(handle.as_str()) {
            return Err(Error::UnknownJob {
                handle: handle.to_string(),
            });
        }

        loop {
            let ready = match self.pending.get(handle.as_str()) {
                Some(pending) => pending.failed || pending.completed.is_some(),
                None => {
                    return Err(Error::UnknownJob {
                        handle: handle.to_string(),
                    });
                }
            };

            if ready {
                let pending = self.pending.remove(handle.as_str()).expect("checked above");
                if pending.failed {
                    let exception = pending.exception.map(|data| field_to_string(&data));
                    return Err(Error::JobFailed(JobFailure::new(
                        handle.as_str(),
                        exception,
                    )));
                }
                return Ok(JobResult {
                    handle: handle.clone(),
                    data: pending.completed.unwrap_or_default(),
                    streamed_data: pending.streamed_data,
                    warnings: pending.warnings,
                    status: pending.status,
                });
            }

            let packet = self.connection.recv().await?;
            let kind = packet.kind;
            if kind == PacketType::Error {
                return Err(protocol::server_error(packet.data));
            }
            if !self.route_job_event(packet)? {
                return Err(Error::UnexpectedPacket {
                    expected: "job result",
                    got: kind,
                });
            }
        }
    }

    /// Queries the status of a job by handle.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("job_status");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg(function, b"payload").await?;
    /// let status = client.job_status(&handle).await?;
    /// assert!(status.known);
    /// assert!(!status.running);
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn job_status(&mut self, handle: &JobHandle) -> Result<JobStatus> {
        self.connection
            .send(PacketType::GetStatus, handle.as_str().as_bytes())
            .await?;
        loop {
            let packet = self.connection.recv().await?;
            let kind = packet.kind;
            match kind {
                PacketType::StatusRes => return parse_status(&packet.data),
                PacketType::Error => return Err(protocol::server_error(packet.data)),
                _ if self.route_job_event(packet)? => {}
                _ => {
                    return Err(Error::UnexpectedPacket {
                        expected: "STATUS_RES",
                        got: kind,
                    });
                }
            }
        }
    }

    /// Queries the status of a job by unique id.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// let unique = "job-1";
    /// # let function = gearman_rs::_test_support::unique("job_status_unique");
    /// # let unique = gearman_rs::_test_support::unique("job_status_unique_id");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg_unique(function, unique.clone(), b"payload").await?;
    /// let status = client.job_status_unique(&unique).await?;
    /// assert!(status.known);
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn job_status_unique(&mut self, unique: &str) -> Result<JobStatus> {
        ensure_no_nul(unique, "unique id")?;
        self.connection
            .send(PacketType::GetStatusUnique, unique.as_bytes())
            .await?;
        loop {
            let packet = self.connection.recv().await?;
            let kind = packet.kind;
            match kind {
                PacketType::StatusResUnique => return parse_unique_status(&packet.data),
                PacketType::Error => return Err(protocol::server_error(packet.data)),
                _ if self.route_job_event(packet)? => {}
                _ => {
                    return Err(Error::UnexpectedPacket {
                        expected: "STATUS_RES_UNIQUE",
                        got: kind,
                    });
                }
            }
        }
    }

    /// Requests a connection option such as `"exceptions"`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// client.option("exceptions").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn option(&mut self, option: &str) -> Result<()> {
        ensure_no_nul(option, "option name")?;
        self.connection
            .send(PacketType::OptionReq, option.as_bytes())
            .await?;
        loop {
            let packet = self.connection.recv().await?;
            let kind = packet.kind;
            match kind {
                PacketType::OptionRes => return Ok(()),
                PacketType::Error => return Err(protocol::server_error(packet.data)),
                _ if self.route_job_event(packet)? => {}
                _ => {
                    return Err(Error::UnexpectedPacket {
                        expected: "OPTION_RES",
                        got: kind,
                    });
                }
            }
        }
    }

    /// Returns whether the server accepts a connection option.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// assert!(client.supports_option("exceptions").await?);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn supports_option(&mut self, option: &str) -> Result<bool> {
        match self.option(option).await {
            Ok(()) => Ok(true),
            Err(Error::Server { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Sends an `ECHO_REQ` and returns the echoed bytes.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// assert_eq!(client.echo(b"ping").await?, b"ping");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn echo(&mut self, data: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.connection
            .send(PacketType::EchoReq, data.as_ref())
            .await?;
        loop {
            let packet = self.connection.recv().await?;
            let kind = packet.kind;
            match kind {
                PacketType::EchoRes => return Ok(packet.data),
                PacketType::Error => return Err(protocol::server_error(packet.data)),
                _ if self.route_job_event(packet)? => {}
                _ => {
                    return Err(Error::UnexpectedPacket {
                        expected: "ECHO_RES",
                        got: kind,
                    });
                }
            }
        }
    }

    /// Sends a low-level packet.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, PacketType};
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// client.send_packet(PacketType::EchoReq, b"ping").await?;
    /// let packet = client.receive_packet().await?;
    /// assert_eq!(packet.kind, PacketType::EchoRes);
    /// assert_eq!(packet.data, b"ping");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_packet(&mut self, kind: PacketType, data: &[u8]) -> Result<()> {
        self.connection.send(kind, data).await
    }

    /// Receives a low-level packet.
    pub async fn receive_packet(&mut self) -> Result<Packet> {
        self.connection.recv().await
    }

    /// Changes the read timeout used by subsequent operations.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.connection.set_read_timeout(timeout);
    }

    /// Changes the write timeout used by subsequent operations.
    pub fn set_write_timeout(&mut self, timeout: Option<Duration>) {
        self.connection.set_write_timeout(timeout);
    }

    /// Changes the maximum accepted packet size.
    pub fn set_max_packet_size(&mut self, max_packet_size: usize) {
        self.connection.set_max_packet_size(max_packet_size);
    }

    /// Returns a reference to the underlying TCP stream.
    ///
    /// When the connection uses TLS this is the raw encrypted transport.
    /// Reading from or writing to it directly bypasses TLS and corrupts the
    /// connection.
    pub fn stream(&self) -> &tokio::net::TcpStream {
        self.connection.stream()
    }

    /// Returns a mutable reference to the underlying TCP stream.
    ///
    /// When the connection uses TLS, writing to this stream bypasses TLS and
    /// corrupts the connection. Use it only to inspect or change socket
    /// options; check [`Self::is_tls`] first when in doubt.
    pub fn stream_mut(&mut self) -> &mut tokio::net::TcpStream {
        self.connection.stream_mut()
    }

    /// Returns `true` when the connection is encrypted with TLS.
    pub fn is_tls(&self) -> bool {
        self.connection.is_tls()
    }

    /// Shuts down the TCP connection.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncClient;
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// client.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(&mut self) -> Result<()> {
        self.connection.shutdown().await
    }

    async fn read_job_created(&mut self) -> Result<JobHandle> {
        loop {
            let packet = self.connection.recv().await?;
            let kind = packet.kind;
            match kind {
                PacketType::JobCreated => {
                    let handle = field_to_string_trim(&packet.data);
                    if handle.is_empty() {
                        return Err(Error::Protocol(
                            "server returned an empty handle".to_owned(),
                        ));
                    }
                    return Ok(JobHandle::new(handle));
                }
                PacketType::Error => return Err(protocol::server_error(packet.data)),
                _ if self.route_job_event(packet)? => {}
                _ => {
                    return Err(Error::UnexpectedPacket {
                        expected: "JOB_CREATED",
                        got: kind,
                    });
                }
            }
        }
    }

    fn route_job_event(&mut self, packet: Packet) -> Result<bool> {
        let kind = packet.kind;
        match kind {
            PacketType::WorkData => {
                let fields = split_fields::<2>(&packet.data)?;
                let handle = field_to_string_trim(fields[0]);
                if let Some(pending) = self.pending.get_mut(&handle) {
                    pending.streamed_data.push(fields[1].to_vec());
                }
                Ok(true)
            }
            PacketType::WorkWarning => {
                let fields = split_fields::<2>(&packet.data)?;
                let handle = field_to_string_trim(fields[0]);
                if let Some(pending) = self.pending.get_mut(&handle) {
                    pending.warnings.push(fields[1].to_vec());
                }
                Ok(true)
            }
            PacketType::WorkException => {
                let fields = split_fields::<2>(&packet.data)?;
                let handle = field_to_string_trim(fields[0]);
                if let Some(pending) = self.pending.get_mut(&handle) {
                    pending.exception = Some(fields[1].to_vec());
                    // With the "exceptions" option enabled the server sends
                    // WORK_EXCEPTION instead of WORK_FAIL.
                    pending.failed = true;
                }
                Ok(true)
            }
            PacketType::WorkStatus => {
                let fields = split_fields::<3>(&packet.data)?;
                let handle = field_to_string_trim(fields[0]);
                let numerator = parse_u32(fields[1], "status numerator")?;
                let denominator = parse_u32(fields[2], "status denominator")?;
                if let Some(pending) = self.pending.get_mut(&handle) {
                    pending.status = Some(JobStatus {
                        id: handle,
                        known: true,
                        running: true,
                        numerator,
                        denominator,
                        clients: None,
                    });
                }
                Ok(true)
            }
            PacketType::WorkComplete => {
                let fields = split_fields::<2>(&packet.data)?;
                let handle = field_to_string_trim(fields[0]);
                if let Some(pending) = self.pending.get_mut(&handle) {
                    pending.completed = Some(fields[1].to_vec());
                }
                Ok(true)
            }
            PacketType::WorkFail => {
                let handle = field_to_string_trim(trim_trailing_nul(&packet.data));
                if let Some(pending) = self.pending.get_mut(&handle) {
                    pending.failed = true;
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}
