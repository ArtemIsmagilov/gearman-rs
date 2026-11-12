//! Asynchronous Gearman worker.

use std::collections::HashSet;
use std::ops::AsyncFnMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::ToSocketAddrs;

use tokio::sync::Mutex;

use crate::client::JobHandle;
use crate::error::{Error, Result};
use crate::net::Config;
use crate::protocol::{
    self, Packet, PacketType, ensure_no_nul, field_to_string, field_to_string_trim, split_fields,
};
use crate::worker::Job;

use super::connection::Connection;

/// Builder for [`Worker`].
#[derive(Debug, Clone, Default)]
pub struct WorkerBuilder {
    config: Config,
}

impl WorkerBuilder {
    /// Creates a builder with default connection settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the timeout for the TCP connect and TLS handshake.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = Some(timeout);
        self
    }

    /// Sets the read timeout.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.config.read_timeout = Some(timeout);
        self
    }

    /// Sets the write timeout.
    pub fn write_timeout(mut self, timeout: Duration) -> Self {
        self.config.write_timeout = Some(timeout);
        self
    }

    /// Enables or disables `TCP_NODELAY`.
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
    pub async fn connect<A: ToSocketAddrs>(self, address: A) -> Result<Worker> {
        Worker::with_config(address, self.config).await
    }
}

/// An asynchronous Gearman worker.
pub struct Worker {
    connection: Arc<Mutex<Connection>>,
    abilities: HashSet<String>,
    client_id: Option<String>,
}

impl Worker {
    /// Returns a builder for configuring the connection.
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    /// Connects to a Gearman server with default settings.
    pub async fn connect<A: ToSocketAddrs>(address: A) -> Result<Self> {
        Self::with_config(address, Config::default()).await
    }

    async fn with_config<A: ToSocketAddrs>(address: A, config: Config) -> Result<Self> {
        Ok(Self {
            connection: Arc::new(Mutex::new(Connection::connect(address, &config).await?)),
            abilities: HashSet::new(),
            client_id: None,
        })
    }

    /// Registers a function with `CAN_DO`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncWorker;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("register");
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// assert!(worker.abilities().any(|name| name == function.as_str()));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn register(&mut self, function: impl AsRef<str>) -> Result<()> {
        let function = function.as_ref();
        ensure_function(function)?;
        if self.abilities.contains(function) {
            return Ok(());
        }
        self.send(PacketType::CanDo, function.as_bytes()).await?;
        self.abilities.insert(function.to_owned());
        Ok(())
    }

    /// Registers a function with an execution timeout.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use std::time::Duration;
    /// use gearman_rs::AsyncWorker;
    /// let function = "transcode";
    /// # let function = gearman_rs::_test_support::unique("register_with_timeout");
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register_with_timeout(&function, Duration::from_secs(30)).await?;
    /// assert!(worker.abilities().any(|name| name == function.as_str()));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn register_with_timeout(
        &mut self,
        function: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<()> {
        let function = function.as_ref();
        ensure_function(function)?;
        let millis = u64::try_from(timeout.as_millis()).map_err(|_| {
            Error::InvalidArgument("timeout is too large for the Gearman protocol".to_owned())
        })?;
        let mut payload = Vec::with_capacity(function.len() + 24);
        payload.extend_from_slice(function.as_bytes());
        payload.push(0);
        payload.extend_from_slice(millis.to_string().as_bytes());
        self.send(PacketType::CanDoTimeout, &payload).await?;
        self.abilities.insert(function.to_owned());
        Ok(())
    }

    /// Unregisters a function with `CANT_DO`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncWorker;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("unregister");
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// worker.unregister(&function).await?;
    /// assert!(!worker.abilities().any(|name| name == function.as_str()));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unregister(&mut self, function: impl AsRef<str>) -> Result<()> {
        let function = function.as_ref();
        ensure_function(function)?;
        self.send(PacketType::CantDo, function.as_bytes()).await?;
        self.abilities.remove(function);
        Ok(())
    }

    /// Removes every registered function.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncWorker;
    /// # let first = gearman_rs::_test_support::unique("reset_abilities_a");
    /// # let second = gearman_rs::_test_support::unique("reset_abilities_b");
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&first).await?;
    /// worker.register(&second).await?;
    /// worker.reset_abilities().await?;
    /// assert!(worker.abilities().next().is_none());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn reset_abilities(&mut self) -> Result<()> {
        self.send(PacketType::ResetAbilities, &[]).await?;
        self.abilities.clear();
        Ok(())
    }

    /// Sets the worker's client id.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncWorker;
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.set_client_id("doc-worker").await?;
    /// assert_eq!(worker.client_id(), Some("doc-worker"));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_client_id(&mut self, client_id: impl AsRef<str>) -> Result<()> {
        let client_id = client_id.as_ref();
        ensure_no_nul(client_id, "client id")?;
        if client_id.chars().any(char::is_whitespace) {
            return Err(Error::InvalidArgument(
                "client id must not contain whitespace".to_owned(),
            ));
        }
        self.send(PacketType::SetClientId, client_id.as_bytes())
            .await?;
        self.client_id = Some(client_id.to_owned());
        Ok(())
    }

    /// Sends `ALL_YOURS`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncWorker;
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.all_yours().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn all_yours(&mut self) -> Result<()> {
        self.send(PacketType::AllYours, &[]).await
    }

    /// Returns the registered function names.
    pub fn abilities(&self) -> impl Iterator<Item = &str> {
        self.abilities.iter().map(String::as_str)
    }

    /// Returns the configured client id.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Sends `GRAB_JOB` and returns the next assignment or `None`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("grab_job");
    /// # let worker_function = function.clone();
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         assert_eq!(job.function, worker_function);
    /// #         worker.send_work_complete(&job.handle, job.workload().to_vec()).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let result = client.execute(function, b"hello").await?;
    /// assert_eq!(result.data, b"hello");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn grab_job(&self) -> Result<Option<Job>> {
        self.grab(PacketType::GrabJob).await
    }

    /// Sends `GRAB_JOB_UNIQ` and returns the next assignment or `None`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// let unique = "request-1";
    /// # let function = gearman_rs::_test_support::unique("grab_job_unique");
    /// # let unique = gearman_rs::_test_support::unique("grab_job_unique_id");
    /// # let worker_unique = unique.clone();
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job_unique().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         assert_eq!(job.unique.as_deref(), Some(worker_unique.as_str()));
    /// #         worker.send_work_complete(&job.handle, b"unique").await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_unique(function, unique, b"hello").await?;
    /// assert_eq!(client.wait(&handle).await?.data, b"unique");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn grab_job_unique(&self) -> Result<Option<Job>> {
        self.grab(PacketType::GrabJobUniq).await
    }

    /// Sends `GRAB_JOB_ALL` and returns the next assignment or `None`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "reduce-sum";
    /// let unique = "reduce-1";
    /// # let function = gearman_rs::_test_support::unique("grab_job_all");
    /// # let unique = gearman_rs::_test_support::unique("grab_job_all_id");
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
    pub async fn grab_job_all(&self) -> Result<Option<Job>> {
        self.grab(PacketType::GrabJobAll).await
    }

    /// Sends `PRE_SLEEP` and waits for the server to wake the worker.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("pre_sleep");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         worker.pre_sleep().await?;
    /// #         let job = worker.grab_job().await?.expect("job after NOOP");
    /// #         worker.send_work_complete(&job.handle, b"awake").await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// assert_eq!(client.execute(function, b"hello").await?.data, b"awake");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn pre_sleep(&self) -> Result<()> {
        let mut connection = self.connection.lock().await;
        connection.send(PacketType::PreSleep, &[]).await?;
        let packet = connection.recv().await?;
        let kind = packet.kind;
        match kind {
            PacketType::Noop => Ok(()),
            PacketType::Error => Err(protocol::server_error(packet.data)),
            _ => Err(Error::UnexpectedPacket {
                expected: "NOOP",
                got: kind,
            }),
        }
    }

    /// Sends an `ECHO_REQ` and returns the echoed bytes.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncWorker;
    /// let worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// assert_eq!(worker.echo(b"ping").await?, b"ping");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn echo(&self, data: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        let mut connection = self.connection.lock().await;
        connection.send(PacketType::EchoReq, data.as_ref()).await?;
        let packet = connection.recv().await?;
        match packet.kind {
            PacketType::EchoRes => Ok(packet.data),
            PacketType::Error => Err(protocol::server_error(packet.data)),
            kind => Err(Error::UnexpectedPacket {
                expected: "ECHO_RES",
                got: kind,
            }),
        }
    }

    /// Requests a connection option.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncWorker;
    /// let worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.option("exceptions").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn option(&self, option: &str) -> Result<()> {
        ensure_no_nul(option, "option name")?;
        let mut connection = self.connection.lock().await;
        connection
            .send(PacketType::OptionReq, option.as_bytes())
            .await?;
        let packet = connection.recv().await?;
        match packet.kind {
            PacketType::OptionRes => Ok(()),
            PacketType::Error => Err(protocol::server_error(packet.data)),
            kind => Err(Error::UnexpectedPacket {
                expected: "OPTION_RES",
                got: kind,
            }),
        }
    }

    /// Sends intermediate job data.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_data");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         worker.send_work_data(&job.handle, b"chunk").await?;
    /// #         worker.send_work_complete(&job.handle, b"done").await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job(function, b"hello").await?;
    /// let result = client.wait(&handle).await?;
    /// assert_eq!(result.streamed_data, vec![b"chunk".to_vec()]);
    /// assert_eq!(result.data, b"done");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_work_data(&self, handle: &JobHandle, data: impl AsRef<[u8]>) -> Result<()> {
        self.send_handle_payload(PacketType::WorkData, handle, data.as_ref())
            .await
    }

    /// Sends a job warning.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_warning");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         worker.send_work_warning(&job.handle, b"careful").await?;
    /// #         worker.send_work_complete(&job.handle, b"done").await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job(function, b"hello").await?;
    /// let result = client.wait(&handle).await?;
    /// assert_eq!(result.warnings, vec![b"careful".to_vec()]);
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_work_warning(
        &self,
        handle: &JobHandle,
        data: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.send_handle_payload(PacketType::WorkWarning, handle, data.as_ref())
            .await
    }

    /// Sends a job exception.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker, Error};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_exception");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         worker.send_work_exception(&job.handle, b"boom").await?;
    /// #         worker.send_work_fail(&job.handle).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// client.option("exceptions").await?;
    /// let handle = client.submit_job(function, b"hello").await?;
    /// match client.wait(&handle).await {
    ///     Err(Error::JobFailed(failure)) => {
    ///         assert_eq!(failure.exception.as_deref(), Some("boom"));
    ///     }
    ///     other => panic!("expected a job failure, got {other:?}"),
    /// }
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_work_exception(
        &self,
        handle: &JobHandle,
        data: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.send_handle_payload(PacketType::WorkException, handle, data.as_ref())
            .await
    }

    /// Sends progress information.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_status");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         worker.send_work_status(&job.handle, 1, 2).await?;
    /// #         worker.send_work_complete(&job.handle, b"done").await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job(function, b"hello").await?;
    /// let result = client.wait(&handle).await?;
    /// let status = result.status.expect("progress status");
    /// assert_eq!((status.numerator, status.denominator), (1, 2));
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_work_status(
        &self,
        handle: &JobHandle,
        numerator: u32,
        denominator: u32,
    ) -> Result<()> {
        let mut payload = Vec::with_capacity(handle.as_str().len() + 24);
        payload.extend_from_slice(handle.as_str().as_bytes());
        payload.push(0);
        payload.extend_from_slice(numerator.to_string().as_bytes());
        payload.push(0);
        payload.extend_from_slice(denominator.to_string().as_bytes());
        self.send(PacketType::WorkStatus, &payload).await
    }

    /// Completes a job with a result.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_complete");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         worker.send_work_complete(&job.handle, b"done").await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job(function, b"hello").await?;
    /// let result = client.wait(&handle).await?;
    /// assert_eq!(result.data, b"done");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_work_complete(
        &self,
        handle: &JobHandle,
        data: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.send_handle_payload(PacketType::WorkComplete, handle, data.as_ref())
            .await
    }

    /// Marks a job as failed.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker, Error};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_fail");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |worker| async move {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job().await? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep().await?;
    /// #         };
    /// #         worker.send_work_fail(&job.handle).await
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job(function, b"hello").await?;
    /// assert!(matches!(client.wait(&handle).await, Err(Error::JobFailed(_))));
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_work_fail(&self, handle: &JobHandle) -> Result<()> {
        self.send(PacketType::WorkFail, handle.as_str().as_bytes())
            .await
    }

    /// Waits for the next job, sleeping between attempts.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("next_job");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_async_worker(
    /// #     function.clone(),
    /// #     |mut worker| async move {
    /// #         let mut job = worker.next_job().await?;
    /// #         job.complete(job.workload().to_vec()).await;
    /// #         Ok(())
    /// #     },
    /// # );
    /// # ready.await.unwrap();
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// assert_eq!(client.execute(function, b"hello").await?.data, b"hello");
    /// # worker.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_job(&mut self) -> Result<WorkerJob> {
        loop {
            if let Some(job) = self.grab_job().await? {
                return Ok(WorkerJob::new(self.connection.clone(), job));
            }
            self.pre_sleep().await?;
        }
    }

    /// Processes jobs forever using an async closure.
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), gearman_rs::Error> {
    /// let mut worker = gearman_rs::AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register("reverse").await?;
    /// worker.run(async |job| {
    ///     let mut data = job.workload().to_vec();
    ///     data.reverse();
    ///     job.complete(data).await
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("run");
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// let task = tokio::spawn(async move {
    ///     worker.run(async |job| {
    ///         let mut data = job.workload().to_vec();
    ///         data.reverse();
    ///         job.complete(data).await
    ///     }).await
    /// });
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// assert_eq!(client.execute(function, b"hello").await?.data, b"olleh");
    /// task.abort();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run<F>(&mut self, mut handler: F) -> Result<()>
    where
        F: AsyncFnMut(&mut WorkerJob) -> Result<()>,
    {
        loop {
            let mut job = self.next_job().await?;
            let handler_result = handler(&mut job).await;

            if !job.is_finished() {
                let fail_result = job.fail().await;
                if handler_result.is_ok() {
                    fail_result?;
                }
            }

            handler_result?;
        }
    }

    /// Sends a low-level packet.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncWorker, PacketType};
    /// let worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.send_packet(PacketType::EchoReq, b"ping").await?;
    /// let packet = worker.receive_packet().await?;
    /// assert_eq!(packet.kind, PacketType::EchoRes);
    /// assert_eq!(packet.data, b"ping");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_packet(&self, kind: PacketType, data: &[u8]) -> Result<()> {
        self.send(kind, data).await
    }

    /// Receives a low-level packet.
    pub async fn receive_packet(&self) -> Result<Packet> {
        self.connection.lock().await.recv().await
    }

    /// Changes the read timeout used by subsequent operations.
    pub async fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.connection.lock().await.set_read_timeout(timeout);
    }

    /// Changes the write timeout used by subsequent operations.
    pub async fn set_write_timeout(&mut self, timeout: Option<Duration>) {
        self.connection.lock().await.set_write_timeout(timeout);
    }

    /// Changes the maximum accepted packet size.
    pub async fn set_max_packet_size(&mut self, max_packet_size: usize) {
        self.connection
            .lock()
            .await
            .set_max_packet_size(max_packet_size);
    }

    async fn send(&self, kind: PacketType, data: &[u8]) -> Result<()> {
        self.connection.lock().await.send(kind, data).await
    }

    async fn grab(&self, request: PacketType) -> Result<Option<Job>> {
        let mut connection = self.connection.lock().await;
        connection.send(request, &[]).await?;
        loop {
            let packet = connection.recv().await?;
            let kind = packet.kind;
            match kind {
                PacketType::JobAssign => return Ok(Some(parse_job_assign(packet.data)?)),
                PacketType::JobAssignUniq => {
                    return Ok(Some(parse_job_assign_unique(packet.data)?));
                }
                PacketType::JobAssignAll => return Ok(Some(parse_job_assign_all(packet.data)?)),
                PacketType::NoJob => return Ok(None),
                PacketType::Noop => continue,
                PacketType::Error => return Err(protocol::server_error(packet.data)),
                _ => {
                    return Err(Error::UnexpectedPacket {
                        expected: "JOB_ASSIGN or NO_JOB",
                        got: kind,
                    });
                }
            }
        }
    }

    async fn send_handle_payload(
        &self,
        kind: PacketType,
        handle: &JobHandle,
        data: &[u8],
    ) -> Result<()> {
        let mut payload = Vec::with_capacity(handle.as_str().len() + data.len() + 1);
        payload.extend_from_slice(handle.as_str().as_bytes());
        payload.push(0);
        payload.extend_from_slice(data);
        self.send(kind, &payload).await
    }
}

/// A job currently being processed by an asynchronous [`Worker`].
pub struct WorkerJob {
    connection: Arc<Mutex<Connection>>,
    job: Job,
    finished: bool,
}

impl WorkerJob {
    fn new(connection: Arc<Mutex<Connection>>, job: Job) -> Self {
        Self {
            connection,
            job,
            finished: false,
        }
    }

    /// Returns the job handle.
    pub fn handle(&self) -> &JobHandle {
        &self.job.handle
    }

    /// Returns the function name.
    pub fn function(&self) -> &str {
        &self.job.function
    }

    /// Returns the unique id, if any.
    pub fn unique(&self) -> Option<&str> {
        self.job.unique.as_deref()
    }

    /// Returns the reducer name, if any.
    pub fn reducer(&self) -> Option<&str> {
        self.job.reducer.as_deref()
    }

    /// Returns the workload.
    pub fn workload(&self) -> &[u8] {
        &self.job.workload
    }

    /// Returns the underlying assignment.
    pub fn assignment(&self) -> &Job {
        &self.job
    }

    /// Returns `true` after a final packet was sent.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Sends intermediate data to the client.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_send_data");
    /// # let client_function = function.clone();
    /// # let client = tokio::spawn(async move {
    /// #     let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// #     let handle = client.submit_job(client_function, b"hello").await?;
    /// #     client.wait(&handle).await?;
    /// #     Ok::<(), gearman_rs::Error>(())
    /// # });
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// let mut job = worker.next_job().await?;
    /// job.send_data(b"chunk").await?;
    /// job.complete(b"done").await;
    /// # client.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_data(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.send_handle_payload(PacketType::WorkData, data.as_ref())
            .await
    }

    /// Sends a warning to the client.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_send_warning");
    /// # let client_function = function.clone();
    /// # let client = tokio::spawn(async move {
    /// #     let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// #     let handle = client.submit_job(client_function, b"hello").await?;
    /// #     client.wait(&handle).await?;
    /// #     Ok::<(), gearman_rs::Error>(())
    /// # });
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// let mut job = worker.next_job().await?;
    /// job.send_warning(b"careful").await?;
    /// job.complete(b"done").await;
    /// # client.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_warning(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.send_handle_payload(PacketType::WorkWarning, data.as_ref())
            .await
    }

    /// Sends progress information.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_send_status");
    /// # let client_function = function.clone();
    /// # let client = tokio::spawn(async move {
    /// #     let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// #     let handle = client.submit_job(client_function, b"hello").await?;
    /// #     client.wait(&handle).await?;
    /// #     Ok::<(), gearman_rs::Error>(())
    /// # });
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// let mut job = worker.next_job().await?;
    /// job.send_status(1, 2).await?;
    /// job.complete(b"done").await;
    /// # client.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_status(&mut self, numerator: u32, denominator: u32) -> Result<()> {
        self.ensure_active()?;
        let mut payload = Vec::with_capacity(self.job.handle.as_str().len() + 24);
        payload.extend_from_slice(self.job.handle.as_str().as_bytes());
        payload.push(0);
        payload.extend_from_slice(numerator.to_string().as_bytes());
        payload.push(0);
        payload.extend_from_slice(denominator.to_string().as_bytes());
        self.send(PacketType::WorkStatus, &payload).await
    }

    /// Sends an exception without finishing the job.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_send_exception");
    /// # let client_function = function.clone();
    /// # let client = tokio::spawn(async move {
    /// #     let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// #     client.option("exceptions").await?;
    /// #     let handle = client.submit_job(client_function, b"hello").await?;
    /// #     assert!(matches!(client.wait(&handle).await, Err(gearman_rs::Error::JobFailed(_))));
    /// #     Ok::<(), gearman_rs::Error>(())
    /// # });
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// let mut job = worker.next_job().await?;
    /// job.send_exception(b"boom").await?;
    /// job.fail().await;
    /// # client.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_exception(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.send_handle_payload(PacketType::WorkException, data.as_ref())
            .await
    }

    /// Completes the job with a result.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_complete");
    /// # let client_function = function.clone();
    /// # let client = tokio::spawn(async move {
    /// #     let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// #     let handle = client.submit_job(client_function, b"hello").await?;
    /// #     client.wait(&handle).await?;
    /// #     Ok::<(), gearman_rs::Error>(())
    /// # });
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// let mut job = worker.next_job().await?;
    /// job.complete(job.workload().to_vec()).await;
    /// # client.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn complete(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.send_handle_payload(PacketType::WorkComplete, data.as_ref())
            .await?;
        self.finished = true;
        Ok(())
    }

    /// Marks the job as failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_fail");
    /// # let client_function = function.clone();
    /// # let client = tokio::spawn(async move {
    /// #     let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// #     let handle = client.submit_job(client_function, b"hello").await?;
    /// #     assert!(matches!(client.wait(&handle).await, Err(gearman_rs::Error::JobFailed(_))));
    /// #     Ok::<(), gearman_rs::Error>(())
    /// # });
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// let mut job = worker.next_job().await?;
    /// job.fail().await;
    /// # client.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fail(&mut self) -> Result<()> {
        self.ensure_active()?;
        self.send(PacketType::WorkFail, self.job.handle.as_str().as_bytes())
            .await?;
        self.finished = true;
        Ok(())
    }

    /// Sends an exception and marks the job as failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncClient, AsyncWorker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_fail_with_exception");
    /// # let client_function = function.clone();
    /// # let client = tokio::spawn(async move {
    /// #     let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// #     client.option("exceptions").await?;
    /// #     let handle = client.submit_job(client_function, b"hello").await?;
    /// #     assert!(matches!(client.wait(&handle).await, Err(gearman_rs::Error::JobFailed(_))));
    /// #     Ok::<(), gearman_rs::Error>(())
    /// # });
    /// let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    /// worker.register(&function).await?;
    /// let mut job = worker.next_job().await?;
    /// job.fail_with_exception(b"boom").await;
    /// # client.await.unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fail_with_exception(&mut self, exception: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.send_handle_payload(PacketType::WorkException, exception.as_ref())
            .await?;
        self.send(PacketType::WorkFail, self.job.handle.as_str().as_bytes())
            .await?;
        self.finished = true;
        Ok(())
    }

    async fn send_handle_payload(&self, kind: PacketType, data: &[u8]) -> Result<()> {
        let mut payload = Vec::with_capacity(self.job.handle.as_str().len() + data.len() + 1);
        payload.extend_from_slice(self.job.handle.as_str().as_bytes());
        payload.push(0);
        payload.extend_from_slice(data);
        self.send(kind, &payload).await
    }

    async fn send(&self, kind: PacketType, data: &[u8]) -> Result<()> {
        self.connection.lock().await.send(kind, data).await
    }

    fn ensure_active(&self) -> Result<()> {
        if self.finished {
            return Err(Error::JobAlreadyFinished {
                handle: self.job.handle.to_string(),
            });
        }
        Ok(())
    }
}

fn ensure_function(function: &str) -> Result<()> {
    if function.is_empty() {
        return Err(Error::InvalidArgument(
            "function name must not be empty".to_owned(),
        ));
    }
    ensure_no_nul(function, "function name")
}

fn parse_job_assign(data: Vec<u8>) -> Result<Job> {
    let fields = split_fields::<3>(&data)?;
    Ok(Job {
        handle: JobHandle::new(field_to_string_trim(fields[0])),
        function: field_to_string(fields[1]),
        unique: None,
        reducer: None,
        workload: fields[2].to_vec(),
    })
}

fn parse_job_assign_unique(data: Vec<u8>) -> Result<Job> {
    let fields = split_fields::<4>(&data)?;
    let unique = field_to_string(fields[2]);
    Ok(Job {
        handle: JobHandle::new(field_to_string_trim(fields[0])),
        function: field_to_string(fields[1]),
        unique: (!unique.is_empty()).then_some(unique),
        reducer: None,
        workload: fields[3].to_vec(),
    })
}

fn parse_job_assign_all(data: Vec<u8>) -> Result<Job> {
    let fields = split_fields::<5>(&data)?;
    let unique = field_to_string(fields[2]);
    let reducer = field_to_string(fields[3]);
    Ok(Job {
        handle: JobHandle::new(field_to_string_trim(fields[0])),
        function: field_to_string(fields[1]),
        unique: (!unique.is_empty()).then_some(unique),
        reducer: (!reducer.is_empty()).then_some(reducer),
        workload: fields[4].to_vec(),
    })
}
