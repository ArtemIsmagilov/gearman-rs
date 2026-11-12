//! Synchronous Gearman worker.

use std::collections::HashSet;
use std::fmt;
use std::net::ToSocketAddrs;
use std::time::Duration;

use crate::client::JobHandle;
use crate::error::{Error, Result};
use crate::net::{Config, Connection};
use crate::protocol::{
    self, Packet, PacketType, ensure_no_nul, field_to_string, field_to_string_trim, split_fields,
};

/// A job assignment received from a Gearman server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    /// Server-assigned job handle.
    pub handle: JobHandle,
    /// Function name.
    pub function: String,
    /// Optional unique id supplied by the client.
    pub unique: Option<String>,
    /// Optional reducer name (only for `GRAB_JOB_ALL`).
    pub reducer: Option<String>,
    /// Opaque workload.
    pub workload: Vec<u8>,
}

impl Job {
    /// Returns the job handle.
    pub fn handle(&self) -> &JobHandle {
        &self.handle
    }

    /// Returns the function name.
    pub fn function(&self) -> &str {
        &self.function
    }

    /// Returns the optional unique id.
    pub fn unique(&self) -> Option<&str> {
        self.unique.as_deref()
    }

    /// Returns the optional reducer name.
    pub fn reducer(&self) -> Option<&str> {
        self.reducer.as_deref()
    }

    /// Returns the workload.
    pub fn workload(&self) -> &[u8] {
        &self.workload
    }
}

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

    /// Sets the read timeout used for blocking reads.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.config.read_timeout = Some(timeout);
        self
    }

    /// Sets the write timeout used for blocking writes.
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
    #[cfg(feature = "tls")]
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
    pub fn connect<A: ToSocketAddrs>(self, address: A) -> Result<Worker> {
        Worker::with_config(address, self.config)
    }
}

/// A synchronous Gearman worker.
pub struct Worker {
    connection: Connection,
    abilities: HashSet<String>,
    client_id: Option<String>,
}

impl Worker {
    /// Returns a builder for configuring the connection.
    pub fn builder() -> WorkerBuilder {
        WorkerBuilder::new()
    }

    /// Connects to a Gearman server with default settings.
    pub fn connect<A: ToSocketAddrs>(address: A) -> Result<Self> {
        Self::with_config(address, Config::default())
    }

    fn with_config<A: ToSocketAddrs>(address: A, config: Config) -> Result<Self> {
        Ok(Self {
            connection: Connection::connect(address, &config)?,
            abilities: HashSet::new(),
            client_id: None,
        })
    }

    /// Registers a function with `CAN_DO`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Worker;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("register");
    /// worker.register(&function)?;
    /// assert!(worker.abilities().any(|name| name == function.as_str()));
    /// # Ok(())
    /// # }
    /// ```
    pub fn register(&mut self, function: impl AsRef<str>) -> Result<()> {
        let function = function.as_ref();
        ensure_function(function)?;
        if self.abilities.contains(function) {
            return Ok(());
        }
        self.connection
            .send(PacketType::CanDo, function.as_bytes())?;
        self.abilities.insert(function.to_owned());
        Ok(())
    }

    /// Registers a function with an execution timeout (`CAN_DO_TIMEOUT`).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use gearman_rs::Worker;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("register_with_timeout");
    /// worker.register_with_timeout(&function, Duration::from_secs(30))?;
    /// assert!(worker.abilities().any(|name| name == function.as_str()));
    /// # Ok(())
    /// # }
    /// ```
    pub fn register_with_timeout(
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
        self.connection.send(PacketType::CanDoTimeout, &payload)?;
        self.abilities.insert(function.to_owned());
        Ok(())
    }

    /// Unregisters a function with `CANT_DO`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Worker;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("unregister");
    /// worker.register(&function)?;
    /// worker.unregister(&function)?;
    /// assert!(!worker.abilities().any(|name| name == function.as_str()));
    /// # Ok(())
    /// # }
    /// ```
    pub fn unregister(&mut self, function: impl AsRef<str>) -> Result<()> {
        let function = function.as_ref();
        ensure_function(function)?;
        self.connection
            .send(PacketType::CantDo, function.as_bytes())?;
        self.abilities.remove(function);
        Ok(())
    }

    /// Removes every registered function with `RESET_ABILITIES`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Worker;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// let first = "reverse";
    /// let second = "transcode";
    /// # let first = gearman_rs::_test_support::unique("reset_abilities_a");
    /// # let second = gearman_rs::_test_support::unique("reset_abilities_b");
    /// worker.register(&first)?;
    /// worker.register(&second)?;
    /// worker.reset_abilities()?;
    /// assert!(worker.abilities().next().is_none());
    /// # Ok(())
    /// # }
    /// ```
    pub fn reset_abilities(&mut self) -> Result<()> {
        self.connection.send(PacketType::ResetAbilities, &[])?;
        self.abilities.clear();
        Ok(())
    }

    /// Sets the worker's client id.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Worker;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.set_client_id("doc-worker")?;
    /// assert_eq!(worker.client_id(), Some("doc-worker"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_client_id(&mut self, client_id: impl AsRef<str>) -> Result<()> {
        let client_id = client_id.as_ref();
        ensure_no_nul(client_id, "client id")?;
        if client_id.chars().any(char::is_whitespace) {
            return Err(Error::InvalidArgument(
                "client id must not contain whitespace".to_owned(),
            ));
        }
        self.connection
            .send(PacketType::SetClientId, client_id.as_bytes())?;
        self.client_id = Some(client_id.to_owned());
        Ok(())
    }

    /// Tells the server that this worker is its only connection (`ALL_YOURS`).
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Worker;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.all_yours()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn all_yours(&mut self) -> Result<()> {
        self.connection.send(PacketType::AllYours, &[])?;
        Ok(())
    }

    /// Returns the registered function names.
    pub fn abilities(&self) -> impl Iterator<Item = &str> {
        self.abilities.iter().map(String::as_str)
    }

    /// Returns the configured client id, if any.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Sends `GRAB_JOB` and returns the next assignment or `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("grab_job");
    /// let worker_function = function.clone();
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     move |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         assert_eq!(job.function, worker_function);
    /// #         worker.send_work_complete(&job.handle, job.workload().to_vec())
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let result = client.execute(function, b"hello")?;
    /// assert_eq!(result.data, b"hello");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn grab_job(&mut self) -> Result<Option<Job>> {
        self.grab(PacketType::GrabJob)
    }

    /// Sends `GRAB_JOB_UNIQ` and returns the next assignment or `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("grab_job_unique");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("grab_job_unique_id");
    /// let worker_unique = unique.clone();
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     move |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job_unique()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         assert_eq!(job.unique.as_deref(), Some(worker_unique.as_str()));
    /// #         worker.send_work_complete(&job.handle, b"unique")
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_unique(function, unique, b"hello")?;
    /// assert_eq!(client.wait(&handle)?.data, b"unique");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn grab_job_unique(&mut self) -> Result<Option<Job>> {
        self.grab(PacketType::GrabJobUniq)
    }

    /// Sends `GRAB_JOB_ALL` and returns the next assignment or `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reduce-sum";
    /// # let function = gearman_rs::_test_support::unique("grab_job_all");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("grab_job_all_id");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job_all()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         assert_eq!(job.reducer.as_deref(), Some("sum"));
    /// #         worker.send_work_complete(&job.handle, b"reduced")
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_reduce_job(function, unique, "sum", b"parts")?;
    /// assert_eq!(client.wait(&handle)?.data, b"reduced");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn grab_job_all(&mut self) -> Result<Option<Job>> {
        self.grab(PacketType::GrabJobAll)
    }

    /// Sends `PRE_SLEEP` and blocks until the server wakes the worker.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("pre_sleep");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         worker.pre_sleep()?;
    /// #         let job = worker.grab_job()?.expect("job after NOOP");
    /// #         worker.send_work_complete(&job.handle, b"awake")
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// assert_eq!(client.execute(function, b"hello")?.data, b"awake");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn pre_sleep(&mut self) -> Result<()> {
        self.connection.send(PacketType::PreSleep, &[])?;
        let packet = self.connection.recv()?;
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
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Worker;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// assert_eq!(worker.echo(b"ping")?, b"ping");
    /// # Ok(())
    /// # }
    /// ```
    pub fn echo(&mut self, data: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.connection.send(PacketType::EchoReq, data.as_ref())?;
        let packet = self.connection.recv()?;
        match packet.kind {
            PacketType::EchoRes => Ok(packet.data),
            PacketType::Error => Err(protocol::server_error(packet.data)),
            kind => Err(Error::UnexpectedPacket {
                expected: "ECHO_RES",
                got: kind,
            }),
        }
    }

    /// Requests a connection option such as `"exceptions"`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Worker;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.option("exceptions")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn option(&mut self, option: &str) -> Result<()> {
        ensure_no_nul(option, "option name")?;
        self.connection
            .send(PacketType::OptionReq, option.as_bytes())?;
        let packet = self.connection.recv()?;
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
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_data");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         worker.send_work_data(&job.handle, b"chunk")?;
    /// #         worker.send_work_complete(&job.handle, b"done")
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job(function, b"hello")?;
    /// let result = client.wait(&handle)?;
    /// assert_eq!(result.streamed_data, vec![b"chunk".to_vec()]);
    /// assert_eq!(result.data, b"done");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_work_data(&mut self, handle: &JobHandle, data: impl AsRef<[u8]>) -> Result<()> {
        self.send_handle_payload(PacketType::WorkData, handle, data.as_ref())
    }

    /// Sends a job warning.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_warning");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         worker.send_work_warning(&job.handle, b"careful")?;
    /// #         worker.send_work_complete(&job.handle, b"done")
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job(function, b"hello")?;
    /// let result = client.wait(&handle)?;
    /// assert_eq!(result.warnings, vec![b"careful".to_vec()]);
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_work_warning(&mut self, handle: &JobHandle, data: impl AsRef<[u8]>) -> Result<()> {
        self.send_handle_payload(PacketType::WorkWarning, handle, data.as_ref())
    }

    /// Sends a job exception.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Error, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_exception");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         worker.send_work_exception(&job.handle, b"boom")?;
    /// #         worker.send_work_fail(&job.handle)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// client.option("exceptions")?;
    /// let handle = client.submit_job(function, b"hello")?;
    /// match client.wait(&handle) {
    ///     Err(Error::JobFailed(failure)) => {
    ///         assert_eq!(failure.exception.as_deref(), Some("boom"));
    ///     }
    ///     other => panic!("expected a job failure, got {other:?}"),
    /// }
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_work_exception(
        &mut self,
        handle: &JobHandle,
        data: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.send_handle_payload(PacketType::WorkException, handle, data.as_ref())
    }

    /// Sends progress information.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("send_work_status");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         worker.send_work_status(&job.handle, 1, 2)?;
    /// #         worker.send_work_complete(&job.handle, b"done")
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job(function, b"hello")?;
    /// let result = client.wait(&handle)?;
    /// let status = result.status.expect("progress status");
    /// assert_eq!((status.numerator, status.denominator), (1, 2));
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_work_status(
        &mut self,
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
        self.connection.send(PacketType::WorkStatus, &payload)
    }

    /// Completes a job with a result.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_complete");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         worker.send_work_complete(&job.handle, b"done")
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job(function, b"hello")?;
    /// assert_eq!(client.wait(&handle)?.data, b"done");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_work_complete(&mut self, handle: &JobHandle, data: impl AsRef<[u8]>) -> Result<()> {
        self.send_handle_payload(PacketType::WorkComplete, handle, data.as_ref())
    }

    /// Marks a job as failed.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Error, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("send_work_fail");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         let job = loop {
    /// #             if let Some(job) = worker.grab_job()? {
    /// #                 break job;
    /// #             }
    /// #             worker.pre_sleep()?;
    /// #         };
    /// #         worker.send_work_fail(&job.handle)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job(function, b"hello")?;
    /// assert!(matches!(client.wait(&handle), Err(Error::JobFailed(_))));
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_work_fail(&mut self, handle: &JobHandle) -> Result<()> {
        self.connection
            .send(PacketType::WorkFail, handle.as_str().as_bytes())
    }

    /// Processes jobs forever.
    ///
    /// The handler must finish each job by calling [`WorkerJob::complete`],
    /// [`WorkerJob::fail`] or [`WorkerJob::fail_with_exception`]. If it returns
    /// without finishing the job, the job is marked as failed.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, Worker};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("run");
    /// # let client_function = function.clone();
    /// # let client = std::thread::spawn(move || {
    /// #     std::thread::sleep(Duration::from_millis(200));
    /// #     let result = Client::connect("127.0.0.1:4730")
    /// #         .unwrap()
    /// #         .execute(client_function, b"hello")
    /// #         .unwrap();
    /// #     assert_eq!(result.data, b"hello");
    /// # });
    ///
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.register(function)?;
    /// # worker.set_read_timeout(Some(Duration::from_secs(10)))?;
    ///
    /// // `run` loops forever, so stop it after the first job.
    /// let result = worker.run(|job| {
    ///     job.complete(job.workload().to_vec())?;
    ///     Err(gearman_rs::Error::InvalidArgument("stop".to_owned()))
    /// });
    /// // The error from the handler is returned by `run`.
    /// assert!(result.is_err());
    /// # client.join().unwrap();
    /// # Ok(())
    /// # }
    /// ```
    pub fn run<F>(&mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(&mut WorkerJob<'_>) -> Result<()>,
    {
        loop {
            self.run_once(&mut handler)?;
        }
    }

    /// Waits for, then processes exactly one job.
    /// # Examples
    ///
    /// ```
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{Client, Worker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("run_once");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |worker| {
    /// #         worker.run_once(|job| job.complete(job.workload().to_vec()))
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// assert_eq!(client.execute(function, b"hello")?.data, b"hello");
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn run_once<F>(&mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(&mut WorkerJob<'_>) -> Result<()>,
    {
        let job = loop {
            if let Some(job) = self.grab_job()? {
                break job;
            }
            self.pre_sleep()?;
        };

        let mut worker_job = WorkerJob::new(self, job);
        let handler_result = handler(&mut worker_job);

        if !worker_job.is_finished() {
            let fail_result = worker_job.fail();
            if handler_result.is_ok() {
                fail_result?;
            }
        }

        handler_result
    }

    /// Sends a low-level packet.
    /// # Examples
    ///
    /// ```
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{PacketType, Worker};
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.send_packet(PacketType::EchoReq, b"ping")?;
    /// let packet = worker.receive_packet()?;
    /// assert_eq!(packet.kind, PacketType::EchoRes);
    /// assert_eq!(packet.data, b"ping");
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_packet(&mut self, kind: PacketType, data: &[u8]) -> Result<()> {
        self.connection.send(kind, data)
    }

    /// Receives a low-level packet.
    pub fn receive_packet(&mut self) -> Result<Packet> {
        self.connection.recv()
    }

    /// Changes the read timeout of the underlying socket.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.connection.set_read_timeout(timeout)
    }

    /// Changes the write timeout of the underlying socket.
    pub fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.connection.set_write_timeout(timeout)
    }

    /// Enables or disables `TCP_NODELAY`.
    pub fn set_nodelay(&mut self, nodelay: bool) -> Result<()> {
        self.connection.set_nodelay(nodelay)
    }

    /// Changes the maximum accepted packet size.
    pub fn set_max_packet_size(&mut self, max_packet_size: usize) {
        self.connection.set_max_packet_size(max_packet_size);
    }

    fn grab(&mut self, request: PacketType) -> Result<Option<Job>> {
        self.connection.send(request, &[])?;
        loop {
            let packet = self.connection.recv()?;
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

    fn send_handle_payload(
        &mut self,
        kind: PacketType,
        handle: &JobHandle,
        data: &[u8],
    ) -> Result<()> {
        let mut payload = Vec::with_capacity(handle.as_str().len() + data.len() + 1);
        payload.extend_from_slice(handle.as_str().as_bytes());
        payload.push(0);
        payload.extend_from_slice(data);
        self.connection.send(kind, &payload)
    }
}

/// A job currently being processed by a [`Worker`].
///
/// The wrapper provides high-level methods for streaming data and finishing the
/// job while tracking whether a final packet was already sent.
pub struct WorkerJob<'a> {
    worker: &'a mut Worker,
    job: Job,
    finished: bool,
}

impl<'a> WorkerJob<'a> {
    fn new(worker: &'a mut Worker, job: Job) -> Self {
        Self {
            worker,
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

    /// Returns the unique id, if the job was grabbed with `GRAB_JOB_UNIQ` or
    /// `GRAB_JOB_ALL`.
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
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{Client, Worker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_send_data");
    /// # let client_function = function.clone();
    /// # let client = std::thread::spawn(move || -> gearman_rs::Result<()> {
    /// #     let mut client = Client::connect("127.0.0.1:4730")?;
    /// #     let handle = client.submit_job(client_function, b"hello")?;
    /// #     client.wait(&handle)?;
    /// #     Ok(())
    /// # });
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.register(&function)?;
    /// worker.run_once(|job| {
    ///     job.send_data(b"chunk")?;
    ///     job.complete(b"done")
    /// })?;
    /// # client.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_data(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.worker.send_work_data(&self.job.handle, data)
    }

    /// Sends a warning to the client.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{Client, Worker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_send_warning");
    /// # let client_function = function.clone();
    /// # let client = std::thread::spawn(move || -> gearman_rs::Result<()> {
    /// #     let mut client = Client::connect("127.0.0.1:4730")?;
    /// #     let handle = client.submit_job(client_function, b"hello")?;
    /// #     client.wait(&handle)?;
    /// #     Ok(())
    /// # });
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.register(&function)?;
    /// worker.run_once(|job| {
    ///     job.send_warning(b"careful")?;
    ///     job.complete(b"done")
    /// })?;
    /// # client.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_warning(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.worker.send_work_warning(&self.job.handle, data)
    }

    /// Sends progress information for the job.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{Client, Worker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_send_status");
    /// # let client_function = function.clone();
    /// # let client = std::thread::spawn(move || -> gearman_rs::Result<()> {
    /// #     let mut client = Client::connect("127.0.0.1:4730")?;
    /// #     let handle = client.submit_job(client_function, b"hello")?;
    /// #     client.wait(&handle)?;
    /// #     Ok(())
    /// # });
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.register(&function)?;
    /// worker.run_once(|job| {
    ///     job.send_status(1, 2)?;
    ///     job.complete(b"done")
    /// })?;
    /// # client.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_status(&mut self, numerator: u32, denominator: u32) -> Result<()> {
        self.ensure_active()?;
        self.worker
            .send_work_status(&self.job.handle, numerator, denominator)
    }

    /// Sends a job exception without finishing the job.
    ///
    /// Send `WORK_FAIL` afterwards, or use [`WorkerJob::fail_with_exception`].
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{Client, Worker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_send_exception");
    /// # let client_function = function.clone();
    /// # let client = std::thread::spawn(move || -> gearman_rs::Result<()> {
    /// #     let mut client = Client::connect("127.0.0.1:4730")?;
    /// #     client.option("exceptions")?;
    /// #     let handle = client.submit_job(client_function, b"hello")?;
    /// #     assert!(matches!(client.wait(&handle), Err(gearman_rs::Error::JobFailed(_))));
    /// #     Ok(())
    /// # });
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.register(&function)?;
    /// worker.run_once(|job| {
    ///     job.send_exception(b"boom")?;
    ///     job.fail()
    /// })?;
    /// # client.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_exception(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.worker
            .send_work_exception(&self.job.handle, data.as_ref())
    }

    /// Completes the job with a result.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{Client, Worker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_complete");
    /// # let client_function = function.clone();
    /// # let client = std::thread::spawn(move || -> gearman_rs::Result<()> {
    /// #     let mut client = Client::connect("127.0.0.1:4730")?;
    /// #     let handle = client.submit_job(client_function, b"hello")?;
    /// #     client.wait(&handle)?;
    /// #     Ok(())
    /// # });
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.register(&function)?;
    /// worker.run_once(|job| {
    ///     job.complete(job.workload().to_vec())
    /// })?;
    /// # client.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn complete(&mut self, data: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.worker
            .send_work_complete(&self.job.handle, data.as_ref())?;
        self.finished = true;
        Ok(())
    }

    /// Marks the job as failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{Client, Worker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_fail");
    /// # let client_function = function.clone();
    /// # let client = std::thread::spawn(move || -> gearman_rs::Result<()> {
    /// #     let mut client = Client::connect("127.0.0.1:4730")?;
    /// #     let handle = client.submit_job(client_function, b"hello")?;
    /// #     assert!(matches!(client.wait(&handle), Err(gearman_rs::Error::JobFailed(_))));
    /// #     Ok(())
    /// # });
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.register(&function)?;
    /// worker.run_once(|job| {
    ///     job.fail()
    /// })?;
    /// # client.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn fail(&mut self) -> Result<()> {
        self.ensure_active()?;
        self.worker.send_work_fail(&self.job.handle)?;
        self.finished = true;
        Ok(())
    }

    /// Sends an exception and then marks the job as failed.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{Client, Worker};
    /// let function = "example";
    /// # let function = gearman_rs::_test_support::unique("job_fail_with_exception");
    /// # let client_function = function.clone();
    /// # let client = std::thread::spawn(move || -> gearman_rs::Result<()> {
    /// #     let mut client = Client::connect("127.0.0.1:4730")?;
    /// #     client.option("exceptions")?;
    /// #     let handle = client.submit_job(client_function, b"hello")?;
    /// #     assert!(matches!(client.wait(&handle), Err(gearman_rs::Error::JobFailed(_))));
    /// #     Ok(())
    /// # });
    /// let mut worker = Worker::connect("127.0.0.1:4730")?;
    /// worker.register(&function)?;
    /// worker.run_once(|job| {
    ///     job.fail_with_exception(b"boom")
    /// })?;
    /// # client.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn fail_with_exception(&mut self, exception: impl AsRef<[u8]>) -> Result<()> {
        self.ensure_active()?;
        self.worker
            .send_work_exception(&self.job.handle, exception.as_ref())?;
        self.worker.send_work_fail(&self.job.handle)?;
        self.finished = true;
        Ok(())
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

impl fmt::Debug for WorkerJob<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerJob")
            .field("job", &self.job)
            .field("finished", &self.finished)
            .finish()
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
