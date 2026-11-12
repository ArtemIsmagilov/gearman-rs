//! Synchronous Gearman client.

use std::collections::HashMap;
use std::fmt;
use std::net::{TcpStream, ToSocketAddrs};
use std::str::FromStr;
use std::time::Duration;

use crate::error::{Error, JobFailure, Result};
use crate::net::{Config, Connection};
use crate::protocol::{
    self, Packet, PacketType, ensure_no_nul, field_to_string, field_to_string_trim, parse_u32,
    split_fields, trim_trailing_nul,
};

/// A server-assigned Gearman job handle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobHandle(String);

impl JobHandle {
    /// Creates a handle from a string.
    pub fn new(handle: impl Into<String>) -> Self {
        Self(handle.into())
    }

    /// Returns the handle as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the handle and returns the underlying string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for JobHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for JobHandle {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<String> for JobHandle {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for JobHandle {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Job priority understood by Gearman.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Priority {
    /// Low priority.
    Low,
    /// Normal priority (the default).
    #[default]
    Normal,
    /// High priority.
    High,
}

impl Priority {
    fn submit_packet(self, background: bool) -> PacketType {
        match (self, background) {
            (Self::High, false) => PacketType::SubmitJobHigh,
            (Self::High, true) => PacketType::SubmitJobHighBg,
            (Self::Low, false) => PacketType::SubmitJobLow,
            (Self::Low, true) => PacketType::SubmitJobLowBg,
            (Self::Normal, false) => PacketType::SubmitJob,
            (Self::Normal, true) => PacketType::SubmitJobBg,
        }
    }
}

/// Cron-like schedule used by `SUBMIT_JOB_SCHED`.
///
/// Gearman expects five fields: minute, hour, day of month, month and day of
/// week. Day of week uses `0` for Monday. The job server does not support the
/// full cron syntax; each field is usually `*`, a single number or a range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    /// Minute field (`0-59` or `*`).
    pub minute: String,
    /// Hour field (`0-23` or `*`).
    pub hour: String,
    /// Day-of-month field (`1-31` or `*`).
    pub day: String,
    /// Month field (`1-12` or `*`).
    pub month: String,
    /// Day-of-week field (`0-6`, Monday is `0`, or `*`).
    pub weekday: String,
}

impl Default for Schedule {
    fn default() -> Self {
        Self::every()
    }
}

impl Schedule {
    /// Creates a schedule that matches every minute.
    pub fn every() -> Self {
        Self {
            minute: "*".to_owned(),
            hour: "*".to_owned(),
            day: "*".to_owned(),
            month: "*".to_owned(),
            weekday: "*".to_owned(),
        }
    }

    /// Creates a schedule from its five fields.
    pub fn new(
        minute: impl Into<String>,
        hour: impl Into<String>,
        day: impl Into<String>,
        month: impl Into<String>,
        weekday: impl Into<String>,
    ) -> Self {
        Self {
            minute: minute.into(),
            hour: hour.into(),
            day: day.into(),
            month: month.into(),
            weekday: weekday.into(),
        }
    }

    /// Parses a five-field schedule expression.
    pub fn parse(expression: &str) -> Result<Self> {
        let fields: Vec<_> = expression.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(Error::InvalidArgument(format!(
                "schedule must contain exactly five fields, got {}",
                fields.len()
            )));
        }
        Ok(Self::new(
            fields[0], fields[1], fields[2], fields[3], fields[4],
        ))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("minute", &self.minute),
            ("hour", &self.hour),
            ("day", &self.day),
            ("month", &self.month),
            ("weekday", &self.weekday),
        ] {
            ensure_no_nul(value, name)?;
            if value.is_empty() {
                return Err(Error::InvalidArgument(format!(
                    "schedule {name} field must not be empty"
                )));
            }
        }
        Ok(())
    }
}

impl FromStr for Schedule {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl fmt::Display for Schedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {} {} {}",
            self.minute, self.hour, self.day, self.month, self.weekday
        )
    }
}

/// A job submission request.
///
/// The builder-style methods make it easy to set a unique id, priority,
/// background mode, reducer or schedule before calling [`Client::submit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobRequest {
    function: String,
    unique: String,
    workload: Vec<u8>,
    priority: Priority,
    background: bool,
    schedule: Option<Schedule>,
    epoch: Option<u64>,
    reducer: Option<String>,
}

impl JobRequest {
    /// Creates a normal-priority foreground request.
    pub fn new(function: impl Into<String>, workload: impl AsRef<[u8]>) -> Self {
        Self {
            function: function.into(),
            unique: String::new(),
            workload: workload.as_ref().to_vec(),
            priority: Priority::Normal,
            background: false,
            schedule: None,
            epoch: None,
            reducer: None,
        }
    }

    /// Sets the unique id used by the server for job coalescing.
    pub fn unique(mut self, unique: impl Into<String>) -> Self {
        self.unique = unique.into();
        self
    }

    /// Sets the job priority.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Marks the request as high priority.
    pub fn high(mut self) -> Self {
        self.priority = Priority::High;
        self
    }

    /// Marks the request as low priority.
    pub fn low(mut self) -> Self {
        self.priority = Priority::Low;
        self
    }

    /// Submits the job in the background.
    pub fn background(mut self) -> Self {
        self.background = true;
        self
    }

    /// Makes the request a foreground request (the default).
    pub fn foreground(mut self) -> Self {
        self.background = false;
        self
    }

    /// Schedules the job using `SUBMIT_JOB_SCHED`.
    pub fn schedule(mut self, schedule: Schedule) -> Self {
        self.schedule = Some(schedule);
        self
    }

    /// Schedules the job at a Unix timestamp using `SUBMIT_JOB_EPOCH`.
    pub fn run_at(mut self, epoch: u64) -> Self {
        self.epoch = Some(epoch);
        self
    }

    /// Uses a reducer (`SUBMIT_REDUCE_JOB`).
    pub fn reducer(mut self, reducer: impl Into<String>) -> Self {
        self.reducer = Some(reducer.into());
        self
    }

    /// Returns the function name.
    pub fn function(&self) -> &str {
        &self.function
    }

    /// Returns the unique id (empty when none was set).
    pub fn unique_id(&self) -> &str {
        &self.unique
    }

    /// Returns the workload bytes.
    pub fn workload(&self) -> &[u8] {
        &self.workload
    }

    /// Returns the priority.
    pub fn priority_level(&self) -> Priority {
        self.priority
    }

    /// Returns `true` for explicit background requests.
    pub fn is_background(&self) -> bool {
        self.background
    }

    /// Returns the optional schedule.
    pub fn scheduled(&self) -> Option<&Schedule> {
        self.schedule.as_ref()
    }

    /// Returns the optional epoch timestamp.
    pub fn epoch(&self) -> Option<u64> {
        self.epoch
    }

    /// Returns the optional reducer name.
    pub fn reducer_name(&self) -> Option<&str> {
        self.reducer.as_deref()
    }

    /// Returns `true` when this request expects a result on this connection.
    ///
    /// Background, scheduled and epoch jobs never send `WORK_*` packets.
    pub fn expects_result(&self) -> bool {
        !self.background && self.schedule.is_none() && self.epoch.is_none()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.function.is_empty() {
            return Err(Error::InvalidArgument(
                "function name must not be empty".to_owned(),
            ));
        }
        ensure_no_nul(&self.function, "function name")?;
        ensure_no_nul(&self.unique, "unique id")?;
        if self.unique.len() > 64 {
            return Err(Error::InvalidArgument(
                "unique id must not be longer than 64 bytes".to_owned(),
            ));
        }
        if let Some(reducer) = &self.reducer {
            ensure_no_nul(reducer, "reducer name")?;
        }
        if self.schedule.is_some() && self.epoch.is_some() {
            return Err(Error::InvalidArgument(
                "a job cannot have both a schedule and an epoch".to_owned(),
            ));
        }
        if self.reducer.is_some() && (self.schedule.is_some() || self.epoch.is_some()) {
            return Err(Error::InvalidArgument(
                "reduce jobs cannot be scheduled".to_owned(),
            ));
        }
        if let Some(schedule) = &self.schedule {
            schedule.validate()?;
        }
        Ok(())
    }

    pub(crate) fn packet_type(&self) -> PacketType {
        if self.schedule.is_some() {
            return PacketType::SubmitJobSched;
        }
        if self.epoch.is_some() {
            return PacketType::SubmitJobEpoch;
        }
        if self.reducer.is_some() {
            return if self.background {
                PacketType::SubmitReduceJobBackground
            } else {
                PacketType::SubmitReduceJob
            };
        }
        self.priority.submit_packet(self.background)
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(
            self.function.len()
                + self.unique.len()
                + self.workload.len()
                + self.reducer.as_ref().map_or(0, String::len)
                + 64,
        );

        push_field(&mut data, &self.function);
        push_field(&mut data, &self.unique);

        if let Some(schedule) = &self.schedule {
            push_field(&mut data, &schedule.minute);
            push_field(&mut data, &schedule.hour);
            push_field(&mut data, &schedule.day);
            push_field(&mut data, &schedule.month);
            push_field(&mut data, &schedule.weekday);
        } else if let Some(epoch) = self.epoch {
            push_field(&mut data, &epoch.to_string());
        } else if let Some(reducer) = &self.reducer {
            push_field(&mut data, reducer);
            // Gearman's reduce packet contains an unused argument before the
            // workload. libgearman sends an empty string here.
            data.push(0);
        }

        data.extend_from_slice(&self.workload);
        data
    }
}

fn push_field(data: &mut Vec<u8>, value: &str) {
    data.extend_from_slice(value.as_bytes());
    data.push(0);
}

/// Status of a Gearman job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobStatus {
    /// Job handle, or the unique id for a unique status query.
    pub id: String,
    /// Whether the server knows about the job.
    pub known: bool,
    /// Whether a worker is currently running the job.
    pub running: bool,
    /// Progress numerator.
    pub numerator: u32,
    /// Progress denominator.
    pub denominator: u32,
    /// Number of clients waiting for this job (`STATUS_RES_UNIQUE` only).
    pub clients: Option<u32>,
}

impl JobStatus {
    /// Returns progress as a fraction between `0.0` and `1.0`.
    pub fn progress(&self) -> Option<f64> {
        if self.denominator == 0 {
            None
        } else {
            Some(f64::from(self.numerator) / f64::from(self.denominator))
        }
    }
}

/// Result of a foreground job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobResult {
    /// Job handle.
    pub handle: JobHandle,
    /// Final payload from `WORK_COMPLETE`.
    pub data: Vec<u8>,
    /// Intermediate payloads received through `WORK_DATA`.
    pub streamed_data: Vec<Vec<u8>>,
    /// Warning payloads received through `WORK_WARNING`.
    pub warnings: Vec<Vec<u8>>,
    /// Last known status reported through `WORK_STATUS`.
    pub status: Option<JobStatus>,
}

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

    /// Sets the read timeout used for all blocking reads.
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.config.read_timeout = Some(timeout);
        self
    }

    /// Sets the write timeout used for all blocking writes.
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
    pub fn connect<A: ToSocketAddrs>(self, address: A) -> Result<Client> {
        Client::with_config(address, self.config)
    }
}

/// A synchronous Gearman client.
///
/// A `Client` owns one TCP connection. Foreground job results are
/// demultiplexed by handle, so multiple foreground jobs may be in flight on the
/// same connection.
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
    pub fn connect<A: ToSocketAddrs>(address: A) -> Result<Self> {
        Self::with_config(address, Config::default())
    }

    fn with_config<A: ToSocketAddrs>(address: A, config: Config) -> Result<Self> {
        Ok(Self {
            connection: Connection::connect(address, &config)?,
            pending: HashMap::new(),
        })
    }

    /// Submits a job request and returns its server-assigned handle.
    ///
    /// For foreground requests the result can be retrieved with
    /// [`Client::wait`] or by using [`Client::submit_and_wait`].
    ///
    /// # Examples
    ///
    /// Submit a foreground job and wait for its result:
    ///
    /// ```
    /// use gearman_rs::{Client, JobRequest};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker_once(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |job| {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let request = JobRequest::new(function.clone(), b"hello")
    ///     .unique("request-1");
    /// let handle = client.submit(&request)?;
    /// let result = client.wait(&handle)?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit(&mut self, request: &JobRequest) -> Result<JobHandle> {
        request.validate()?;

        let packet_type = request.packet_type();
        let payload = request.encode();
        self.connection.send(packet_type, &payload)?;

        let handle = self.read_job_created()?;
        if request.expects_result() {
            self.pending.entry(handle.to_string()).or_default();
        }
        Ok(handle)
    }

    /// Submits a foreground job and waits for its result.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, JobRequest};
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// # let function = gearman_rs::_test_support::unique("submit_and_wait");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker_once(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |job| {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let request = JobRequest::new(function.clone(), b"hello");
    /// let result = client.submit_and_wait(&request)?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_and_wait(&mut self, request: &JobRequest) -> Result<JobResult> {
        if !request.expects_result() {
            return Err(Error::InvalidArgument(
                "background or scheduled requests cannot be awaited".to_owned(),
            ));
        }
        let handle = self.submit(request)?;
        self.wait(&handle)
    }

    /// Submits a normal-priority foreground job.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// # let function = gearman_rs::_test_support::unique("submit_job");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker_once(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |job| {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job(function.clone(), b"hello")?;
    /// let result = client.wait(&handle)?;
    /// assert_eq!(result.data, b"olleh");
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload))
    }

    /// Submits a normal-priority foreground job with a unique id.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit_job_unique");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("submit_job_unique_id");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker_once(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |job| {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_unique(function.clone(), unique.clone(), b"hello")?;
    /// let result = client.wait(&handle)?;
    /// assert_eq!(result.data, b"olleh");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_unique(
        &mut self,
        function: impl AsRef<str>,
        unique: impl Into<String>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload).unique(unique))
    }

    /// Submits a normal-priority background job.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("submit_job_bg");
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_bg(function, b"payload")?;
    /// assert!(!handle.as_str().is_empty());
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_bg(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload).background())
    }

    /// Submits a normal-priority background job with a unique id.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("submit_job_bg_unique");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("submit_job_bg_unique_id");
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_bg_unique(function, unique.clone(), b"payload")?;
    ///
    /// let status = client.job_status_unique(&unique)?;
    /// assert!(status.known);
    /// assert!(!status.running);
    ///
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_bg_unique(
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
    }

    /// Submits a high-priority foreground job.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit_job_high");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker_once(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |job| {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_high(function, b"hello")?;
    /// assert_eq!(client.wait(&handle)?.data, b"olleh");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_high(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload).high())
    }

    /// Submits a high-priority background job.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("submit_job_high_bg");
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_high_bg(function, b"payload")?;
    /// assert!(!handle.as_str().is_empty());
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_high_bg(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .high()
                .background(),
        )
    }

    /// Submits a low-priority foreground job.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("submit_job_low");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker_once(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |job| {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_low(function, b"hello")?;
    /// assert_eq!(client.wait(&handle)?.data, b"olleh");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_low(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(&JobRequest::new(function.as_ref(), workload).low())
    }

    /// Submits a low-priority background job.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("submit_job_low_bg");
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_low_bg(function, b"payload")?;
    /// assert!(!handle.as_str().is_empty());
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_low_bg(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobHandle> {
        self.submit(
            &JobRequest::new(function.as_ref(), workload)
                .low()
                .background(),
        )
    }

    /// Submits a scheduled background job.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use gearman_rs::{Client, Schedule};
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "cleanup";
    /// # let function = gearman_rs::_test_support::unique("submit_job_sched");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("submit_job_sched_id");
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_sched(
    ///     function,
    ///     unique,
    ///     Schedule::every(),
    ///     b"payload",
    /// )?;
    /// assert!(!handle.as_str().is_empty());
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_sched(
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
    }

    /// Submits a background job to run at a Unix epoch.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reminder";
    /// # let function = gearman_rs::_test_support::unique("submit_job_epoch");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("submit_job_epoch_id");
    /// let epoch = 1_900_000_000;
    /// # let epoch = gearman_rs::_test_support::epoch_in(3_600);
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_epoch(function, unique, epoch, b"payload")?;
    /// assert!(!handle.as_str().is_empty());
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_job_epoch(
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
    }

    /// Submits a foreground reduce job.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reduce-sum";
    /// # let function = gearman_rs::_test_support::unique("submit_reduce_job");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("submit_reduce_job_id");
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
    pub fn submit_reduce_job(
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
    }

    /// Submits a background reduce job.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reduce-sum";
    /// # let function = gearman_rs::_test_support::unique("submit_reduce_job_bg");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("submit_reduce_job_bg_id");
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_reduce_job_bg(function, unique, "sum", b"parts")?;
    ///
    /// let status = client.job_status(&handle)?;
    /// assert!(status.known);
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_reduce_job_bg(
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
    }

    /// Submits a foreground job and waits for the result.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("execute");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker_once(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |job| {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let result = client.execute(function, b"hello")?;
    /// assert_eq!(result.data, b"olleh");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute(
        &mut self,
        function: impl AsRef<str>,
        workload: impl AsRef<[u8]>,
    ) -> Result<JobResult> {
        self.submit_and_wait(&JobRequest::new(function.as_ref(), workload))
    }

    /// Waits for the result of a foreground job.
    ///
    /// While waiting, result packets for other tracked jobs are buffered.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # use std::time::Duration;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "reverse";
    /// # let function = gearman_rs::_test_support::unique("wait");
    /// # let (ready, worker) = gearman_rs::_test_support::spawn_worker_once(
    /// #     "127.0.0.1:4730",
    /// #     function.clone(),
    /// #     |job| {
    /// #         let mut data = job.workload().to_vec();
    /// #         data.reverse();
    /// #         job.complete(data)
    /// #     },
    /// # );
    /// # ready.recv_timeout(Duration::from_secs(5)).unwrap();
    ///
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job(function, b"hello")?;
    /// let result = client.wait(&handle)?;
    /// assert_eq!(result.data, b"olleh");
    ///
    /// # worker.join().unwrap()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn wait(&mut self, handle: &JobHandle) -> Result<JobResult> {
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

            let packet = self.connection.recv()?;
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
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("job_status");
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_bg(function, b"payload")?;
    ///
    /// let status = client.job_status(&handle)?;
    /// assert!(status.known);
    /// assert!(!status.running);
    ///
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn job_status(&mut self, handle: &JobHandle) -> Result<JobStatus> {
        self.connection
            .send(PacketType::GetStatus, handle.as_str().as_bytes())?;
        loop {
            let packet = self.connection.recv()?;
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
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("job_status_unique");
    /// let unique = "request-1";
    /// # let unique = gearman_rs::_test_support::unique("job_status_unique_id");
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// let handle = client.submit_job_bg_unique(function, unique.clone(), b"payload")?;
    ///
    /// let status = client.job_status_unique(&unique)?;
    /// assert!(status.known);
    /// # gearman_rs::_test_support::cancel("127.0.0.1:4730", &handle)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn job_status_unique(&mut self, unique: &str) -> Result<JobStatus> {
        ensure_no_nul(unique, "unique id")?;
        self.connection
            .send(PacketType::GetStatusUnique, unique.as_bytes())?;
        loop {
            let packet = self.connection.recv()?;
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
    ///
    /// The server replies with `OPTION_RES` on success and `ERROR` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// client.option("exceptions")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn option(&mut self, option: &str) -> Result<()> {
        ensure_no_nul(option, "option name")?;
        self.connection
            .send(PacketType::OptionReq, option.as_bytes())?;
        loop {
            let packet = self.connection.recv()?;
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
    ///
    /// Any server-side rejection is reported as `Ok(false)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// assert!(client.supports_option("exceptions")?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn supports_option(&mut self, option: &str) -> Result<bool> {
        match self.option(option) {
            Ok(()) => Ok(true),
            Err(Error::Server { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Sends an `ECHO_REQ` and returns the echoed bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// assert_eq!(client.echo(b"ping")?, b"ping");
    /// # Ok(())
    /// # }
    /// ```
    pub fn echo(&mut self, data: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.connection.send(PacketType::EchoReq, data.as_ref())?;
        loop {
            let packet = self.connection.recv()?;
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
    ///
    /// This bypasses all pending-job bookkeeping.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::{Client, PacketType};
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// client.send_packet(PacketType::EchoReq, b"ping")?;
    /// let packet = client.receive_packet()?;
    /// assert_eq!(packet.kind, PacketType::EchoRes);
    /// assert_eq!(packet.data, b"ping");
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_packet(&mut self, kind: PacketType, data: &[u8]) -> Result<()> {
        self.connection.send(kind, data)
    }

    /// Receives a low-level packet.
    ///
    /// This bypasses all pending-job bookkeeping.
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

    /// Returns a reference to the underlying TCP stream.
    ///
    /// When the connection uses TLS this is the raw encrypted transport.
    /// Reading from or writing to it directly bypasses TLS and corrupts the
    /// connection.
    pub fn stream(&self) -> &TcpStream {
        self.connection.stream()
    }

    /// Returns a mutable reference to the underlying TCP stream.
    ///
    /// When the connection uses TLS, writing to this stream bypasses TLS and
    /// corrupts the connection. Use it only to inspect or change socket
    /// options; check [`Self::is_tls`] first when in doubt.
    pub fn stream_mut(&mut self) -> &mut TcpStream {
        self.connection.stream_mut()
    }

    /// Returns `true` when the connection is encrypted with TLS.
    pub fn is_tls(&self) -> bool {
        self.connection.is_tls()
    }

    /// Shuts down the TCP connection.
    ///
    /// # Examples
    ///
    /// ```
    /// use gearman_rs::Client;
    /// # fn main() -> gearman_rs::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:4730")?;
    /// client.shutdown()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn shutdown(&mut self) -> Result<()> {
        self.connection.shutdown()
    }

    fn read_job_created(&mut self) -> Result<JobHandle> {
        loop {
            let packet = self.connection.recv()?;
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

pub(crate) fn parse_status(data: &[u8]) -> Result<JobStatus> {
    let fields = split_fields::<5>(data)?;
    Ok(JobStatus {
        id: field_to_string_trim(fields[0]),
        known: parse_u32(fields[1], "known flag")? != 0,
        running: parse_u32(fields[2], "running flag")? != 0,
        numerator: parse_u32(fields[3], "status numerator")?,
        denominator: parse_u32(fields[4], "status denominator")?,
        clients: None,
    })
}

pub(crate) fn parse_unique_status(data: &[u8]) -> Result<JobStatus> {
    let fields = split_fields::<6>(data)?;
    Ok(JobStatus {
        id: field_to_string_trim(fields[0]),
        known: parse_u32(fields[1], "known flag")? != 0,
        running: parse_u32(fields[2], "running flag")? != 0,
        numerator: parse_u32(fields[3], "status numerator")?,
        denominator: parse_u32(fields[4], "status denominator")?,
        clients: Some(parse_u32(fields[5], "client count")?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::split_fields;

    #[test]
    fn encodes_regular_jobs_and_priorities() {
        let request = JobRequest::new("reverse", b"data").unique("u1");
        assert_eq!(request.packet_type(), PacketType::SubmitJob);
        let encoded = request.encode();
        let fields = split_fields::<3>(&encoded).unwrap();
        assert_eq!(fields, [&b"reverse"[..], &b"u1"[..], &b"data"[..]]);

        assert_eq!(
            JobRequest::new("f", b"").high().packet_type(),
            PacketType::SubmitJobHigh
        );
        assert_eq!(
            JobRequest::new("f", b"").low().background().packet_type(),
            PacketType::SubmitJobLowBg
        );
        assert_eq!(
            JobRequest::new("f", b"").background().packet_type(),
            PacketType::SubmitJobBg
        );
    }

    #[test]
    fn encodes_reduce_jobs() {
        let request = JobRequest::new("map", b"parts").unique("u").reducer("sum");
        assert_eq!(request.packet_type(), PacketType::SubmitReduceJob);
        let encoded = request.encode();
        let fields = split_fields::<5>(&encoded).unwrap();
        assert_eq!(
            fields,
            [&b"map"[..], &b"u"[..], &b"sum"[..], &b""[..], &b"parts"[..]]
        );

        assert_eq!(
            request.background().packet_type(),
            PacketType::SubmitReduceJobBackground
        );
    }

    #[test]
    fn encodes_scheduled_and_epoch_jobs() {
        let schedule = Schedule::new("*/5", "1", "2", "3", "4");
        let request = JobRequest::new("f", b"payload")
            .unique("u")
            .schedule(schedule);
        assert_eq!(request.packet_type(), PacketType::SubmitJobSched);
        assert!(!request.expects_result());
        let encoded = request.encode();
        let fields = split_fields::<8>(&encoded).unwrap();
        assert_eq!(
            fields,
            [
                &b"f"[..],
                &b"u"[..],
                &b"*/5"[..],
                &b"1"[..],
                &b"2"[..],
                &b"3"[..],
                &b"4"[..],
                &b"payload"[..],
            ]
        );

        let request = JobRequest::new("f", b"payload").unique("u").run_at(1234);
        assert_eq!(request.packet_type(), PacketType::SubmitJobEpoch);
        let encoded = request.encode();
        let fields = split_fields::<4>(&encoded).unwrap();
        assert_eq!(
            fields,
            [&b"f"[..], &b"u"[..], &b"1234"[..], &b"payload"[..]]
        );
    }

    #[test]
    fn validates_protocol_arguments() {
        assert!(JobRequest::new("", b"").validate().is_err());
        assert!(JobRequest::new("f\0x", b"").validate().is_err());
        assert!(
            JobRequest::new("f", b"")
                .unique("x".repeat(65))
                .validate()
                .is_err()
        );
        assert!(
            JobRequest::new("f", b"")
                .schedule(Schedule::every())
                .run_at(1)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn parses_status_packets() {
        let status = parse_status(b"H:1\x001\x000\x003\x004").unwrap();
        assert_eq!(status.id, "H:1");
        assert!(status.known);
        assert!(!status.running);
        assert_eq!((status.numerator, status.denominator), (3, 4));
        assert_eq!(status.clients, None);

        let unique = parse_unique_status(b"u\x001\x001\x002\x003\x007").unwrap();
        assert_eq!(unique.id, "u");
        assert_eq!(unique.clients, Some(7));
        assert_eq!(unique.progress(), Some(2.0 / 3.0));
    }
}
