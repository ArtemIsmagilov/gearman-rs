//! Asynchronous Gearman administrative client.

use std::time::Duration;
use tokio::net::ToSocketAddrs;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::admin::{FunctionStatus, JobInfo, PriorityStatus, WorkerInfo};
use crate::error::{Error, Result};
use crate::net::Config;

use super::connection::{Stream, connect_stream};

/// Builder for [`AdminClient`].
#[derive(Debug, Clone, Default)]
pub struct AdminClientBuilder {
    config: Config,
}

impl AdminClientBuilder {
    /// Creates a builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the TCP connect timeout.
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

    /// Connects to a Gearman server.
    pub async fn connect<A: ToSocketAddrs>(self, address: A) -> Result<AdminClient> {
        AdminClient::with_config(address, self.config).await
    }
}

/// An asynchronous Gearman administrative client.
pub struct AdminClient {
    reader: BufReader<Stream>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
}

impl AdminClient {
    /// Returns a builder for configuring the connection.
    pub fn builder() -> AdminClientBuilder {
        AdminClientBuilder::new()
    }

    /// Connects to a Gearman server with default settings.
    pub async fn connect<A: ToSocketAddrs>(address: A) -> Result<Self> {
        Self::with_config(address, Config::default()).await
    }

    async fn with_config<A: ToSocketAddrs>(address: A, config: Config) -> Result<Self> {
        let stream = connect_stream(address, &config).await?;
        Ok(Self {
            reader: BufReader::new(stream),
            read_timeout: config.read_timeout,
            write_timeout: config.write_timeout,
        })
    }

    /// Sends an arbitrary admin command and returns its response lines.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// let lines = admin.command("version").await?;
    /// assert!(lines[0].starts_with("OK"));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn command(&mut self, command: &str) -> Result<Vec<String>> {
        self.send(command).await?;
        let mode = if is_list_command(command) {
            ResponseMode::List
        } else {
            ResponseMode::Auto
        };
        self.read_response(mode).await
    }

    /// Runs `status`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// for row in admin.status().await? {
    ///     println!("{}: queued={} running={} workers={}", row.function, row.total, row.running, row.workers);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn status(&mut self) -> Result<Vec<FunctionStatus>> {
        let lines = self.list_command("status").await?;
        let mut result = Vec::with_capacity(lines.len());
        for line in lines {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 4 {
                return Err(Error::Protocol(format!("invalid status row: {line:?}")));
            }
            result.push(FunctionStatus {
                function: fields[0].to_owned(),
                total: parse_u64(fields[1], "status total")?,
                running: parse_u64(fields[2], "status running")?,
                workers: parse_u64(fields[3], "status workers")?,
            });
        }
        Ok(result)
    }

    /// Runs `prioritystatus`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// for row in admin.priority_status().await? {
    ///     println!("{}: high={} normal={} low={} workers={}", row.function, row.high, row.normal, row.low, row.workers);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn priority_status(&mut self) -> Result<Vec<PriorityStatus>> {
        let lines = self.list_command("prioritystatus").await?;
        let mut result = Vec::with_capacity(lines.len());
        for line in lines {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 5 {
                return Err(Error::Protocol(format!(
                    "invalid prioritystatus row: {line:?}"
                )));
            }
            result.push(PriorityStatus {
                function: fields[0].to_owned(),
                high: parse_u64(fields[1], "high queued")?,
                normal: parse_u64(fields[2], "normal queued")?,
                low: parse_u64(fields[3], "low queued")?,
                workers: parse_u64(fields[4], "available workers")?,
            });
        }
        Ok(result)
    }

    /// Runs `workers`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// for worker in admin.workers().await? {
    ///     println!("{} {} {:?}", worker.ip, worker.client_id, worker.functions);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn workers(&mut self) -> Result<Vec<WorkerInfo>> {
        let lines = self.list_command("workers").await?;
        let mut result = Vec::with_capacity(lines.len());
        for line in lines {
            let (left, functions) = match line.split_once(" :") {
                Some((left, right)) => (left, right),
                None => (line.as_str(), ""),
            };
            let mut parts = left.split_whitespace();
            let fd = parts
                .next()
                .ok_or_else(|| Error::Protocol(format!("invalid workers row: {line:?}")))?
                .parse::<i32>()
                .map_err(|_| Error::Protocol(format!("invalid worker fd: {line:?}")))?;
            let ip = parts
                .next()
                .ok_or_else(|| Error::Protocol(format!("invalid workers row: {line:?}")))?
                .to_owned();
            let client_id = parts.next().unwrap_or("-").to_owned();
            result.push(WorkerInfo {
                fd,
                ip,
                client_id,
                functions: functions.split_whitespace().map(str::to_owned).collect(),
            });
        }
        Ok(result)
    }

    /// Runs `version`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// assert!(!admin.version().await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn version(&mut self) -> Result<String> {
        let line = self.single_line("version").await?;
        Ok(line.strip_prefix("OK ").unwrap_or(&line).to_owned())
    }

    /// Runs `getpid`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// assert!(admin.get_pid().await? > 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_pid(&mut self) -> Result<u32> {
        let line = self.single_line("getpid").await?;
        let value = line.strip_prefix("OK ").unwrap_or(&line);
        value
            .parse()
            .map_err(|_| Error::Protocol(format!("invalid getpid response: {line:?}")))
    }

    /// Runs `verbose`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// assert!(!admin.verbose().await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verbose(&mut self) -> Result<String> {
        let line = self.single_line("verbose").await?;
        Ok(line.strip_prefix("OK ").unwrap_or(&line).to_owned())
    }

    /// Sets one maximum queue size for a function.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let function = "thumbnail";
    /// # let function = gearman_rs::_test_support::unique("max_queue");
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.max_queue(&function, 100).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn max_queue(&mut self, function: &str, size: u32) -> Result<()> {
        self.single_line(&format!("maxqueue {function} {size}"))
            .await?;
        Ok(())
    }

    /// Sets per-priority maximum queue sizes.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let function = "thumbnail";
    /// # let function = gearman_rs::_test_support::unique("max_queue_priority");
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.max_queue_priority(&function, 10, 50, 100).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn max_queue_priority(
        &mut self,
        function: &str,
        high: u32,
        normal: u32,
        low: u32,
    ) -> Result<()> {
        self.single_line(&format!("maxqueue {function} {high} {normal} {low}"))
            .await?;
        Ok(())
    }

    /// Cancels a queued job by handle.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("cancel_job");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg(function, b"payload").await?;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_job(&mut self, handle: &str) -> Result<()> {
        self.single_line(&format!("cancel job {handle}")).await?;
        Ok(())
    }

    /// Runs `show jobs`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// # let function = gearman_rs::_test_support::unique("show_jobs");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg(function, b"payload").await?;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// let jobs = admin.show_jobs().await?;
    /// assert!(jobs.iter().any(|job| job.handle == handle.as_str()));
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_jobs(&mut self) -> Result<Vec<JobInfo>> {
        let lines = self.list_command("show jobs").await?;
        let mut result = Vec::with_capacity(lines.len());
        for line in lines {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 4 {
                return Err(Error::Protocol(format!("invalid show jobs row: {line:?}")));
            }
            result.push(JobInfo {
                handle: fields[0].to_owned(),
                retries: parse_u64(fields[1], "retry count")?,
                ignore: parse_u64(fields[2], "ignore flag")?,
                queued: parse_u64(fields[3], "queued flag")?,
            });
        }
        Ok(result)
    }

    /// Runs `show unique jobs`.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::{AsyncAdminClient, AsyncClient};
    /// let function = "resize";
    /// let unique = "job-1";
    /// # let function = gearman_rs::_test_support::unique("show_unique_jobs");
    /// # let unique = gearman_rs::_test_support::unique("show_unique_jobs_id");
    /// let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    /// let handle = client.submit_job_bg_unique(function, unique, b"payload").await?;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// let jobs = admin.show_unique_jobs().await?;
    /// assert!(jobs.iter().any(|(job, _)| job == handle.as_str()));
    /// admin.cancel_job(handle.as_str()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_unique_jobs(&mut self) -> Result<Vec<(String, String)>> {
        let lines = self.list_command("show unique jobs").await?;
        let mut result = Vec::with_capacity(lines.len());
        for line in lines {
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 2 {
                return Err(Error::Protocol(format!(
                    "invalid show unique jobs row: {line:?}"
                )));
            }
            result.push((fields[0].to_owned(), fields[1].to_owned()));
        }
        Ok(result)
    }

    /// Creates the named function.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let function = "thumbnail";
    /// # let function = gearman_rs::_test_support::unique("create_function");
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.create_function(&function).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_function(&mut self, function: &str) -> Result<()> {
        self.single_line(&format!("create function {function}"))
            .await?;
        Ok(())
    }

    /// Drops the named function.
    /// # Examples
    ///
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.drop_function("thumbnail").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn drop_function(&mut self, function: &str) -> Result<()> {
        self.single_line(&format!("drop function {function}"))
            .await?;
        Ok(())
    }

    /// Changes the read timeout used by subsequent operations.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.read_timeout = timeout;
    }

    /// Changes the write timeout used by subsequent operations.
    pub fn set_write_timeout(&mut self, timeout: Option<Duration>) {
        self.write_timeout = timeout;
    }

    /// Returns a reference to the underlying TCP stream.
    pub fn stream(&self) -> &TcpStream {
        self.reader.get_ref().tcp()
    }

    /// Shuts down the TCP connection.
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main]
    /// # async fn main() -> gearman_rs::Result<()> {
    /// use gearman_rs::AsyncAdminClient;
    /// let mut admin = AsyncAdminClient::connect("127.0.0.1:4730").await?;
    /// admin.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown(&mut self) -> Result<()> {
        self.reader.get_mut().shutdown().await
    }

    async fn single_line(&mut self, command: &str) -> Result<String> {
        self.send(command).await?;
        let lines = self.read_response(ResponseMode::Single).await?;
        lines.into_iter().next().ok_or(Error::ConnectionClosed)
    }

    async fn list_command(&mut self, command: &str) -> Result<Vec<String>> {
        self.send(command).await?;
        self.read_response(ResponseMode::List).await
    }

    async fn send(&mut self, command: &str) -> Result<()> {
        if command.is_empty() {
            return Err(Error::InvalidArgument(
                "admin command must not be empty".to_owned(),
            ));
        }
        if command.as_bytes().first() == Some(&0) {
            return Err(Error::InvalidArgument(
                "admin command must not start with a NUL byte".to_owned(),
            ));
        }
        if command.contains(['\r', '\n']) {
            return Err(Error::InvalidArgument(
                "admin command must not contain line breaks".to_owned(),
            ));
        }

        let mut bytes = Vec::with_capacity(command.len() + 1);
        bytes.extend_from_slice(command.as_bytes());
        bytes.push(b'\n');

        let timeout = self.write_timeout;
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, async {
                let stream = self.reader.get_mut();
                stream.write_all(&bytes).await?;
                stream.flush().await
            })
            .await
            .map_err(|_| Error::Timeout("writing the admin command"))??,
            None => {
                let stream = self.reader.get_mut();
                stream.write_all(&bytes).await?;
                stream.flush().await?;
            }
        }
        Ok(())
    }

    async fn read_response(&mut self, mode: ResponseMode) -> Result<Vec<String>> {
        let mut lines = Vec::new();

        loop {
            let mut line = String::new();
            let timeout = self.read_timeout;
            let read = match timeout {
                Some(timeout) => tokio::time::timeout(timeout, self.reader.read_line(&mut line))
                    .await
                    .map_err(|_| Error::Timeout("reading the admin response"))??,
                None => self.reader.read_line(&mut line).await?,
            };

            if read == 0 {
                return Err(Error::ConnectionClosed);
            }

            let line = line.trim_end_matches(['\r', '\n']).to_owned();

            if let Some(error) = parse_admin_error(&line) {
                return Err(error);
            }
            if line == "." {
                break;
            }

            match mode {
                ResponseMode::Single => {
                    lines.push(line);
                    break;
                }
                ResponseMode::List => lines.push(line),
                ResponseMode::Auto => {
                    let single = line == "OK" || line.starts_with("OK ");
                    lines.push(line);
                    if single {
                        break;
                    }
                }
            }
        }

        Ok(lines)
    }
}

fn is_list_command(command: &str) -> bool {
    matches!(
        command
            .split_whitespace()
            .next()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("status" | "prioritystatus" | "workers" | "show")
    )
}

#[derive(Debug, Clone, Copy)]
enum ResponseMode {
    Single,
    List,
    Auto,
}

fn parse_admin_error(line: &str) -> Option<Error> {
    let rest = line.strip_prefix("ERR")?;
    if rest.is_empty() {
        return Some(Error::AdminServer {
            code: "ERR".to_owned(),
            message: String::new(),
        });
    }
    let rest = rest.strip_prefix(' ')?;
    let mut parts = rest.splitn(2, ' ');
    let code = parts.next().unwrap_or_default().to_owned();
    let message = parts.next().unwrap_or_default().to_owned();
    Some(Error::AdminServer { code, message })
}

fn parse_u64(value: &str, what: &str) -> Result<u64> {
    value
        .parse()
        .map_err(|_| Error::Protocol(format!("invalid {what}: {value:?}")))
}
