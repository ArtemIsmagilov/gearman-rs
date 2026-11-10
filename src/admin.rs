//! Gearman administrative (text) protocol client.
//!
//! The administrative protocol shares the normal Gearman port. It is line
//! based and is selected automatically by the server when the first byte is
//! not a NUL. Responses are terminated either by a line containing a single
//! `.` (list commands) or by an `OK`/`ERR` line (single-value commands).

use std::io::{BufRead, BufReader, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::net::{Config, connect_tcp};

/// One row returned by the `status` admin command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionStatus {
    /// Function name.
    pub function: String,
    /// Total number of queued jobs.
    pub total: u64,
    /// Number of running jobs.
    pub running: u64,
    /// Number of available workers.
    pub workers: u64,
}

/// One row returned by the `prioritystatus` admin command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorityStatus {
    /// Function name.
    pub function: String,
    /// Queued high-priority jobs.
    pub high: u64,
    /// Queued normal-priority jobs.
    pub normal: u64,
    /// Queued low-priority jobs.
    pub low: u64,
    /// Number of available workers.
    pub workers: u64,
}

/// One row returned by the `workers` admin command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    /// Socket file descriptor on the server.
    pub fd: i32,
    /// Peer IP address.
    pub ip: String,
    /// Client id (usually `-` when not set).
    pub client_id: String,
    /// Registered function names.
    pub functions: Vec<String>,
}

/// One row returned by the `show jobs` admin command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobInfo {
    /// Job handle.
    pub handle: String,
    /// Retry count.
    pub retries: u64,
    /// Internal ignore flag.
    pub ignore: u64,
    /// Internal queued timestamp/flag.
    pub queued: u64,
}

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

    /// Connects to a Gearman server.
    pub fn connect<A: ToSocketAddrs>(self, address: A) -> Result<AdminClient> {
        AdminClient::with_config(address, self.config)
    }
}

/// A synchronous Gearman administrative client.
pub struct AdminClient {
    reader: BufReader<TcpStream>,
}

impl AdminClient {
    /// Returns a builder for configuring the connection.
    pub fn builder() -> AdminClientBuilder {
        AdminClientBuilder::new()
    }

    /// Connects to a Gearman server with default settings.
    pub fn connect<A: ToSocketAddrs>(address: A) -> Result<Self> {
        Self::with_config(address, Config::default())
    }

    fn with_config<A: ToSocketAddrs>(address: A, config: Config) -> Result<Self> {
        Ok(Self {
            reader: BufReader::new(connect_tcp(address, &config)?),
        })
    }

    /// Sends an arbitrary admin command and returns its response lines.
    ///
    /// Single-value commands (`version`, `maxqueue`, ...) and list commands
    /// (`status`, `workers`, ...) are both handled.
    pub fn command(&mut self, command: &str) -> Result<Vec<String>> {
        self.send(command)?;
        let mode = if is_list_command(command) {
            ResponseMode::List
        } else {
            ResponseMode::Auto
        };
        self.read_response(mode)
    }

    /// Runs `status` and returns queue statistics per function.
    pub fn status(&mut self) -> Result<Vec<FunctionStatus>> {
        let lines = self.list_command("status")?;
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

    /// Runs `prioritystatus` and returns queue statistics per priority.
    pub fn priority_status(&mut self) -> Result<Vec<PriorityStatus>> {
        let lines = self.list_command("prioritystatus")?;
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

    /// Runs `workers` and returns connected workers.
    pub fn workers(&mut self) -> Result<Vec<WorkerInfo>> {
        let lines = self.list_command("workers")?;
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

    /// Runs `version` and returns the server version.
    pub fn version(&mut self) -> Result<String> {
        let line = self.single_line("version")?;
        Ok(line.strip_prefix("OK ").unwrap_or(&line).to_owned())
    }

    /// Runs `getpid` and returns the server process id.
    pub fn get_pid(&mut self) -> Result<u32> {
        let line = self.single_line("getpid")?;
        let value = line.strip_prefix("OK ").unwrap_or(&line);
        value
            .parse()
            .map_err(|_| Error::Protocol(format!("invalid getpid response: {line:?}")))
    }

    /// Runs `verbose` and returns the current verbosity name.
    pub fn verbose(&mut self) -> Result<String> {
        let line = self.single_line("verbose")?;
        Ok(line.strip_prefix("OK ").unwrap_or(&line).to_owned())
    }

    /// Sets one maximum queue size for a function.
    pub fn max_queue(&mut self, function: &str, size: u32) -> Result<()> {
        self.single_line(&format!("maxqueue {function} {size}"))?;
        Ok(())
    }

    /// Sets separate maximum queue sizes for high, normal and low priority.
    pub fn max_queue_priority(
        &mut self,
        function: &str,
        high: u32,
        normal: u32,
        low: u32,
    ) -> Result<()> {
        self.single_line(&format!("maxqueue {function} {high} {normal} {low}"))?;
        Ok(())
    }

    /// Cancels a queued job by handle.
    pub fn cancel_job(&mut self, handle: &str) -> Result<()> {
        self.single_line(&format!("cancel job {handle}"))?;
        Ok(())
    }

    /// Runs `show jobs` and returns queued jobs.
    pub fn show_jobs(&mut self) -> Result<Vec<JobInfo>> {
        let lines = self.list_command("show jobs")?;
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

    /// Runs `show unique jobs` and returns `(handle, unique)` pairs.
    pub fn show_unique_jobs(&mut self) -> Result<Vec<(String, String)>> {
        let lines = self.list_command("show unique jobs")?;
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

    /// Creates the named function on the server.
    pub fn create_function(&mut self, function: &str) -> Result<()> {
        self.single_line(&format!("create function {function}"))?;
        Ok(())
    }

    /// Drops the named function from the server.
    pub fn drop_function(&mut self, function: &str) -> Result<()> {
        self.single_line(&format!("drop function {function}"))?;
        Ok(())
    }

    /// Changes the read timeout of the underlying socket.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.reader.get_mut().set_read_timeout(timeout)?;
        Ok(())
    }

    /// Changes the write timeout of the underlying socket.
    pub fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.reader.get_mut().set_write_timeout(timeout)?;
        Ok(())
    }

    /// Returns a reference to the underlying TCP stream.
    pub fn stream(&self) -> &TcpStream {
        self.reader.get_ref()
    }

    /// Shuts down the TCP connection.
    pub fn shutdown(&mut self) -> Result<()> {
        self.reader.get_ref().shutdown(Shutdown::Both)?;
        Ok(())
    }

    fn single_line(&mut self, command: &str) -> Result<String> {
        self.send(command)?;
        let lines = self.read_response(ResponseMode::Single)?;
        lines.into_iter().next().ok_or(Error::ConnectionClosed)
    }

    fn list_command(&mut self, command: &str) -> Result<Vec<String>> {
        self.send(command)?;
        self.read_response(ResponseMode::List)
    }

    fn send(&mut self, command: &str) -> Result<()> {
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

        let stream = self.reader.get_mut();
        stream
            .write_all(command.as_bytes())
            .map_err(|error| Error::map_io_timeout(error, "writing the admin command"))?;
        stream
            .write_all(b"\n")
            .map_err(|error| Error::map_io_timeout(error, "writing the admin command"))?;
        stream
            .flush()
            .map_err(|error| Error::map_io_timeout(error, "flushing the admin command"))?;
        Ok(())
    }

    fn read_response(&mut self, mode: ResponseMode) -> Result<Vec<String>> {
        let mut lines = Vec::new();

        loop {
            let mut line = String::new();
            let read = self
                .reader
                .read_line(&mut line)
                .map_err(|error| Error::map_io_timeout(error, "reading the admin response"))?;
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
