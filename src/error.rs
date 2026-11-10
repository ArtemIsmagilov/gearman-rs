//! Error and result types.

use std::fmt;
use std::io;

use crate::protocol::PacketType;

/// Convenient result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Information about a failed Gearman job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFailure {
    /// Job handle.
    pub handle: String,
    /// Optional exception payload sent by the worker.
    pub exception: Option<String>,
}

impl JobFailure {
    /// Creates a new job failure.
    pub fn new(handle: impl Into<String>, exception: Option<String>) -> Self {
        Self {
            handle: handle.into(),
            exception,
        }
    }
}

impl fmt::Display for JobFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "job {} failed", self.handle)?;
        if let Some(exception) = &self.exception {
            write!(f, ": {exception}")?;
        }
        Ok(())
    }
}

/// Errors returned by the Gearman client, worker and admin APIs.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Underlying I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// The peer closed the connection.
    #[error("connection closed by peer")]
    ConnectionClosed,

    /// An operation exceeded its configured timeout.
    #[error("timed out while {0}")]
    Timeout(&'static str),

    /// The packet header contained unexpected magic bytes.
    #[error("invalid packet magic: {got:?}")]
    InvalidMagic {
        /// Magic bytes that were received.
        got: [u8; 4],
    },

    /// A packet was larger than the configured maximum.
    #[error("packet of {size} bytes exceeds the limit of {max} bytes")]
    PacketTooLarge {
        /// Received packet size.
        size: usize,
        /// Configured maximum.
        max: usize,
    },

    /// The server sent malformed data.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The server sent an `ERROR` packet.
    #[error("server error {code}: {message}")]
    Server {
        /// Error code sent by the server.
        code: String,
        /// Human-readable error text.
        message: String,
    },

    /// A packet of an unexpected type was received.
    #[error("unexpected packet: expected {expected}, got {got}")]
    UnexpectedPacket {
        /// Description of the expected packet.
        expected: &'static str,
        /// Packet type that was actually received.
        got: PacketType,
    },

    /// A job failed.
    #[error("{0}")]
    JobFailed(JobFailure),

    /// The requested job is not tracked by this client instance.
    #[error("job {handle} is not tracked by this client")]
    UnknownJob {
        /// Job handle.
        handle: String,
    },

    /// A worker handler tried to finish an already finished job.
    #[error("job {handle} has already been completed or failed")]
    JobAlreadyFinished {
        /// Job handle.
        handle: String,
    },

    /// An argument cannot be encoded in the Gearman protocol.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The server returned an administrative protocol error.
    #[error("admin server error {code}: {message}")]
    AdminServer {
        /// Error code sent by the server.
        code: String,
        /// Human-readable error text.
        message: String,
    },
}

impl Error {
    /// Returns `true` if this error was caused by an operation timeout.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout(_))
    }

    /// Maps `WouldBlock` and `TimedOut` I/O errors to [`Error::Timeout`].
    pub(crate) fn map_io_timeout(error: io::Error, operation: &'static str) -> Self {
        match error.kind() {
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => Self::Timeout(operation),
            io::ErrorKind::UnexpectedEof => Self::ConnectionClosed,
            _ => Self::Io(error),
        }
    }
}
