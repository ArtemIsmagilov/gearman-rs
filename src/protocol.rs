//! Gearman binary protocol types and codec.
//!
//! The Gearman binary protocol wraps every request and response in a 12-byte
//! header:
//!
//! ```text
//! 4 bytes  magic  ("\0REQ" or "\0RES")
//! 4 bytes  type   (big-endian u32)
//! 4 bytes  size   (big-endian u32)
//! N bytes  data
//! ```
//!
//! This module exposes both the raw packet codec and the typed packet
//! enumeration. Most users should use [`crate::Client`] or [`crate::Worker`]
//! instead.

use std::fmt;
use std::io::{Read, Write};

use crate::{Error, Result};

/// Magic bytes for a request packet.
pub const MAGIC_REQUEST: [u8; 4] = *b"\0REQ";
/// Magic bytes for a response packet.
pub const MAGIC_RESPONSE: [u8; 4] = *b"\0RES";
/// Size of the binary packet header.
pub const HEADER_SIZE: usize = 12;
/// Default maximum accepted packet size (64 MiB).
pub const DEFAULT_MAX_PACKET_SIZE: usize = 64 * 1024 * 1024;

/// Every packet type defined by the Gearman binary protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PacketType {
    /// Worker request: register a function.
    CanDo = 1,
    /// Worker request: unregister a function.
    CantDo = 2,
    /// Worker request: unregister every function.
    ResetAbilities = 3,
    /// Worker request: go to sleep until a `NOOP` is received.
    PreSleep = 4,
    /// Unused packet type reserved by the protocol.
    Unused = 5,
    /// Server response: wake a sleeping worker.
    Noop = 6,
    /// Client request: submit a foreground job.
    SubmitJob = 7,
    /// Server response: a job was created.
    JobCreated = 8,
    /// Worker request: grab a job.
    GrabJob = 9,
    /// Server response: no job is available.
    NoJob = 10,
    /// Server response: job assignment.
    JobAssign = 11,
    /// Worker request / client response: job progress.
    WorkStatus = 12,
    /// Worker request / client response: job completed.
    WorkComplete = 13,
    /// Worker request / client response: job failed.
    WorkFail = 14,
    /// Client request: query a job by handle.
    GetStatus = 15,
    /// Client/worker request: echo data.
    EchoReq = 16,
    /// Server response: echoed data.
    EchoRes = 17,
    /// Client request: submit a background job.
    SubmitJobBg = 18,
    /// Server response: an error occurred.
    Error = 19,
    /// Server response: job status.
    StatusRes = 20,
    /// Client request: submit a high-priority foreground job.
    SubmitJobHigh = 21,
    /// Worker request: set the worker's client id.
    SetClientId = 22,
    /// Worker request: register a function with an execution timeout.
    CanDoTimeout = 23,
    /// Worker request: tell the server that this is the worker's only server.
    AllYours = 24,
    /// Worker request / client response: job raised an exception.
    WorkException = 25,
    /// Client/worker request: enable a connection option.
    OptionReq = 26,
    /// Server response: option accepted.
    OptionRes = 27,
    /// Worker request / client response: intermediate job data.
    WorkData = 28,
    /// Worker request / client response: job warning.
    WorkWarning = 29,
    /// Worker request: grab a job including its unique id.
    GrabJobUniq = 30,
    /// Server response: job assignment with a unique id.
    JobAssignUniq = 31,
    /// Client request: submit a high-priority background job.
    SubmitJobHighBg = 32,
    /// Client request: submit a low-priority foreground job.
    SubmitJobLow = 33,
    /// Client request: submit a low-priority background job.
    SubmitJobLowBg = 34,
    /// Client request: submit a scheduled background job.
    SubmitJobSched = 35,
    /// Client request: submit a job at a specific Unix epoch.
    SubmitJobEpoch = 36,
    /// Client request: submit a foreground reduce job.
    SubmitReduceJob = 37,
    /// Client request: submit a background reduce job.
    SubmitReduceJobBackground = 38,
    /// Worker request: grab a job including unique id and reducer.
    GrabJobAll = 39,
    /// Server response: job assignment with unique id and reducer.
    JobAssignAll = 40,
    /// Client request: query a job by unique id.
    GetStatusUnique = 41,
    /// Server response: job status by unique id.
    StatusResUnique = 42,
}

impl PacketType {
    /// Returns the numeric protocol value.
    pub const fn as_u32(self) -> u32 {
        self as u32
    }

    /// Converts a numeric protocol value into a packet type.
    pub const fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            1 => Self::CanDo,
            2 => Self::CantDo,
            3 => Self::ResetAbilities,
            4 => Self::PreSleep,
            5 => Self::Unused,
            6 => Self::Noop,
            7 => Self::SubmitJob,
            8 => Self::JobCreated,
            9 => Self::GrabJob,
            10 => Self::NoJob,
            11 => Self::JobAssign,
            12 => Self::WorkStatus,
            13 => Self::WorkComplete,
            14 => Self::WorkFail,
            15 => Self::GetStatus,
            16 => Self::EchoReq,
            17 => Self::EchoRes,
            18 => Self::SubmitJobBg,
            19 => Self::Error,
            20 => Self::StatusRes,
            21 => Self::SubmitJobHigh,
            22 => Self::SetClientId,
            23 => Self::CanDoTimeout,
            24 => Self::AllYours,
            25 => Self::WorkException,
            26 => Self::OptionReq,
            27 => Self::OptionRes,
            28 => Self::WorkData,
            29 => Self::WorkWarning,
            30 => Self::GrabJobUniq,
            31 => Self::JobAssignUniq,
            32 => Self::SubmitJobHighBg,
            33 => Self::SubmitJobLow,
            34 => Self::SubmitJobLowBg,
            35 => Self::SubmitJobSched,
            36 => Self::SubmitJobEpoch,
            37 => Self::SubmitReduceJob,
            38 => Self::SubmitReduceJobBackground,
            39 => Self::GrabJobAll,
            40 => Self::JobAssignAll,
            41 => Self::GetStatusUnique,
            42 => Self::StatusResUnique,
            _ => return None,
        })
    }

    /// Returns `true` for packets that carry job progress or results.
    pub const fn is_job_event(self) -> bool {
        matches!(
            self,
            Self::WorkStatus
                | Self::WorkData
                | Self::WorkWarning
                | Self::WorkComplete
                | Self::WorkFail
                | Self::WorkException
        )
    }
}

impl fmt::Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// A decoded Gearman binary packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    /// Packet type.
    pub kind: PacketType,
    /// Packet payload.
    pub data: Vec<u8>,
}

impl Packet {
    /// Creates an empty packet of the given type.
    pub fn new(kind: PacketType) -> Self {
        Self {
            kind,
            data: Vec::new(),
        }
    }

    /// Creates a packet with the given payload.
    pub fn with_data(kind: PacketType, data: impl Into<Vec<u8>>) -> Self {
        Self {
            kind,
            data: data.into(),
        }
    }

    /// Reads a packet response (`\0RES`) from a reader.
    pub fn read_response<R: Read>(reader: &mut R, max_size: usize) -> Result<Self> {
        read_packet(reader, MAGIC_RESPONSE, max_size)
    }

    /// Reads a packet request (`\0REQ`) from a reader.
    ///
    /// This is mostly useful for tests and for implementing a Gearman server.
    pub fn read_request<R: Read>(reader: &mut R, max_size: usize) -> Result<Self> {
        read_packet(reader, MAGIC_REQUEST, max_size)
    }

    /// Writes this packet as a request (`\0REQ`).
    pub fn write_request<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_packet(writer, MAGIC_REQUEST, self.kind, &self.data)
    }

    /// Writes this packet as a response (`\0RES`).
    pub fn write_response<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_packet(writer, MAGIC_RESPONSE, self.kind, &self.data)
    }

    /// Returns the payload as a UTF-8 string, replacing invalid sequences.
    pub fn data_lossy(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

/// Reads one binary packet, validating its magic bytes.
pub fn read_packet<R: Read>(
    reader: &mut R,
    expected_magic: [u8; 4],
    max_size: usize,
) -> Result<Packet> {
    let mut header = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::ConnectionClosed
        } else {
            Error::Io(error)
        }
    })?;

    let magic: [u8; 4] = header[0..4].try_into().expect("fixed-size slice");
    if magic != expected_magic {
        return Err(Error::InvalidMagic { got: magic });
    }

    let raw_type = u32::from_be_bytes(header[4..8].try_into().expect("fixed-size slice"));
    let size = u32::from_be_bytes(header[8..12].try_into().expect("fixed-size slice")) as usize;

    if size > max_size {
        return Err(Error::PacketTooLarge {
            size,
            max: max_size,
        });
    }

    let kind = PacketType::from_u32(raw_type)
        .ok_or_else(|| Error::Protocol(format!("unknown packet type {raw_type}")))?;

    let mut data = vec![0u8; size];
    reader.read_exact(&mut data).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::ConnectionClosed
        } else {
            Error::Io(error)
        }
    })?;

    Ok(Packet { kind, data })
}

/// Writes one binary packet.
pub fn write_packet<W: Write>(
    writer: &mut W,
    magic: [u8; 4],
    kind: PacketType,
    data: &[u8],
) -> Result<()> {
    let size = u32::try_from(data.len()).map_err(|_| Error::PacketTooLarge {
        size: data.len(),
        max: u32::MAX as usize,
    })?;

    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&magic);
    header[4..8].copy_from_slice(&kind.as_u32().to_be_bytes());
    header[8..12].copy_from_slice(&size.to_be_bytes());

    writer.write_all(&header)?;
    writer.write_all(data)?;
    Ok(())
}

/// Splits a byte slice at the first NUL byte.
///
/// Returns `None` when no NUL byte is present.
pub fn split_nul(data: &[u8]) -> Option<(&[u8], &[u8])> {
    let index = data.iter().position(|byte| *byte == 0)?;
    Some((&data[..index], &data[index + 1..]))
}

/// Splits a packet payload into exactly `N` fields.
///
/// The first `N - 1` fields are separated by NUL bytes. The final field is the
/// remainder of the payload and may be empty or contain arbitrary bytes.
pub fn split_fields<const N: usize>(data: &[u8]) -> Result<[&[u8]; N]> {
    if N == 0 {
        return Ok([&[][..]; N]);
    }

    let mut fields = [&[][..]; N];
    let mut rest = data;

    for field in fields.iter_mut().take(N - 1) {
        let (value, tail) = split_nul(rest).ok_or_else(|| {
            Error::Protocol(format!(
                "malformed packet: expected {N} NUL-separated fields"
            ))
        })?;
        *field = value;
        rest = tail;
    }
    fields[N - 1] = rest;

    Ok(fields)
}

/// Converts a protocol field to a UTF-8 string, replacing invalid sequences.
pub fn field_to_string(field: &[u8]) -> String {
    String::from_utf8_lossy(field).into_owned()
}

/// Converts a protocol field to a string without a trailing NUL byte.
pub fn field_to_string_trim(field: &[u8]) -> String {
    field_to_string(trim_trailing_nul(field))
}

/// Parses a protocol integer field, ignoring trailing NUL bytes.
pub fn parse_u32(field: &[u8], what: &str) -> Result<u32> {
    let text = std::str::from_utf8(field)
        .map_err(|_| Error::Protocol(format!("{what} is not valid UTF-8")))?;
    let text = text.trim_end_matches('\0');
    text.parse::<u32>()
        .map_err(|_| Error::Protocol(format!("invalid {what}: {text:?}")))
}

/// Parses a protocol integer field as `u64`, ignoring trailing NUL bytes.
pub fn parse_u64(field: &[u8], what: &str) -> Result<u64> {
    let text = std::str::from_utf8(field)
        .map_err(|_| Error::Protocol(format!("{what} is not valid UTF-8")))?;
    let text = text.trim_end_matches('\0');
    text.parse::<u64>()
        .map_err(|_| Error::Protocol(format!("invalid {what}: {text:?}")))
}

/// Builds a `Server` error from an `ERROR` packet payload.
pub fn server_error(data: Vec<u8>) -> Error {
    let (code, message) = match split_nul(&data) {
        Some((code, message)) => (field_to_string(code), field_to_string(message)),
        None => (field_to_string(&data), String::new()),
    };
    Error::Server { code, message }
}

pub(crate) fn ensure_no_nul(value: &str, field: &str) -> Result<()> {
    if value.as_bytes().contains(&0) {
        return Err(Error::InvalidArgument(format!(
            "{field} must not contain a NUL byte"
        )));
    }
    Ok(())
}

pub(crate) fn trim_trailing_nul(mut data: &[u8]) -> &[u8] {
    while data.last() == Some(&0) {
        data = &data[..data.len() - 1];
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_round_trip() {
        let packet = Packet::with_data(PacketType::SubmitJob, b"reverse\0\0hello");
        let mut bytes = Vec::new();
        packet.write_request(&mut bytes).unwrap();

        assert_eq!(&bytes[..4], &MAGIC_REQUEST);
        assert_eq!(&bytes[4..8], &7u32.to_be_bytes());
        assert_eq!(&bytes[8..12], &14u32.to_be_bytes());

        let decoded = Packet::read_request(&mut bytes.as_slice(), 1024).unwrap();
        assert_eq!(decoded, packet);
    }

    #[test]
    fn rejects_oversized_packet() {
        let packet = Packet::with_data(PacketType::EchoRes, vec![0u8; 32]);
        let mut bytes = Vec::new();
        packet.write_response(&mut bytes).unwrap();

        let error = Packet::read_response(&mut bytes.as_slice(), 8).unwrap_err();
        assert!(matches!(error, Error::PacketTooLarge { size: 32, max: 8 }));
    }

    #[test]
    fn rejects_wrong_magic() {
        let packet = Packet::with_data(PacketType::Noop, Vec::new());
        let mut bytes = Vec::new();
        packet.write_response(&mut bytes).unwrap();

        let error = Packet::read_request(&mut bytes.as_slice(), 1024).unwrap_err();
        assert!(matches!(error, Error::InvalidMagic { got } if got == MAGIC_RESPONSE));
    }

    #[test]
    fn splits_fields() {
        let fields = split_fields::<3>(b"handle\0function\0binary\0payload").unwrap();
        assert_eq!(
            fields,
            [&b"handle"[..], &b"function"[..], &b"binary\0payload"[..]]
        );
    }

    #[test]
    fn packet_type_round_trip() {
        for value in 1..=42 {
            let kind = PacketType::from_u32(value).unwrap();
            assert_eq!(kind.as_u32(), value);
        }
        assert!(PacketType::from_u32(0).is_none());
        assert!(PacketType::from_u32(43).is_none());
    }
}
