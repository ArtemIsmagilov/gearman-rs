use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::protocol::{self, DEFAULT_MAX_PACKET_SIZE, MAGIC_RESPONSE, Packet, PacketType};

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) read_timeout: Option<Duration>,
    pub(crate) write_timeout: Option<Duration>,
    pub(crate) nodelay: bool,
    pub(crate) max_packet_size: usize,
    #[cfg(feature = "tls")]
    pub(crate) tls: Option<crate::TlsConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connect_timeout: None,
            read_timeout: None,
            write_timeout: None,
            nodelay: true,
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

pub(crate) enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<native_tls::TlsStream<TcpStream>>),
}

impl Stream {
    pub(crate) fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.get_ref(),
        }
    }

    pub(crate) fn tcp_mut(&mut self) -> &mut TcpStream {
        match self {
            Self::Plain(stream) => stream,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.get_mut(),
        }
    }

    pub(crate) fn is_tls(&self) -> bool {
        #[cfg(feature = "tls")]
        {
            matches!(self, Self::Tls(_))
        }
        #[cfg(not(feature = "tls"))]
        {
            false
        }
    }

    pub(crate) fn shutdown(&mut self) -> io::Result<()> {
        #[cfg(feature = "tls")]
        let tls_result = if let Self::Tls(stream) = self {
            stream.shutdown()
        } else {
            Ok(())
        };
        #[cfg(not(feature = "tls"))]
        let tls_result: io::Result<()> = Ok(());

        let tcp_result = self.tcp().shutdown(Shutdown::Both);
        tls_result.and(tcp_result)
    }
}

impl Read for Stream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.flush(),
        }
    }
}

fn connect_tcp<A: ToSocketAddrs>(address: A, config: &Config) -> Result<TcpStream> {
    let addresses: Vec<_> = address.to_socket_addrs()?.collect();
    if addresses.is_empty() {
        return Err(Error::InvalidArgument(
            "address resolved to zero socket addresses".to_owned(),
        ));
    }

    let mut last_error = None;
    for address in addresses {
        let result = match config.connect_timeout {
            Some(timeout) => TcpStream::connect_timeout(&address, timeout),
            None => TcpStream::connect(address),
        };

        match result {
            Ok(stream) => {
                stream.set_nodelay(config.nodelay)?;
                stream.set_read_timeout(config.read_timeout)?;
                stream.set_write_timeout(config.write_timeout)?;
                return Ok(stream);
            }
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error
        .map(|error| Error::map_io_timeout(error, "connecting"))
        .unwrap_or(Error::ConnectionClosed))
}

pub(crate) fn connect_stream<A: ToSocketAddrs>(address: A, config: &Config) -> Result<Stream> {
    let stream = connect_tcp(address, config)?;

    #[cfg(feature = "tls")]
    if let Some(tls) = &config.tls {
        let connector = tls.connector()?;

        // Bound the handshake by the connect timeout in addition to any
        // configured read/write timeouts, then restore the read/write timeouts
        // for normal operation.
        stream.set_read_timeout(handshake_timeout(
            config.read_timeout,
            config.connect_timeout,
        ))?;
        stream.set_write_timeout(handshake_timeout(
            config.write_timeout,
            config.connect_timeout,
        ))?;

        let stream = match connector.connect(tls.server_name(), stream) {
            Ok(stream) => stream,
            Err(native_tls::HandshakeError::Failure(error)) => return Err(Error::Tls(error)),
            Err(native_tls::HandshakeError::WouldBlock(_)) => {
                return Err(Error::Protocol(
                    "TLS handshake was interrupted by a non-blocking socket".to_owned(),
                ));
            }
        };

        stream.get_ref().set_read_timeout(config.read_timeout)?;
        stream.get_ref().set_write_timeout(config.write_timeout)?;
        return Ok(Stream::Tls(Box::new(stream)));
    }

    Ok(Stream::Plain(stream))
}

#[cfg(feature = "tls")]
fn handshake_timeout(
    configured: Option<Duration>,
    connect_timeout: Option<Duration>,
) -> Option<Duration> {
    match (configured, connect_timeout) {
        (Some(configured), Some(connect)) => Some(configured.min(connect)),
        (configured, connect) => configured.or(connect),
    }
}

pub(crate) struct Connection {
    stream: Stream,
    max_packet_size: usize,
}

impl Connection {
    pub(crate) fn connect<A: ToSocketAddrs>(address: A, config: &Config) -> Result<Self> {
        Ok(Self {
            stream: connect_stream(address, config)?,
            max_packet_size: config.max_packet_size,
        })
    }

    pub(crate) fn send(&mut self, kind: PacketType, data: &[u8]) -> Result<()> {
        protocol::write_packet(&mut self.stream, protocol::MAGIC_REQUEST, kind, data)
            .map_err(map_write_error)?;
        self.stream
            .flush()
            .map_err(|error| Error::map_io_timeout(error, "flushing the request"))?;
        Ok(())
    }

    pub(crate) fn recv(&mut self) -> Result<Packet> {
        protocol::read_packet(&mut self.stream, MAGIC_RESPONSE, self.max_packet_size)
            .map_err(map_read_error)
    }

    pub(crate) fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.stream.tcp_mut().set_read_timeout(timeout)?;
        Ok(())
    }

    pub(crate) fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.stream.tcp_mut().set_write_timeout(timeout)?;
        Ok(())
    }

    pub(crate) fn set_nodelay(&mut self, nodelay: bool) -> Result<()> {
        self.stream.tcp_mut().set_nodelay(nodelay)?;
        Ok(())
    }

    pub(crate) fn set_max_packet_size(&mut self, max_packet_size: usize) {
        self.max_packet_size = max_packet_size;
    }

    pub(crate) fn stream(&self) -> &TcpStream {
        self.stream.tcp()
    }

    pub(crate) fn stream_mut(&mut self) -> &mut TcpStream {
        self.stream.tcp_mut()
    }

    pub(crate) fn is_tls(&self) -> bool {
        self.stream.is_tls()
    }

    pub(crate) fn shutdown(&mut self) -> Result<()> {
        self.stream.shutdown()?;
        Ok(())
    }
}

fn map_read_error(error: Error) -> Error {
    match error {
        Error::Io(io) => Error::map_io_timeout(io, "reading from the server"),
        other => other,
    }
}

fn map_write_error(error: Error) -> Error {
    match error {
        Error::Io(io) => Error::map_io_timeout(io, "writing to the server"),
        other => other,
    }
}
