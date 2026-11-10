use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connect_timeout: None,
            read_timeout: None,
            write_timeout: None,
            nodelay: true,
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
        }
    }
}

pub(crate) fn connect_tcp<A: ToSocketAddrs>(address: A, config: &Config) -> Result<TcpStream> {
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

pub(crate) struct Connection {
    stream: TcpStream,
    max_packet_size: usize,
}

impl Connection {
    pub(crate) fn connect<A: ToSocketAddrs>(address: A, config: &Config) -> Result<Self> {
        Ok(Self {
            stream: connect_tcp(address, config)?,
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
        self.stream.set_read_timeout(timeout)?;
        Ok(())
    }

    pub(crate) fn set_write_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.stream.set_write_timeout(timeout)?;
        Ok(())
    }

    pub(crate) fn set_nodelay(&mut self, nodelay: bool) -> Result<()> {
        self.stream.set_nodelay(nodelay)?;
        Ok(())
    }

    pub(crate) fn set_max_packet_size(&mut self, max_packet_size: usize) {
        self.max_packet_size = max_packet_size;
    }

    pub(crate) fn stream(&self) -> &TcpStream {
        &self.stream
    }

    pub(crate) fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
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
