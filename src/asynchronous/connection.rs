use std::time::Duration;
use tokio::net::ToSocketAddrs;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::net::Config;
use crate::protocol::{self, HEADER_SIZE, MAGIC_RESPONSE, Packet, PacketType};

pub(crate) struct Connection {
    stream: TcpStream,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    max_packet_size: usize,
}

impl Connection {
    pub(crate) async fn connect<A: ToSocketAddrs>(address: A, config: &Config) -> Result<Self> {
        let stream = connect_stream(address, config).await?;
        Ok(Self {
            stream,
            read_timeout: config.read_timeout,
            write_timeout: config.write_timeout,
            max_packet_size: config.max_packet_size,
        })
    }

    pub(crate) async fn send(&mut self, kind: PacketType, data: &[u8]) -> Result<()> {
        let mut bytes = Vec::with_capacity(HEADER_SIZE + data.len());
        protocol::write_packet(&mut bytes, protocol::MAGIC_REQUEST, kind, data)?;

        match self.write_timeout {
            Some(timeout) => tokio::time::timeout(timeout, async {
                self.stream.write_all(&bytes).await?;
                self.stream.flush().await
            })
            .await
            .map_err(|_| Error::Timeout("writing to the server"))??,
            None => {
                self.stream.write_all(&bytes).await?;
                self.stream.flush().await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn recv(&mut self) -> Result<Packet> {
        match self.read_timeout {
            Some(timeout) => {
                tokio::time::timeout(timeout, read_packet(&mut self.stream, self.max_packet_size))
                    .await
                    .map_err(|_| Error::Timeout("reading from the server"))?
            }
            None => read_packet(&mut self.stream, self.max_packet_size).await,
        }
    }

    pub(crate) fn set_read_timeout(&mut self, timeout: Option<Duration>) {
        self.read_timeout = timeout;
    }

    pub(crate) fn set_write_timeout(&mut self, timeout: Option<Duration>) {
        self.write_timeout = timeout;
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

async fn read_packet(stream: &mut TcpStream, max_packet_size: usize) -> Result<Packet> {
    let mut header = [0u8; HEADER_SIZE];
    read_exact(stream, &mut header).await?;

    let magic: [u8; 4] = header[0..4].try_into().expect("fixed-size slice");
    if magic != MAGIC_RESPONSE {
        return Err(Error::InvalidMagic { got: magic });
    }

    let raw_type = u32::from_be_bytes(header[4..8].try_into().expect("fixed-size slice"));
    let size = u32::from_be_bytes(header[8..12].try_into().expect("fixed-size slice")) as usize;

    if size > max_packet_size {
        return Err(Error::PacketTooLarge {
            size,
            max: max_packet_size,
        });
    }

    let kind = PacketType::from_u32(raw_type)
        .ok_or_else(|| Error::Protocol(format!("unknown packet type {raw_type}")))?;

    let mut data = vec![0u8; size];
    if size > 0 {
        read_exact(stream, &mut data).await?;
    }

    Ok(Packet { kind, data })
}

async fn read_exact(stream: &mut TcpStream, buffer: &mut [u8]) -> Result<()> {
    stream.read_exact(buffer).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::ConnectionClosed
        } else {
            Error::map_io_timeout(error, "reading from the server")
        }
    })?;
    Ok(())
}

pub(crate) async fn connect_stream<A: ToSocketAddrs>(
    address: A,
    config: &Config,
) -> Result<TcpStream> {
    let connect = TcpStream::connect(address);
    let stream = match config.connect_timeout {
        Some(timeout) => tokio::time::timeout(timeout, connect)
            .await
            .map_err(|_| Error::Timeout("connecting"))??,
        None => connect.await?,
    };
    stream.set_nodelay(config.nodelay)?;
    Ok(stream)
}
