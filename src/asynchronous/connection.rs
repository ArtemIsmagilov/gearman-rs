use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::ToSocketAddrs;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::net::Config;
use crate::protocol::{self, HEADER_SIZE, MAGIC_RESPONSE, Packet, PacketType};

pub(crate) enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tokio-tls")]
    Tls(Box<tokio_native_tls::TlsStream<TcpStream>>),
}

impl Stream {
    pub(crate) fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            #[cfg(feature = "tokio-tls")]
            Self::Tls(stream) => stream.get_ref().get_ref().get_ref(),
        }
    }

    pub(crate) fn tcp_mut(&mut self) -> &mut TcpStream {
        match self {
            Self::Plain(stream) => stream,
            #[cfg(feature = "tokio-tls")]
            Self::Tls(stream) => stream.get_mut().get_mut().get_mut(),
        }
    }

    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        AsyncWriteExt::shutdown(self).await?;
        Ok(())
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(context, buffer),
            #[cfg(feature = "tokio-tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(context, buffer),
            #[cfg(feature = "tokio-tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(context, buffer),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(context),
            #[cfg(feature = "tokio-tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(context),
            #[cfg(feature = "tokio-tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(context),
        }
    }
}

pub(crate) struct Connection {
    stream: Stream,
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
        self.stream.tcp()
    }

    pub(crate) fn stream_mut(&mut self) -> &mut TcpStream {
        self.stream.tcp_mut()
    }

    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        self.stream.shutdown().await
    }
}

async fn read_packet<S>(stream: &mut S, max_packet_size: usize) -> Result<Packet>
where
    S: AsyncRead + Unpin,
{
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

async fn read_exact<S>(stream: &mut S, buffer: &mut [u8]) -> Result<()>
where
    S: AsyncRead + Unpin,
{
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
) -> Result<Stream> {
    let connect = TcpStream::connect(address);
    let stream = match config.connect_timeout {
        Some(timeout) => tokio::time::timeout(timeout, connect)
            .await
            .map_err(|_| Error::Timeout("connecting"))??,
        None => connect.await?,
    };
    stream.set_nodelay(config.nodelay)?;

    #[cfg(feature = "tokio-tls")]
    if let Some(tls) = &config.tls {
        let connector = tokio_native_tls::TlsConnector::from(tls.connector()?);
        let stream = connector
            .connect(tls.server_name(), stream)
            .await
            .map_err(Error::Tls)?;
        return Ok(Stream::Tls(Box::new(stream)));
    }

    Ok(Stream::Plain(stream))
}
