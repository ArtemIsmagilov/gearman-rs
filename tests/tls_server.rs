#![cfg(feature = "tls")]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use gearman_rs::protocol::{Packet, PacketType};
use gearman_rs::{AdminClient, Client, Error, TlsConfig, Worker};

const CERT_PEM: &[u8] = include_bytes!("certs/localhost.crt");
const KEY_PEM: &[u8] = include_bytes!("certs/localhost.key");

fn listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind TLS server");
    let address = listener.local_addr().expect("local address");
    (listener, address)
}

fn acceptor() -> native_tls::TlsAcceptor {
    let identity = native_tls::Identity::from_pkcs8(CERT_PEM, KEY_PEM).expect("test identity");
    native_tls::TlsAcceptor::new(identity).expect("TLS acceptor")
}

fn tls_config() -> TlsConfig {
    TlsConfig::new("localhost")
        .add_root_certificate_pem(CERT_PEM)
        .expect("test CA")
}

fn spawn_server<F>(handler: F) -> (SocketAddr, JoinHandle<()>)
where
    F: FnOnce(&mut native_tls::TlsStream<TcpStream>) + Send + 'static,
{
    let (listener, address) = listener();
    let acceptor = acceptor();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept connection");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .expect("write timeout");
        let mut stream = acceptor.accept(stream).expect("TLS handshake");
        handler(&mut stream);
    });
    (address, server)
}

fn read_line(stream: &mut impl Read) -> String {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).expect("read admin command");
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
    }
    String::from_utf8(line).expect("UTF-8 command")
}

#[test]
fn sync_client_uses_tls() {
    let (address, server) = spawn_server(|stream| {
        let request = Packet::read_request(stream, 1 << 20).expect("request");
        assert_eq!(request.kind, PacketType::SubmitJob);

        Packet::with_data(PacketType::JobCreated, b"H:tls:1")
            .write_response(stream)
            .expect("job created");
        Packet::with_data(PacketType::WorkComplete, b"H:tls:1\0olleh")
            .write_response(stream)
            .expect("work complete");
        stream.flush().expect("flush");
    });

    let mut client = Client::builder()
        .tls(tls_config())
        .connect(address)
        .expect("TLS client");
    let result = client.execute("reverse", b"hello").expect("job result");
    assert_eq!(result.data, b"olleh");

    server.join().expect("TLS server");
}

#[test]
fn sync_worker_uses_tls() {
    let (address, server) = spawn_server(|stream| {
        let request = Packet::read_request(stream, 1 << 20).expect("request");
        assert_eq!(request.kind, PacketType::CanDo);
        assert_eq!(request.data, b"reverse");
    });

    let mut worker = Worker::builder()
        .tls(tls_config())
        .connect(address)
        .expect("TLS worker");
    worker.register("reverse").expect("register");

    server.join().expect("TLS server");
}

#[test]
fn sync_admin_client_uses_tls() {
    let (address, server) = spawn_server(|stream| {
        assert_eq!(read_line(stream), "version");
        stream.write_all(b"OK 2.2.0\n").expect("write response");
        stream.flush().expect("flush");
    });

    let mut admin = AdminClient::builder()
        .tls(tls_config())
        .connect(address)
        .expect("TLS admin client");
    assert_eq!(admin.version().expect("version"), "2.2.0");

    server.join().expect("TLS server");
}

#[test]
fn untrusted_certificate_is_rejected() {
    let (listener, address) = listener();
    let acceptor = acceptor();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept connection");
        let _ = acceptor.accept(stream);
    });

    let result = Client::builder()
        .tls(TlsConfig::new("localhost"))
        .connect(address);
    assert!(matches!(result, Err(Error::Tls(_))));

    server.join().expect("TLS server");
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn async_client_uses_tls() {
    use gearman_rs::AsyncClient;

    let (address, server) = spawn_server(|stream| {
        let request = Packet::read_request(stream, 1 << 20).expect("request");
        assert_eq!(request.kind, PacketType::SubmitJob);

        Packet::with_data(PacketType::JobCreated, b"H:tls:2")
            .write_response(stream)
            .expect("job created");
        Packet::with_data(PacketType::WorkComplete, b"H:tls:2\0olleh")
            .write_response(stream)
            .expect("work complete");
        stream.flush().expect("flush");
    });

    let mut client = AsyncClient::builder()
        .tls(tls_config())
        .connect(address)
        .await
        .expect("TLS async client");
    let result = client
        .execute("reverse", b"hello")
        .await
        .expect("job result");
    assert_eq!(result.data, b"olleh");

    server.join().expect("TLS server");
}
