use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use gearman_rs::protocol::{Packet, PacketType, split_fields};
use gearman_rs::{AdminClient, Client, Error, Worker};

fn listener() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
    let address = listener.local_addr().expect("local address");
    (listener, address)
}

fn configure(stream: &TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("write timeout");
}

fn write_packet(stream: &mut TcpStream, kind: PacketType, data: &[u8]) {
    Packet::with_data(kind, data)
        .write_response(stream)
        .unwrap();
    stream.flush().unwrap();
}

#[test]
fn client_foreground_round_trip() {
    let (listener, address) = listener();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        configure(&stream);

        let request = Packet::read_request(&mut stream, 1 << 20).unwrap();
        assert_eq!(request.kind, PacketType::SubmitJob);
        let fields = split_fields::<3>(&request.data).unwrap();
        assert_eq!(fields[0], b"reverse");
        assert_eq!(fields[1], b"");
        assert_eq!(fields[2], b"hello");

        write_packet(&mut stream, PacketType::JobCreated, b"H:mock:1");
        write_packet(&mut stream, PacketType::WorkData, b"H:mock:1\0chunk");
        write_packet(&mut stream, PacketType::WorkWarning, b"H:mock:1\0careful");
        write_packet(&mut stream, PacketType::WorkStatus, b"H:mock:1\x001\x002");
        write_packet(&mut stream, PacketType::WorkComplete, b"H:mock:1\0olleh");
    });

    let mut client = Client::builder()
        .read_timeout(Duration::from_secs(5))
        .connect(address)
        .unwrap();

    let result = client.execute("reverse", b"hello").unwrap();
    assert_eq!(result.handle.as_str(), "H:mock:1");
    assert_eq!(result.data, b"olleh");
    assert_eq!(result.streamed_data, vec![b"chunk".to_vec()]);
    assert_eq!(result.warnings, vec![b"careful".to_vec()]);
    let status = result.status.unwrap();
    assert_eq!((status.numerator, status.denominator), (1, 2));

    server.join().unwrap();
}

#[test]
fn worker_round_trip() {
    let (listener, address) = listener();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        configure(&stream);

        let can_do = Packet::read_request(&mut stream, 1 << 20).unwrap();
        assert_eq!(can_do.kind, PacketType::CanDo);
        assert_eq!(can_do.data, b"reverse");

        let grab = Packet::read_request(&mut stream, 1 << 20).unwrap();
        assert_eq!(grab.kind, PacketType::GrabJobUniq);

        write_packet(
            &mut stream,
            PacketType::JobAssignUniq,
            b"H:mock:2\0reverse\0uid-42\0payload",
        );

        let data = Packet::read_request(&mut stream, 1 << 20).unwrap();
        assert_eq!(data.kind, PacketType::WorkData);
        assert_eq!(data.data, b"H:mock:2\0partial");

        let complete = Packet::read_request(&mut stream, 1 << 20).unwrap();
        assert_eq!(complete.kind, PacketType::WorkComplete);
        assert_eq!(complete.data, b"H:mock:2\0result");
    });

    let mut worker = Worker::builder()
        .read_timeout(Duration::from_secs(5))
        .connect(address)
        .unwrap();

    worker.register("reverse").unwrap();
    let job = worker.grab_job_unique().unwrap().expect("job assignment");
    assert_eq!(job.handle.as_str(), "H:mock:2");
    assert_eq!(job.function, "reverse");
    assert_eq!(job.unique.as_deref(), Some("uid-42"));
    assert_eq!(job.workload, b"payload");

    worker.send_work_data(&job.handle, b"partial").unwrap();
    worker.send_work_complete(&job.handle, b"result").unwrap();

    server.join().unwrap();
}

#[test]
fn admin_status_and_version() {
    let (listener, address) = listener();

    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        configure(&stream);
        let mut reader = BufReader::new(stream);

        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line, "status\n");
        reader
            .get_mut()
            .write_all(b"reverse\t2\t1\t3\r\n.\r\n")
            .unwrap();

        line.clear();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line, "version\n");
        reader.get_mut().write_all(b"OK 1.1.21\n").unwrap();
    });

    let mut admin = AdminClient::builder()
        .read_timeout(Duration::from_secs(5))
        .connect(address)
        .unwrap();

    let status = admin.status().unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].function, "reverse");
    assert_eq!(status[0].total, 2);
    assert_eq!(status[0].running, 1);
    assert_eq!(status[0].workers, 3);
    assert_eq!(admin.version().unwrap(), "1.1.21");

    server.join().unwrap();
}

#[test]
fn admin_error_is_reported() {
    let (listener, address) = listener();

    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        configure(&stream);
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line, "bogus\n");
        reader
            .get_mut()
            .write_all(b"ERR UNKNOWN_COMMAND Unknown command\n")
            .unwrap();
    });

    let mut admin = AdminClient::builder()
        .read_timeout(Duration::from_secs(5))
        .connect(address)
        .unwrap();

    match admin.command("bogus") {
        Err(Error::AdminServer { code, .. }) => assert_eq!(code, "UNKNOWN_COMMAND"),
        other => panic!("unexpected result: {other:?}"),
    }

    server.join().unwrap();
}

#[cfg(feature = "tokio")]
#[tokio::test]
async fn async_client_foreground_round_trip() {
    use gearman_rs::AsyncClient;

    let (listener, address) = listener();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        configure(&stream);

        let request = Packet::read_request(&mut stream, 1 << 20).unwrap();
        assert_eq!(request.kind, PacketType::SubmitJob);
        let fields = split_fields::<3>(&request.data).unwrap();
        assert_eq!(fields[0], b"reverse");
        assert_eq!(fields[2], b"hello");

        write_packet(&mut stream, PacketType::JobCreated, b"H:mock:3");
        write_packet(&mut stream, PacketType::WorkComplete, b"H:mock:3\0olleh");
    });

    let mut client = AsyncClient::builder()
        .read_timeout(Duration::from_secs(5))
        .connect(address)
        .await
        .unwrap();

    let result = client.execute("reverse", b"hello").await.unwrap();
    assert_eq!(result.data, b"olleh");

    server.join().unwrap();
}
