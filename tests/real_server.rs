use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gearman_rs::{AdminClient, Client, Error, Worker};

fn unique_function(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("gearman_rs_{prefix}_{:?}_{nanos}", thread::current().id())
}

#[test]
fn foreground_job_round_trip() {
    let function = unique_function("foreground");
    let (ready_tx, ready_rx) = mpsc::channel();

    let worker_function = function.clone();
    let worker = thread::spawn(move || -> Result<(), Error> {
        let mut worker = Worker::builder()
            .read_timeout(Duration::from_secs(10))
            .connect("127.0.0.1:4730")?;
        worker.register(&worker_function)?;
        ready_tx.send(()).unwrap();
        worker.run_once(|job| {
            let mut data = job.workload().to_vec();
            data.reverse();
            job.complete(data)
        })
    });

    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut client = Client::builder()
        .read_timeout(Duration::from_secs(10))
        .connect("127.0.0.1:4730")
        .unwrap();
    let result = client.execute(&function, b"hello").unwrap();
    assert_eq!(result.data, b"olleh");

    worker.join().unwrap().unwrap();
}

#[test]
fn background_status_and_unique_status() {
    let function = unique_function("background");
    let unique = format!("{function}-unique");

    let mut client = Client::builder()
        .read_timeout(Duration::from_secs(10))
        .connect("127.0.0.1:4730")
        .unwrap();
    let handle = client
        .submit_job_bg_unique(&function, &unique, b"payload")
        .unwrap();

    let status = client.job_status(&handle).unwrap();
    assert!(status.known);
    assert!(!status.running);

    let unique_status = client.job_status_unique(&unique).unwrap();
    assert!(unique_status.known);
    assert!(!unique_status.running);

    let mut admin = AdminClient::builder()
        .read_timeout(Duration::from_secs(10))
        .connect("127.0.0.1:4730")
        .unwrap();
    admin.cancel_job(handle.as_str()).unwrap();
}

#[test]
fn reduce_job_round_trip() {
    let function = unique_function("reduce");
    let unique = format!("{function}-unique");
    let (ready_tx, ready_rx) = mpsc::channel();

    let worker_function = function.clone();
    let worker_unique = unique.clone();
    let worker = thread::spawn(move || -> Result<(), Error> {
        let mut worker = Worker::builder()
            .read_timeout(Duration::from_secs(10))
            .connect("127.0.0.1:4730")?;
        worker.register(&worker_function)?;
        ready_tx.send(()).unwrap();

        let job = loop {
            if let Some(job) = worker.grab_job_all()? {
                break job;
            }
            worker.pre_sleep()?;
        };
        assert_eq!(job.function, worker_function);
        assert_eq!(job.unique.as_deref(), Some(worker_unique.as_str()));
        assert_eq!(job.reducer.as_deref(), Some("sum"));
        assert_eq!(job.workload, b"parts");
        worker.send_work_status(&job.handle, 1, 1)?;
        worker.send_work_complete(&job.handle, b"reduced")
    });

    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut client = Client::builder()
        .read_timeout(Duration::from_secs(10))
        .connect("127.0.0.1:4730")
        .unwrap();
    let handle = client
        .submit_reduce_job(&function, &unique, "sum", b"parts")
        .unwrap();
    let result = client.wait(&handle).unwrap();
    assert_eq!(result.data, b"reduced");

    worker.join().unwrap().unwrap();
}

#[test]
fn echo_option_and_admin_commands() {
    let mut client = Client::builder()
        .read_timeout(Duration::from_secs(10))
        .connect("127.0.0.1:4730")
        .unwrap();
    assert_eq!(client.echo(b"ping").unwrap(), b"ping");
    assert!(client.supports_option("exceptions").unwrap());

    let mut admin = AdminClient::builder()
        .read_timeout(Duration::from_secs(10))
        .connect("127.0.0.1:4730")
        .unwrap();
    assert!(!admin.version().unwrap().is_empty());
    assert!(admin.get_pid().unwrap() > 0);
    let _ = admin.status().unwrap();
    let _ = admin.priority_status().unwrap();
    let _ = admin.workers().unwrap();
    let _ = admin.show_jobs().unwrap();
}

#[cfg(feature = "tokio")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_client_and_worker() {
    use gearman_rs::{AsyncClient, AsyncWorker};
    use tokio::sync::oneshot;

    let function = unique_function("async");
    let (ready_tx, ready_rx) = oneshot::channel();

    let worker_function = function.clone();
    let worker = tokio::spawn(async move {
        let mut worker = AsyncWorker::builder()
            .read_timeout(Duration::from_secs(10))
            .connect("127.0.0.1:4730")
            .await?;
        worker.register(&worker_function).await?;
        ready_tx.send(()).unwrap();

        let mut job = worker.next_job().await?;
        let mut data = job.workload().to_vec();
        data.reverse();
        job.complete(data).await
    });

    ready_rx.await.unwrap();

    let mut client = AsyncClient::builder()
        .read_timeout(Duration::from_secs(10))
        .connect("127.0.0.1:4730")
        .await
        .unwrap();
    let result = client.execute(&function, b"hello").await.unwrap();
    assert_eq!(result.data, b"olleh");

    worker.await.unwrap().unwrap();
}
