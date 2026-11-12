# gearman-rs

[![ci](https://github.com/ArtemIsmagilov/gearman-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/ArtemIsmagilov/gearman-rs/actions/workflows/ci.yaml)
[![crates.io](https://img.shields.io/crates/v/gearman-rs.svg)](https://crates.io/crates/gearman-rs)
[![docs.rs](https://img.shields.io/docsrs/gearman-rs)](https://docs.rs/gearman-rs)

A complete, ergonomic Rust implementation of the [Gearman](https://gearman.org/)
job queue protocol.

The crate implements the whole binary protocol used by clients and workers, the
text-based administrative protocol, and provides both synchronous and
asynchronous (Tokio) APIs.

## Features

- **Full binary protocol** — every client, worker and server response packet
  defined by Gearman, including reduce jobs, scheduled jobs, unique status
  queries, job streaming and exceptions.
- **Convenient client API** — foreground/background jobs, priorities, unique
  ids, scheduling, reduce jobs, status queries, options and echo.
- **Convenient worker API** — function registration, execution timeouts,
  `GRAB_JOB`/`GRAB_JOB_UNIQ`/`GRAB_JOB_ALL`, `PRE_SLEEP`, progress updates,
  warnings, data streaming and exceptions.
- **Admin protocol** — `status`, `prioritystatus`, `workers`, `version`,
  `maxqueue`, `show jobs`, `cancel job` and more.
- **Sync and async** — `Client`/`Worker` by default, `AsyncClient`/
  `AsyncWorker` with the `tokio` feature.
- **Typed and low-level APIs** — use the high-level helpers or send raw packets
  through `protocol`.
- **Configurable timeouts and packet-size limits**.
- `#![forbid(unsafe_code)]`.

## Installation

```toml
[dependencies]
gearman-rs = "0.1"
```

For the asynchronous API:

```toml
[dependencies]
gearman-rs = { version = "0.1", features = ["tokio"] }
```

The feature is also available under the alias `async`.

TLS support is opt-in and split between the synchronous and asynchronous APIs:

```toml
[dependencies]
# TLS for the synchronous client, worker and admin client.
gearman-rs = { version = "0.1", features = ["tls"] }
```

```toml
[dependencies]
# TLS for the asynchronous API (implies `tls` and `tokio`).
gearman-rs = { version = "0.1", features = ["tokio-tls"] }
```

## Client quick start

```rust,no_run
use gearman_rs::Client;

fn main() -> Result<(), gearman_rs::Error> {
    let mut client = Client::connect("127.0.0.1:4730")?;

    // Submit a foreground job and wait for the result.
    let result = client.execute("reverse", b"hello")?;
    println!("{}", String::from_utf8_lossy(&result.data));

    Ok(())
}
```

Submit without blocking, then wait later:

```rust,no_run
# use gearman_rs::{Client, Error};
# fn main() -> Result<(), Error> {
let mut client = Client::connect("127.0.0.1:4730")?;
let handle = client.submit_job("reverse", b"hello")?;
let result = client.wait(&handle)?;
# let _ = result;
# Ok(())
# }
```

Background jobs and status queries:

```rust,no_run
# use gearman_rs::{Client, Error};
# fn main() -> Result<(), Error> {
let mut client = Client::connect("127.0.0.1:4730")?;
let handle = client.submit_job_bg_unique("resize", "image-42", b"payload")?;

let status = client.job_status(&handle)?;
println!("known={} running={} progress={:?}", status.known, status.running, status.progress());

let status = client.job_status_unique("image-42")?;
# let _ = status;
# Ok(())
# }
```

## Job requests

`JobRequest` is a small builder for all submission variants:

```rust,no_run
use gearman_rs::{Client, JobRequest, Schedule};

# fn main() -> Result<(), gearman_rs::Error> {
let mut client = Client::connect("127.0.0.1:4730")?;

let request = JobRequest::new("render", b"frame 1")
    .unique("frame-1")
    .high()
    .background();

let handle = client.submit(&request)?;

// Scheduled jobs use the same builder.
let request = JobRequest::new("cleanup", b"")
    .unique("nightly")
    .schedule("30 3 * * *".parse::<Schedule>()?);
let handle = client.submit(&request)?;
# let _ = handle;
# Ok(())
# }
```

Supported variants:

| Request | API |
| --- | --- |
| `SUBMIT_JOB` / `_BG` | `submit_job`, `submit_job_bg`, `JobRequest::background` |
| `SUBMIT_JOB_HIGH` / `_HIGH_BG` | `submit_job_high`, `submit_job_high_bg`, `.high()` |
| `SUBMIT_JOB_LOW` / `_LOW_BG` | `submit_job_low`, `submit_job_low_bg`, `.low()` |
| `SUBMIT_JOB_SCHED` | `submit_job_sched`, `.schedule(...)` |
| `SUBMIT_JOB_EPOCH` | `submit_job_epoch`, `.run_at(...)` |
| `SUBMIT_REDUCE_JOB` / `_BG` | `submit_reduce_job`, `submit_reduce_job_bg`, `.reducer(...)` |

## Worker quick start

```rust,no_run
use gearman_rs::Worker;

fn main() -> Result<(), gearman_rs::Error> {
    let mut worker = Worker::connect("127.0.0.1:4730")?;
    worker.register("reverse")?;

    worker.run(|job| {
        let mut result = job.workload().to_vec();
        result.reverse();
        job.complete(result)
    })?;

    Ok(())
}
```

Handlers can stream progress and intermediate results:

```rust,no_run
# use gearman_rs::{Worker, Error};
# fn main() -> Result<(), Error> {
let mut worker = Worker::connect("127.0.0.1:4730")?;
worker.register_with_timeout("transcode", std::time::Duration::from_secs(60))?;

worker.run(|job| {
    job.send_status(0, 3)?;
    job.send_data(b"first chunk")?;
    job.send_warning(b"using slow codec")?;
    job.send_status(3, 3)?;
    job.complete(b"done")
})?;
# Ok(())
# }
```

A handler must finish every job with `complete`, `fail` or
`fail_with_exception`. If it returns without doing so, the job is marked as
failed. To process jobs manually, use `grab_job`, `grab_job_unique`,
`grab_job_all` and `pre_sleep`.

## Async API

```rust,no_run
use gearman_rs::AsyncClient;

#[tokio::main]
async fn main() -> Result<(), gearman_rs::Error> {
    let mut client = AsyncClient::connect("127.0.0.1:4730").await?;
    let result = client.execute("reverse", b"hello").await?;
    println!("{}", String::from_utf8_lossy(&result.data));
    Ok(())
}
```

```rust,no_run
use gearman_rs::AsyncWorker;

#[tokio::main]
async fn main() -> Result<(), gearman_rs::Error> {
    let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;
    worker.register("reverse").await?;

    worker.run(async |job| {
        let mut result = job.workload().to_vec();
        result.reverse();
        job.complete(result).await
    }).await?;

    Ok(())
}
```

## Admin protocol

```rust,no_run
use gearman_rs::AdminClient;

fn main() -> Result<(), gearman_rs::Error> {
    let mut admin = AdminClient::connect("127.0.0.1:4730")?;

    println!("gearmand {}", admin.version()?);

    for row in admin.status()? {
        println!(
            "{}: queued={} running={} workers={}",
            row.function, row.total, row.running, row.workers
        );
    }

    admin.cancel_job("H:host:42")?;
    Ok(())
}
```

`AdminClient` also exposes `priority_status`, `workers`, `max_queue`,
`show_jobs`, `show_unique_jobs`, `create_function`, `drop_function`, `get_pid`
and a generic `command` method.

## TLS

Enable the `tls` feature for the synchronous APIs and `tokio-tls` for the
asynchronous ones. The same `TlsConfig` is used by `Client`, `Worker`,
`AdminClient` and their async counterparts.

```rust,no_run
use gearman_rs::{Client, TlsConfig};

# fn main() -> Result<(), gearman_rs::Error> {
let tls = TlsConfig::new("gearman.example.com");
let mut client = Client::builder()
    .tls(tls)
    .connect("gearman.example.com:4730")?;

let result = client.execute("reverse", b"hello")?;
# let _ = result;
# Ok(())
# }
```

The asynchronous API uses the same configuration:

```rust,no_run
use gearman_rs::{AsyncClient, TlsConfig};

# #[tokio::main]
# async fn main() -> Result<(), gearman_rs::Error> {
let tls = TlsConfig::new("gearman.example.com");
let mut client = AsyncClient::builder()
    .tls(tls)
    .connect("gearman.example.com:4730")
    .await?;

let result = client.execute("reverse", b"hello").await?;
# let _ = result;
# Ok(())
# }
```

`TlsConfig::new` sets the name used for SNI and certificate verification; it
may differ from the address passed to `connect`. Certificates are checked
against the platform trust store by default. For a private CA:

```rust,no_run
# use gearman_rs::{Client, TlsConfig};
# fn main() -> Result<(), gearman_rs::Error> {
let tls = TlsConfig::new("gearman.internal")
    .add_root_certificate_pem(include_bytes!("ca.pem"))?;
let client = Client::builder()
    .tls(tls)
    .connect("10.0.0.10:4730");
# let _ = client;
# Ok(())
# }
```

`add_root_certificate_der` accepts a single DER certificate. Additional roots
are added to the platform trust store rather than replacing it. For local
self-signed servers, `danger_accept_invalid_certs(true)` and
`danger_accept_invalid_hostnames(true)` disable verification and must only be
used for development.

## Low-level protocol

The `protocol` module exposes typed packets and a synchronous codec:

```rust,no_run
use gearman_rs::protocol::{Packet, PacketType};

# fn main() -> Result<(), gearman_rs::Error> {
# let mut stream = std::io::sink();
Packet::with_data(PacketType::EchoReq, b"ping")
    .write_request(&mut stream)?;
# Ok(())
# }
```

`Client` and `Worker` also provide `send_packet`/`receive_packet` for advanced
use cases.

## Protocol coverage

Binary packet types `1` through `42` are implemented, including:

- client: `SUBMIT_JOB*`, `SUBMIT_JOB_SCHED`, `SUBMIT_JOB_EPOCH`,
  `SUBMIT_REDUCE_JOB*`, `GET_STATUS`, `GET_STATUS_UNIQUE`, `ECHO_REQ`,
  `OPTION_REQ`;
- worker: `CAN_DO`, `CAN_DO_TIMEOUT`, `CANT_DO`, `RESET_ABILITIES`,
  `SET_CLIENT_ID`, `ALL_YOURS`, `PRE_SLEEP`, `GRAB_JOB`, `GRAB_JOB_UNIQ`,
  `GRAB_JOB_ALL`, `WORK_DATA`, `WORK_WARNING`, `WORK_STATUS`,
  `WORK_COMPLETE`, `WORK_FAIL`, `WORK_EXCEPTION`;
- responses: `JOB_CREATED`, `NO_JOB`, `NOOP`, `JOB_ASSIGN*`, `STATUS_RES*`,
  `WORK_*`, `ECHO_RES`, `OPTION_RES`, `ERROR`.

## Tests

Unit and mock-server tests run without a Gearman server:

```bash
cargo test --features tokio-tls --lib --test mock_server --test tls_server
```

Documentation tests and integration tests use a real `gearmand` at
`127.0.0.1:4730` and a TLS-enabled `gearmand` at `127.0.0.1:4731`:

```bash
docker compose up -d
cargo test --features tokio-tls --doc --test real_server --test real_tls_server
```

## License

MIT
