use gearman_rs::AsyncWorker;

#[tokio::main]
async fn main() -> gearman_rs::Result<()> {
    let mut worker = AsyncWorker::connect("127.0.0.1:4730").await?;

    worker.set_client_id("gearman-rs-async-example").await?;
    worker.register("reverse").await?;
    println!("worker ready");

    worker
        .run(async |job| {
            println!("processing {} ({})", job.handle(), job.function());
            job.send_status(0, 2).await?;
            let mut result = job.workload().to_vec();
            result.reverse();
            job.send_status(1, 2).await?;
            job.complete(result).await
        })
        .await?;

    Ok(())
}
