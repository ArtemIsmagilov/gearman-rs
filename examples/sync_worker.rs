use gearman_rs::Worker;

fn main() -> gearman_rs::Result<()> {
    let mut worker = Worker::connect("127.0.0.1:4730")?;

    worker.set_client_id("gearman-rs-example")?;
    worker.register("reverse")?;
    println!("worker ready");

    worker.run(|job| {
        println!("processing {} ({})", job.handle(), job.function());
        job.send_status(0, 2)?;
        let mut result = job.workload().to_vec();
        result.reverse();
        job.send_status(1, 2)?;
        job.complete(result)
    })?;

    Ok(())
}
