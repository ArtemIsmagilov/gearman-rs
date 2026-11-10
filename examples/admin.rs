use gearman_rs::AdminClient;

fn main() -> gearman_rs::Result<()> {
    let mut admin = AdminClient::connect("127.0.0.1:4730")?;

    println!("version: {}", admin.version()?);

    for function in admin.status()? {
        println!(
            "{}: queued={}, running={}, workers={}",
            function.function, function.total, function.running, function.workers
        );
    }

    Ok(())
}
