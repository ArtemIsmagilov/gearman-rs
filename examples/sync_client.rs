use gearman_rs::Client;

fn main() -> gearman_rs::Result<()> {
    let mut client = Client::connect("127.0.0.1:4730")?;

    let result = client.execute("reverse", b"hello gearman")?;
    println!("handle: {}", result.handle);
    println!("result: {}", String::from_utf8_lossy(&result.data));

    Ok(())
}
