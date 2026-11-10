use gearman_rs::AsyncClient;

#[tokio::main]
async fn main() -> gearman_rs::Result<()> {
    let mut client = AsyncClient::connect("127.0.0.1:4730").await?;

    let result = client.execute("reverse", b"hello async gearman").await?;
    println!("handle: {}", result.handle);
    println!("result: {}", String::from_utf8_lossy(&result.data));

    Ok(())
}
