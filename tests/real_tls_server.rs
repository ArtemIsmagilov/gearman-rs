use gearman_rs::{AsyncClient, Client, TlsConfig};

#[test]
fn test_tls_connection() -> gearman_rs::Result<()> {
    let mut c = Client::builder()
        .tls(
            TlsConfig::new("localhost")
                .add_root_certificate_pem(include_bytes!("../tests/certs/ca.pem"))?,
        )
        .connect("127.0.0.1:4731")?;
    let result = c.echo("ping")?;
    assert_eq!(result, b"ping");
    Ok(())
}

#[tokio::test]
async fn test_async_tls_connection() -> gearman_rs::Result<()> {
    let mut c = AsyncClient::builder()
        .tls(
            TlsConfig::new("localhost")
                .add_root_certificate_pem(include_bytes!("../tests/certs/ca.pem"))?,
        )
        .connect("127.0.0.1:4731")
        .await?;
    let result = c.echo("ping").await?;
    assert_eq!(result, b"ping");
    Ok(())
}
