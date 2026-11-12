//! TLS configuration for the client, worker and admin APIs.
//!
//! TLS is enabled with the `tls` feature for the synchronous APIs and with the
//! `tokio-tls` feature for the asynchronous APIs. Certificates are verified
//! against the platform trust store by default. Use
//! [`TlsConfig::add_root_certificate_pem`] or
//! [`TlsConfig::add_root_certificate_der`] for a private CA, or
//! [`TlsConfig::danger_accept_invalid_certs`] for self-signed development
//! servers.
//!
//! ```no_run
//! # fn main() -> gearman_rs::Result<()> {
//! use gearman_rs::{Client, TlsConfig};
//!
//! let tls = TlsConfig::new("gearman.example.com");
//! let mut client = Client::builder()
//!     .tls(tls)
//!     .connect("gearman.example.com:4730")?;
//! # let _ = client;
//! # Ok(())
//! # }
//! ```

use std::fmt;

use crate::{Error, Result};

/// TLS settings used by the sync and async connection builders.
///
/// The server name is used for SNI and certificate verification. It may differ
/// from the address passed to `connect`; for example, connect to
/// `127.0.0.1:4730` while verifying a certificate for `gearman.example.com`.
#[derive(Clone)]
pub struct TlsConfig {
    server_name: String,
    root_certificates: Vec<native_tls::Certificate>,
    accept_invalid_certs: bool,
    accept_invalid_hostnames: bool,
}

impl TlsConfig {
    /// Creates a configuration that verifies the certificate for `server_name`
    /// using the platform trust store.
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            root_certificates: Vec::new(),
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
        }
    }

    /// Returns the server name used for SNI and certificate verification.
    pub(crate) fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Adds one or more PEM-encoded root certificates.
    ///
    /// The buffer may contain multiple concatenated certificates. The
    /// certificates are added to the platform trust store rather than
    /// replacing it.
    pub fn add_root_certificate_pem(mut self, pem: impl AsRef<[u8]>) -> Result<Self> {
        let certificates = native_tls::Certificate::stack_from_pem(pem.as_ref())?;
        self.root_certificates.extend(certificates);
        Ok(self)
    }

    /// Adds a single DER-encoded root certificate.
    pub fn add_root_certificate_der(mut self, der: impl AsRef<[u8]>) -> Result<Self> {
        self.root_certificates
            .push(native_tls::Certificate::from_der(der.as_ref())?);
        Ok(self)
    }

    /// Disables certificate chain verification.
    ///
    /// This should only be used for local development or testing.
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// Disables hostname verification.
    ///
    /// This should only be used for local development or testing.
    pub fn danger_accept_invalid_hostnames(mut self, accept: bool) -> Self {
        self.accept_invalid_hostnames = accept;
        self
    }

    pub(crate) fn connector(&self) -> Result<native_tls::TlsConnector> {
        if self.server_name.is_empty() {
            return Err(Error::InvalidArgument(
                "TLS server name must not be empty".to_owned(),
            ));
        }

        let mut builder = native_tls::TlsConnector::builder();
        for certificate in &self.root_certificates {
            builder.add_root_certificate(certificate.clone());
        }
        builder.danger_accept_invalid_certs(self.accept_invalid_certs);
        builder.danger_accept_invalid_hostnames(self.accept_invalid_hostnames);
        Ok(builder.build()?)
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsConfig")
            .field("server_name", &self.server_name)
            .field("root_certificates", &self.root_certificates.len())
            .field("accept_invalid_certs", &self.accept_invalid_certs)
            .field("accept_invalid_hostnames", &self.accept_invalid_hostnames)
            .finish()
    }
}
