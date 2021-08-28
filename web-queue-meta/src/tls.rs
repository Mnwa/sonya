use openssl::ssl::{SslAcceptor, SslAcceptorBuilder, SslFiletype, SslMethod};

struct TlsOptions {
    private_key: String,
    cert: String,
}

pub fn get_options_from_env() -> Option<SslAcceptorBuilder> {
    let options = std::env::var("PRIVATE_KEY_PATH")
        .and_then(|private_key| {
            std::env::var("CERT_PATH").map(|cert| TlsOptions { private_key, cert })
        })
        .ok()?;

    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file(options.private_key, SslFiletype::PEM)
        .expect("private key not found");
    builder
        .set_certificate_chain_file(options.cert)
        .expect("cert not found");

    Some(builder)
}
