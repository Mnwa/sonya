use crate::config::Tls;
use openssl::ssl::{SslAcceptor, SslAcceptorBuilder, SslFiletype, SslMethod};

pub fn get_options_from_config(opts: Tls) -> SslAcceptorBuilder {
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder
        .set_private_key_file(opts.private_key, SslFiletype::PEM)
        .expect("private key not found");
    builder
        .set_certificate_chain_file(opts.cert)
        .expect("cert not found");

    builder
}
