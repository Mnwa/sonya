use crate::config::Tls;
use rustls::ServerConfig;
use rustls_pemfile::{certs, pkcs8_private_keys};
use std::fs::File;
use std::io::BufReader;

pub fn get_options_from_config(opts: Tls) -> ServerConfig {
    let cert_file = &mut BufReader::new(File::open(opts.cert).unwrap());
    let key_file = &mut BufReader::new(File::open(opts.private_key).unwrap());

    let cert = certs(cert_file)
        .unwrap()
        .into_iter()
        .map(rustls::Certificate)
        .collect();

    let keys = pkcs8_private_keys(key_file).unwrap();
    let key = rustls::PrivateKey(keys[0].clone());

    ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .unwrap()
}
