use actix_web::http::Uri;
use etcd_client::Client;
use log::info;

pub async fn register_instance(uri: Uri, instance_id: String, instance_addr: String) {
    let key = format!("{}/{}", uri.path().trim_start_matches('/'), instance_id);

    let mut client = Client::connect(
        vec![format!(
            "{}:{}",
            uri.host().unwrap_or("127.0.0.1"),
            uri.port_u16().unwrap_or(2379)
        )],
        None,
    )
    .await
    .expect("connection failed");

    client
        .put(key.as_bytes(), instance_addr.as_bytes(), None)
        .await
        .expect("etcd registering service failed");
    info!("registered in etcd, key: {}, value: {}", key, instance_addr);
}
