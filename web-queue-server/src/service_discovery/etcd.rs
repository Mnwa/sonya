use actix::clock::sleep;
use actix_web::http::Uri;
use etcd_client::{Client, PutOptions};
use log::info;
use std::time::Duration;

const DEFAULT_TTL: i64 = 5;
const DEFAULT_SLEEP: Duration = Duration::from_secs(2);

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

    let cli = client
        .lease_grant(DEFAULT_TTL, None)
        .await
        .expect("grant leasing error");

    client
        .put(
            key.as_bytes(),
            instance_addr.as_bytes(),
            Some(PutOptions::new().with_lease(cli.id())),
        )
        .await
        .expect("etcd registering service failed");

    info!(
        "register instance in etcd, key: {}, value: {}",
        key, instance_addr
    );

    loop {
        client
            .lease_keep_alive(cli.id())
            .await
            .expect("lease keep alive error");

        sleep(DEFAULT_SLEEP).await;
    }
}
