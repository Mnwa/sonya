use actix::clock::sleep;
use actix_web::http::Uri;
use etcd_client::{Client, PutOptions};
use log::{error, info};
use std::time::Duration;

const DEFAULT_TTL: i64 = 5;
const DEFAULT_SLEEP: Duration = Duration::from_secs(2);

pub async fn register_instance(uri: Uri, instance_id: String, instance_addr: String) {
    let key = format!("{}/{}", uri.path().trim_start_matches('/'), instance_id);
    let value = instance_addr;

    let mut attempts: u8 = 0;

    loop {
        // maximum attempts
        if attempts == 10 {
            error!("registration in etcd failed more then 10 times");
            return;
        }
        let client_r = Client::connect(
            vec![format!(
                "{}:{}",
                uri.host().unwrap_or("127.0.0.1"),
                uri.port_u16().unwrap_or(2379)
            )],
            None,
        )
        .await;

        match client_r {
            Ok(c) => {
                register_instance_impl(c, &key, &value).await;
                attempts = 0
            }
            Err(e) => {
                error!("connection error: {}", e);
                attempts += 1
            }
        }

        sleep(DEFAULT_SLEEP).await;
    }
}

async fn register_instance_impl(mut client: Client, key: &str, value: &str) {
    let cli = match client.lease_grant(DEFAULT_TTL, None).await {
        Ok(cli) => cli,
        Err(e) => {
            error!("lease grant error: {}", e);
            return;
        }
    };

    if let Err(e) = client
        .put(
            key.as_bytes(),
            value.as_bytes(),
            Some(PutOptions::new().with_lease(cli.id())),
        )
        .await
    {
        error!("putting instance addr error: {}", e);
        return;
    }

    info!("register instance in etcd, key: {}, value: {}", key, value);

    loop {
        if let Err(e) = client.lease_keep_alive(cli.id()).await {
            error!("lease keep alive error: {}", e);
            return;
        }

        sleep(DEFAULT_SLEEP).await;
    }
}
