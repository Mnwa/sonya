use crate::registry::RegistryList;
use actix_web::http::Uri;
use etcd_client::*;
use futures::stream::BoxStream;
use std::collections::HashMap;

pub async fn factory(uri: Uri) -> Box<dyn FnOnce() -> BoxStream<'static, RegistryList>> {
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
    let prefix = uri.path().trim_start_matches('/');

    let resp = client
        .get(prefix, Some(GetOptions::new().with_prefix()))
        .await
        .expect("error getting list");

    let mut registry_list: HashMap<String, String> = resp
        .kvs()
        .iter()
        .map(|kv| {
            (
                kv.key_str().expect("key is invalid utf-8").into(),
                kv.value_str().expect("value is invalid utf-8").into(),
            )
        })
        .collect();

    let (_, mut stream) = client
        .watch(prefix, Some(WatchOptions::new().with_prefix()))
        .await
        .expect("failed prefix watching");

    Box::new(move || {
        Box::pin(async_stream::stream! {
            yield registry_list.values().cloned().collect();

            while let Ok(Some(resp)) = stream.message().await {
                if resp.canceled() {
                    break;
                }

                for event in resp.events() {
                    if let Some(kv) = event.kv() {
                        match event.event_type() {
                            EventType::Put => registry_list.insert(
                                kv.key_str().expect("key is invalid utf-8").into(),
                                kv.value_str().expect("value is invalid utf-8").into(),
                            ),
                            EventType::Delete => {
                                registry_list.remove(kv.key_str().expect("key is invalid utf-8"))
                            }
                        };

                        yield registry_list.values().cloned().collect();
                    }
                }
            }
        })
    })
}
