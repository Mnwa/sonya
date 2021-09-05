use crate::registry::RegistryList;
use crate::service_discovery::ServiceDiscoveryStreamFactory;
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::StreamExt;
use std::sync::Arc;

pub fn factory() -> (mpsc::Sender<RegistryList>, ServiceDiscoveryStreamFactory) {
    let (sender, receiver) = mpsc::channel::<RegistryList>(1);
    let receiver = Arc::new(Mutex::new(receiver));

    (
        sender,
        Box::new(move || {
            let receiver = receiver.clone();
            Box::pin(async_stream::stream! {
                let mut receiver = receiver.lock().await;

                while let Some(r) = receiver.next().await {
                    yield r
                }
            })
        }),
    )
}
