use crate::queue::connection::BroadcastMessage;
use log::info;
use serde::Serialize;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use web_queue_meta::message::UniqId;

type KeysMap<T> = Arc<RwLock<HashMap<String, broadcast::Sender<BroadcastMessage<T>>>>>;
pub type QueueMap<T> = HashMap<String, KeysMap<T>>;

#[derive(Debug)]
pub struct Queue<T> {
    map: QueueMap<T>,
}

impl<T: 'static + Clone + Serialize + UniqId> Queue<T> {
    pub fn create_queue(&mut self, queue_name: String) -> bool {
        if self.map.contains_key(&queue_name) {
            info!("queue {} already created", queue_name);
            return false;
        }

        info!("queue {} successfully created", queue_name);
        self.map.insert(queue_name, Default::default());

        true
    }

    pub fn subscribe_queue(
        &self,
        queue_name: String,
        id: String,
    ) -> impl Future<Output = Option<broadcast::Receiver<BroadcastMessage<T>>>> {
        let guard: Option<KeysMap<T>> = self.map.get(&queue_name).map(Arc::clone);
        async move {
            let guard = guard?;
            let queue = guard.read().await;
            if let Some(tx) = queue.get(&id) {
                return Some(tx.subscribe());
            }
            drop(queue);

            let mut queue = guard.write().await;
            let (tx, rx) = broadcast::channel(128);

            queue.insert(id, tx);

            Some(rx)
        }
    }

    pub fn send_to_queue(&self, queue_name: String, value: T) -> impl Future<Output = bool> {
        let guard: Option<KeysMap<T>> = self.map.get(&queue_name).map(Arc::clone);
        async move {
            match guard {
                None => {
                    info!("queue {} was not created", queue_name);
                    false
                }
                Some(guard) => {
                    let queue = guard.read().await;
                    match queue.get(value.get_id()) {
                        None => info!(
                            "no one listeners for accepting message in queue {}",
                            queue_name
                        ),
                        Some(tx) => match tx.send(BroadcastMessage::Message(value)) {
                            Ok(brokers) => info!(
                                "message in queue {} accepted by {} brokers",
                                queue_name, brokers
                            ),
                            Err(_) => info!(
                                "no one brokers for accepting message in queue {}",
                                queue_name
                            ),
                        },
                    }
                    true
                }
            }
        }
    }

    pub fn close_queue(&mut self, queue_name: String) -> impl Future<Output = bool> {
        let guard = self.map.remove(&queue_name);
        async move {
            match guard {
                Some(guard) => {
                    let listeners = guard.read().await;
                    for (_, listener) in listeners.iter() {
                        let _ = listener.send(BroadcastMessage::Close);
                    }

                    true
                }
                None => {
                    info!("closing not created queue {}", queue_name);
                    false
                }
            }
        }
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}
