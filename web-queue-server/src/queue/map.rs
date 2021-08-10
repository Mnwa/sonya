use crate::queue::connection::BroadcastMessage;
use log::info;
use serde::Serialize;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use web_queue_meta::message::UniqId;

type MessageSender<T> = broadcast::Sender<BroadcastMessage<T>>;
type QueueIdMap<T> = Arc<RwLock<HashMap<String, MessageSender<T>>>>;
pub type QueueMap<T> = HashMap<String, (MessageSender<T>, QueueIdMap<T>)>;

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
        let (tx, _) = broadcast::channel(8);
        self.map.insert(queue_name, (tx, Default::default()));

        true
    }

    pub fn subscribe_queue_by_id(
        &self,
        queue_name: String,
        id: String,
    ) -> impl Future<Output = Option<broadcast::Receiver<BroadcastMessage<T>>>> {
        let guard: Option<QueueIdMap<T>> =
            self.map.get(&queue_name).map(|(_, m)| m).map(Arc::clone);
        async move {
            let guard = guard?;
            let queue = guard.read().await;
            if let Some(tx) = queue.get(&id) {
                return Some(tx.subscribe());
            }
            drop(queue);

            let mut queue = guard.write().await;
            let (tx, rx) = broadcast::channel(8);

            queue.insert(id, tx);

            Some(rx)
        }
    }

    pub fn subscribe_queue(
        &self,
        queue_name: String,
    ) -> Option<broadcast::Receiver<BroadcastMessage<T>>> {
        let tx = self.map.get(&queue_name).map(|(tx, _)| tx);
        Some(tx?.subscribe())
    }

    pub fn send_to_queue(&self, queue_name: String, value: T) -> impl Future<Output = bool> {
        let queue = self
            .map
            .get(&queue_name)
            .map(|(tx, m)| (tx.clone(), Arc::clone(m)));
        async move {
            match queue {
                None => {
                    info!("queue {} was not created", queue_name);
                    false
                }
                Some((tx, guard)) => {
                    let queue_id_map = guard.read().await;
                    match queue_id_map.get(value.get_id()) {
                        None if tx.receiver_count() > 0 => info!(
                            "no one listeners for accepting message in queue {}",
                            queue_name
                        ),
                        None => info!("sent only to global queue {}", queue_name),
                        Some(tx) => match tx.send(BroadcastMessage::Message(value.clone())) {
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
                    let _ = tx.send(BroadcastMessage::Message(value));
                    true
                }
            }
        }
    }

    pub fn close_queue(&mut self, queue_name: String) -> impl Future<Output = bool> {
        let guard = self.map.remove(&queue_name);
        async move {
            match guard {
                Some((tx, guard)) => {
                    let listeners = guard.read().await;
                    for (_, listener) in listeners.iter() {
                        let _ = listener.send(BroadcastMessage::Close);
                    }

                    let _ = tx.send(BroadcastMessage::Close);

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

impl<T: 'static + Clone + Serialize + UniqId> From<Vec<String>> for Queue<T> {
    fn from(queue_names: Vec<String>) -> Self {
        let mut queue = Self::default();
        for queue_name in queue_names {
            queue.create_queue(queue_name);
        }
        queue
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}
