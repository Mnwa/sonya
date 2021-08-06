use crate::queue::connection::BroadcastMessage;
use log::info;
use serde::Serialize;
use std::collections::HashMap;
use tokio::sync::broadcast;
use web_queue_meta::message::UniqId;

pub type QueueMap<T> = HashMap<String, broadcast::Sender<BroadcastMessage<T>>>;

#[derive(Debug)]
pub struct Queue<T> {
    map: QueueMap<T>,
}

impl<T: 'static + Clone + Serialize + UniqId> Queue<T> {
    pub fn create_queue(
        &mut self,
        queue_name: String,
    ) -> Option<broadcast::Receiver<BroadcastMessage<T>>> {
        if self.map.contains_key(&queue_name) {
            info!("queue {} already created", queue_name);
            return None;
        }

        info!("queue {} successfully created", queue_name);

        let (tx, rx) = broadcast::channel(1024);
        self.map.insert(queue_name, tx);

        Some(rx)
    }

    pub fn subscribe_queue(
        &self,
        queue_name: String,
    ) -> Option<broadcast::Receiver<BroadcastMessage<T>>> {
        self.map.get(&queue_name).map(|tx| tx.subscribe())
    }

    pub fn send_to_queue(&self, queue_name: String, value: T) -> bool {
        match self.map.get(&queue_name) {
            None => {
                info!("queue {} was not created", queue_name);
                false
            }
            Some(tx) => {
                match tx.send(BroadcastMessage::Message(value)) {
                    Ok(brokers) => info!(
                        "message in queue {} accepted by {} brokers",
                        queue_name, brokers
                    ),
                    Err(_) => info!(
                        "no one brokers for accepting message in queue {}",
                        queue_name
                    ),
                }
                true
            }
        }
    }

    pub fn close_queue(&mut self, queue_name: String) -> bool {
        if let Some(tx) = self.map.remove(&queue_name) {
            info!("closing queue {}", queue_name);
            tx.send(BroadcastMessage::Close).is_ok()
        } else {
            info!("closing not created queue {}", queue_name);
            false
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
