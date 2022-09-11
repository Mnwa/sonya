use crate::queue::connection::BroadcastMessage;
use derive_more::{Display, Error, From};
use futures::stream::BoxStream;
use log::error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::{Batch, IVec, Tree};
use sonya_meta::config::Queue as QueueOptions;
use sonya_meta::message::{RequestSequence, RequestSequenceId, SequenceId, UniqId};
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::Mutex;
use tokio::sync::broadcast::{channel, Receiver, Sender};

pub type QueueMap = sled::Db;

#[derive(Debug)]
pub struct Queue<T> {
    map: QueueMap,
    max_key_updates: Option<usize>,
    queue_broadcasts: Mutex<HashMap<String, QueueBroadcast<T>>>,
}

impl<'a, T> Queue<T>
where
    T: 'a + Send + DeserializeOwned + Serialize + Debug + UniqId + Clone,
{
    pub fn new(config: QueueOptions) -> QueueResult<Self> {
        let db_config = match config.db_path {
            None => sled::Config::new().temporary(true),
            Some(dp) => sled::Config::new().path(dp).use_compression(true),
        };

        let map = db_config.open()?;

        let this = Self {
            map,
            max_key_updates: config.max_key_updates,
            queue_broadcasts: Default::default(),
        };

        config
            .default
            .into_iter()
            .try_for_each(|q| this.create_queue(q))?;

        Ok(this)
    }

    pub fn create_queue(&self, queue_name: String) -> QueueResult<()> {
        self.map
            .open_tree(queue_name.as_bytes())
            .map(|_| ())
            .map_err(QueueError::from)
    }

    pub fn delete_queue(&self, queue_name: String, id: String) -> QueueResult<()> {
        let mut queue_b = self.queue_broadcasts.lock().unwrap();
        let queue = get_queue_broadcast(queue_name.clone(), &mut queue_b);
        queue.keys.remove(&id);

        let mut batch = Batch::default();

        let tree = self.map.open_tree(queue_name.as_bytes())?;

        for response in tree.scan_prefix(id.as_bytes()) {
            let (key, _) = response?;

            batch.remove(key);
        }

        tree.apply_batch(batch).map_err(QueueError::from)
    }

    pub fn subscribe_queue_by_id(
        &self,
        queue_name: String,
        id: String,
        sequence: RequestSequence,
    ) -> QueueResult<Subscription<'a, T>> {
        if !self.check_tree_exists(&queue_name) {
            return Ok(Default::default());
        }
        let tree = self.map.open_tree(queue_name.as_bytes())?;

        let prev_items = get_prev_items::<T>(&tree, &id, sequence)?;

        let prev_len = prev_items.as_ref().map(|i| i.len());

        let mut map = self.queue_broadcasts.lock().unwrap();
        let queue = get_queue_broadcast(queue_name, &mut map);
        let key_sender = get_key_broadcast(id, queue);

        let recv = key_sender.subscribe();
        drop(map);

        Ok(Subscription {
            stream: Some(prepare_stream(recv, prev_items)),
            preloaded_count: prev_len,
        })
    }

    pub fn subscribe_queue(
        &self,
        queue_name: String,
        sequence: RequestSequence,
    ) -> QueueResult<Subscription<'a, T>> {
        if !self.check_tree_exists(&queue_name) {
            return Ok(Default::default());
        }
        let tree = self.map.open_tree(queue_name.as_bytes())?;

        let prev_items = get_prev_all_items::<T>(&tree, sequence)?;

        let prev_len = prev_items.as_ref().map(|i| i.len());

        let mut map = self.queue_broadcasts.lock().unwrap();
        let queue = get_queue_broadcast(queue_name, &mut map);

        let recv = queue.sender.subscribe();
        drop(map);

        Ok(Subscription {
            stream: Some(prepare_stream(recv, prev_items)),
            preloaded_count: prev_len,
        })
    }

    pub fn send_to_queue(&self, queue_name: String, mut value: T) -> QueueResult<bool> {
        if !self.check_tree_exists(&queue_name) {
            return Ok(false);
        }

        let id = value.get_id();

        let sequence = match value.get_sequence() {
            None => {
                let id = self.generate_next_id(&queue_name, id)?;

                value.set_sequence(id);

                id.get()
            }
            Some(s) => s.get(),
        };

        if !matches!(self.max_key_updates, Some(0)) {
            let id = get_id(value.get_id(), sequence);

            let tree = self.map.open_tree(queue_name.as_bytes())?;

            tree.insert(id, serde_json::to_vec(&value)?)?;

            if let Some(m) = self.max_key_updates {
                let mut batch = Batch::default();

                tree.scan_prefix(value.get_id().as_bytes())
                    .rev()
                    .skip(m - 1)
                    .try_for_each::<_, QueueResult<()>>(|r| {
                        let (k, _) = r?;
                        batch.remove(k);
                        Ok(())
                    })?;

                tree.apply_batch(batch)?;
            }
        }

        let mut map = self.queue_broadcasts.lock().unwrap();

        let queue = get_queue_broadcast(queue_name, &mut map);
        if let Err(e) = queue.sender.send(value.clone()) {
            error!("broadcast message to queue subscribers error: {}", e)
        }

        let key_sender = get_key_broadcast(value.get_id().to_string(), queue);
        if let Err(e) = key_sender.send(value) {
            error!("broadcast message to key subscribers error: {}", e)
        }

        Ok(true)
    }

    pub fn close_queue(&self, queue_name: String) -> QueueResult<bool> {
        let mut queue_b = self.queue_broadcasts.lock().unwrap();
        queue_b.remove(&queue_name);

        self.map.drop_tree(queue_name).map_err(QueueError::from)
    }

    fn check_tree_exists(&self, queue_name: &str) -> bool {
        matches!(
            self.map
                .tree_names()
                .into_iter()
                .find(|v| v == queue_name.as_bytes()),
            Some(_)
        )
    }

    fn generate_next_id(&self, queue_name: &str, id: &str) -> QueueResult<SequenceId> {
        let mut key = Vec::from("id_");
        key.extend_from_slice(queue_name.as_bytes());
        key.extend_from_slice(id.as_bytes());

        let res = self.map.update_and_fetch(key, |v| {
            v.and_then(|v| Some(u64::from_be_bytes(v.try_into().ok()?)))
                .and_then(|id| id.checked_add(1))
                .map(|id| IVec::from(&id.to_be_bytes()))
                .unwrap_or_else(|| IVec::from(&1u64.to_be_bytes()))
                .into()
        })?;

        res.and_then(|r| Some(u64::from_be_bytes(r.as_ref().try_into().ok()?)))
            .and_then(SequenceId::new)
            .map(Ok)
            .unwrap_or_else(|| Err(QueueError::ZeroSequence))
    }
}

fn prepare_stream<'a, T: 'a + DeserializeOwned + Send + Clone>(
    mut receiver: Receiver<T>,
    prev_items: Option<Vec<T>>,
) -> BoxStream<'a, BroadcastMessage<T>> {
    Box::pin(async_stream::stream! {
        if let Some(pi) = prev_items {
            let mut iter = pi.into_iter();
            while let Some(e) = iter.next() {
                yield BroadcastMessage::Message(e)
            }
        }
        while let Ok(value) = receiver.recv().await {
            yield BroadcastMessage::Message(value)
        }
    })
}

fn get_id(id: &str, sequence: u64) -> Vec<u8> {
    let mut id = Vec::from(id.as_bytes());
    id.extend_from_slice(&sequence.to_be_bytes());

    id
}

fn get_prev_items<T: DeserializeOwned>(
    tree: &Tree,
    id: &str,
    sequence: RequestSequence,
) -> QueueResult<Option<Vec<T>>> {
    sequence
        .map(|sequence_id| {
            extract_sequences(tree, sequence_id, id)
                .map(|r| {
                    r.map(|(_, v)| v)
                        .map_err(QueueError::from)
                        .and_then(|v| serde_json::from_slice(&v).map_err(QueueError::from))
                })
                .collect()
        })
        .transpose()
}

fn extract_sequences(
    tree: &Tree,
    sequence_id: RequestSequenceId,
    id: &str,
) -> Box<dyn Iterator<Item = sled::Result<(IVec, IVec)>>> {
    match sequence_id {
        RequestSequenceId::Id(s) => Box::new(tree.range(get_id(id, s.get())..get_id(id, u64::MAX))),
        RequestSequenceId::Last => Box::new(tree.scan_prefix(id.as_bytes()).rev().take(1)),
        RequestSequenceId::First => Box::new(tree.scan_prefix(id.as_bytes())),
    }
}

fn get_prev_all_items<T: DeserializeOwned + UniqId>(
    tree: &Tree,
    sequence: RequestSequence,
) -> QueueResult<Option<Vec<T>>> {
    sequence
        .map(|sequence_id| {
            let i = tree.iter().values().map(|v| {
                v.map_err(QueueError::from)
                    .and_then(|v| serde_json::from_slice(&v).map_err(QueueError::from))
            });

            let i: Box<dyn Iterator<Item = Result<T, QueueError>>> = match sequence_id {
                RequestSequenceId::Id(s) => {
                    Box::new(i.filter(move |v: &Result<T, QueueError>| match v {
                        Ok(v) => v.get_sequence().filter(|cs| *cs >= s).is_some(),
                        Err(_) => true,
                    }))
                }
                RequestSequenceId::Last => {
                    let mut map: BTreeMap<String, T> = BTreeMap::new();

                    for item in i {
                        match item {
                            Ok(v) => {
                                map.insert(v.get_id().to_string(), v);
                            }
                            e @ Err(_) => return e.map(|r| vec![r]),
                        }
                    }

                    Box::new(map.into_values().map(Ok))
                }
                RequestSequenceId::First => Box::new(i),
            };

            i.collect::<Result<Vec<_>, _>>()
        })
        .transpose()
}

#[derive(Debug, Display, From, Error)]
pub enum QueueError {
    Db(sled::Error),
    Encode(serde_json::Error),
    #[display(fmt = "sequence must be more then 0")]
    ZeroSequence,
}

pub type QueueResult<T> = Result<T, QueueError>;

#[derive(Debug)]
struct QueueBroadcast<T> {
    sender: Sender<T>,
    keys: HashMap<String, Sender<T>>,
}

fn get_queue_broadcast<T: Clone>(
    queue_name: String,
    queue_broadcasts: &mut HashMap<String, QueueBroadcast<T>>,
) -> &mut QueueBroadcast<T> {
    queue_broadcasts
        .entry(queue_name)
        .or_insert_with(|| QueueBroadcast {
            sender: channel(1024).0,
            keys: Default::default(),
        })
}

fn get_key_broadcast<T: Clone>(
    id: String,
    queue_broadcast: &mut QueueBroadcast<T>,
) -> &mut Sender<T> {
    queue_broadcast
        .keys
        .entry(id)
        .or_insert_with(|| channel(1024).0)
}

pub struct Subscription<'a, T> {
    pub stream: Option<BoxStream<'a, BroadcastMessage<T>>>,
    pub preloaded_count: Option<usize>,
}

impl<'a, T> Default for Subscription<'a, T> {
    fn default() -> Self {
        Self {
            stream: None,
            preloaded_count: None,
        }
    }
}
