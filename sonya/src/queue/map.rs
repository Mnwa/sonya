use crate::queue::connection::BroadcastMessage;
use derive_more::{Display, Error, From};
use futures::stream::BoxStream;
use log::error;
use rocksdb::{
    AsColumnFamilyRef, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
    ReadOptions, WriteBatch,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sonya_meta::config::Queue as QueueOptions;
use sonya_meta::message::{RequestSequence, RequestSequenceId, SequenceId, UniqId};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Mutex;
use tokio::sync::broadcast::{channel, Receiver, Sender};

pub type QueueMap = DBWithThreadMode<MultiThreaded>;

#[derive(Debug)]
pub struct Queue<T> {
    map: QueueMap,
    max_key_updates: Option<usize>,
    queue_meta: Mutex<HashMap<String, QueueBroadcast<T>>>,
}

impl<'a, T> Queue<T>
where
    T: 'a + Send + DeserializeOwned + Serialize + Debug + UniqId + Clone,
{
    pub fn new(config: QueueOptions) -> QueueResult<Self> {
        let path = match config.db_path {
            None => {
                let mut temp = std::env::temp_dir();
                temp.push(format!(
                    "{}_{}",
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION")
                ));
                temp
            }
            Some(dp) => dp,
        };

        let mut opts = Options::default();
        opts.create_missing_column_families(true);
        opts.create_if_missing(true);

        let mut list = config.default;
        list.extend(QueueMap::list_cf(&opts, &path).unwrap_or_default());

        let map = QueueMap::open_cf(&opts, path, list)?;

        let this = Self {
            map,
            max_key_updates: config.max_key_updates,
            queue_meta: Default::default(),
        };

        Ok(this)
    }

    pub fn create_queue(&self, queue_name: String) -> QueueResult<()> {
        self.map
            .create_cf(queue_name, &Options::default())
            .map(|_| ())
            .map_err(QueueError::from)
    }

    pub fn delete_queue(&self, queue_name: String, id: String) -> QueueResult<()> {
        let mut queue_b = self.queue_meta.lock().unwrap();
        let queue = get_queue_broadcast(queue_name.clone(), &mut queue_b);
        queue.keys.remove(&id);

        let handle = match self.map.cf_handle(queue_name.as_str()) {
            None => return Ok(()),
            Some(h) => h,
        };

        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);

        let iterator = self.map.iterator_cf_opt(
            &handle,
            opts,
            IteratorMode::From(id.as_bytes(), Direction::Forward),
        );

        let mut batch = WriteBatch::default();

        for response in iterator {
            let (key, _) = response?;

            batch.delete_cf(&handle, key);
        }

        self.map.write(batch).map_err(QueueError::from)
    }

    pub fn subscribe_queue_by_id(
        &self,
        queue_name: String,
        id: String,
        sequence: RequestSequence,
    ) -> QueueResult<Subscription<'a, T>> {
        let handle = match self.map.cf_handle(queue_name.as_str()) {
            None => return Ok(Default::default()),
            Some(h) => h,
        };

        let prev_items = get_prev_items::<T>(&self.map, &handle, &id, sequence)?;

        let prev_len = prev_items.as_ref().map(|i| i.len());

        let mut map = self.queue_meta.lock().unwrap();
        let queue = get_queue_broadcast(queue_name, &mut map);
        let key_sender = get_key_broadcast(id, queue);

        let recv = key_sender.sender.subscribe();
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
        let handle = match self.map.cf_handle(queue_name.as_str()) {
            None => return Ok(Default::default()),
            Some(h) => h,
        };

        let prev_items = get_prev_all_items::<T>(&self.map, &handle, sequence)?;

        let prev_len = prev_items.as_ref().map(|i| i.len());

        let mut map = self.queue_meta.lock().unwrap();
        let queue = get_queue_broadcast(queue_name, &mut map);

        let recv = queue.sender.subscribe();
        drop(map);

        Ok(Subscription {
            stream: Some(prepare_stream(recv, prev_items)),
            preloaded_count: prev_len,
        })
    }

    pub fn send_to_queue(&self, queue_name: String, mut value: T) -> QueueResult<bool> {
        let handle = match self.map.cf_handle(queue_name.as_str()) {
            None => return Ok(false),
            Some(h) => h,
        };

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

            let mut batch = WriteBatch::default();

            batch.put_cf(&handle, id, serde_json::to_vec(&value)?);

            if let Some(m) = self.max_key_updates {
                let mut opts = ReadOptions::default();
                opts.set_prefix_same_as_start(true);

                self.map
                    .snapshot()
                    .iterator_cf_opt(
                        &handle,
                        opts,
                        IteratorMode::From(value.get_id().as_bytes(), Direction::Reverse),
                    )
                    .skip(m - 1)
                    .try_for_each::<_, QueueResult<()>>(|r| {
                        let (k, _) = r?;
                        batch.delete_cf(&handle, k);
                        Ok(())
                    })?;
            }

            self.map.write(batch)?
        }

        let mut map = self.queue_meta.lock().unwrap();

        let queue = get_queue_broadcast(queue_name, &mut map);
        if let Err(e) = queue.sender.send(value.clone()) {
            error!("broadcast message to queue subscribers error: {}", e)
        }

        let key_sender = get_key_broadcast(value.get_id().to_string(), queue);
        if let Err(e) = key_sender.sender.send(value) {
            error!("broadcast message to key subscribers error: {}", e)
        }

        Ok(true)
    }

    pub fn close_queue(&self, queue_name: String) -> QueueResult<bool> {
        let mut queue_b = self.queue_meta.lock().unwrap();
        queue_b.remove(&queue_name);

        self.map
            .drop_cf(queue_name.as_str())
            .map_err(QueueError::from)
            .map(|_| true)
    }

    fn generate_next_id(&self, queue_name: &str, id: &str) -> QueueResult<SequenceId> {
        let mut key = Vec::from("id_");
        key.extend_from_slice(queue_name.as_bytes());
        key.extend_from_slice(id.as_bytes());

        let mut map = self.queue_meta.lock().unwrap();

        let queue = get_queue_broadcast(queue_name.to_string(), &mut map);
        let key_sender = get_key_broadcast(id.to_string(), queue);
        let sid = match key_sender.sequence {
            None => match self.map.get(&key)? {
                None => SequenceId::new(1).unwrap(),
                Some(s) => SequenceId::new(u64::from_be_bytes(
                    s.try_into().unwrap_or_else(|_| 1u64.to_be_bytes()),
                ))
                .unwrap_or_else(|| SequenceId::new(1).unwrap()),
            },
            Some(s) => s,
        };

        let new_sid = sid.get().saturating_add(1);

        key_sender.sequence = SequenceId::new(new_sid);
        self.map.put(key, new_sid.to_be_bytes())?;

        Ok(sid)
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
    map: &QueueMap,
    cf_handle: &impl AsColumnFamilyRef,
    id: &str,
    sequence: RequestSequence,
) -> QueueResult<Option<Vec<T>>> {
    sequence
        .map(|sequence_id| extract_sequences::<T>(map, cf_handle, sequence_id, id))
        .transpose()
}

fn extract_sequences<T: DeserializeOwned>(
    map: &QueueMap,
    cf_handle: &impl AsColumnFamilyRef,
    sequence_id: RequestSequenceId,
    id: &str,
) -> Result<Vec<T>, QueueError> {
    let mut opts = ReadOptions::default();

    let iter: Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>> =
        match sequence_id {
            RequestSequenceId::Id(s) => {
                opts.set_iterate_lower_bound(get_id(id, s.get()));
                opts.set_iterate_upper_bound(get_id(id, u64::MAX));
                Box::new(map.iterator_cf_opt(cf_handle, opts, IteratorMode::Start))
            }
            RequestSequenceId::Last => {
                opts.set_iterate_lower_bound(get_id(id, u64::MIN));
                opts.set_iterate_upper_bound(get_id(id, u64::MAX));

                Box::new(
                    map.iterator_cf_opt(cf_handle, opts, IteratorMode::End)
                        .take(1),
                )
            }
            RequestSequenceId::First => {
                opts.set_iterate_lower_bound(get_id(id, u64::MIN));
                opts.set_iterate_upper_bound(get_id(id, u64::MAX));

                Box::new(
                    map.iterator_cf_opt(cf_handle, opts, IteratorMode::Start)
                        .take(1),
                )
            }
        };

    iter.map(|r| {
        r.map(|(_, v)| v)
            .map_err(QueueError::from)
            .and_then(|v| serde_json::from_slice(&v).map_err(QueueError::from))
    })
    .collect()
}

fn get_prev_all_items<T: DeserializeOwned + UniqId>(
    map: &QueueMap,
    cf_handle: &impl AsColumnFamilyRef,
    sequence: RequestSequence,
) -> QueueResult<Option<Vec<T>>> {
    sequence
        .map(|sequence_id| {
            let i = map
                .full_iterator_cf(cf_handle, IteratorMode::Start)
                .map(|v| {
                    v.map_err(QueueError::from)
                        .and_then(|(_, v)| serde_json::from_slice(&v).map_err(QueueError::from))
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
    Db(rocksdb::Error),
    Encode(serde_json::Error),
    #[display(fmt = "sequence must be more then 0")]
    ZeroSequence,
    #[display(fmt = "these queue name is reserved by system")]
    SystemQueueName,
}

pub type QueueResult<T> = Result<T, QueueError>;

#[derive(Debug)]
struct QueueBroadcast<T> {
    sender: Sender<T>,
    keys: HashMap<String, KeyBroadcast<T>>,
}
#[derive(Debug)]
struct KeyBroadcast<T> {
    sender: Sender<T>,
    sequence: Option<SequenceId>,
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
) -> &mut KeyBroadcast<T> {
    queue_broadcast
        .keys
        .entry(id)
        .or_insert_with(|| KeyBroadcast {
            sender: channel(1024).0,
            sequence: None,
        })
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
