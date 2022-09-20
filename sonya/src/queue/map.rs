use crate::queue::connection::BroadcastMessage;
use dashmap::mapref::one::{Ref, RefMut};
use dashmap::DashMap;
use derive_more::{Display, Error, From};
use futures::stream::BoxStream;
use log::error;
use rocksdb::{
    AsColumnFamilyRef, DBCompressionType, DBWithThreadMode, IteratorMode, MultiThreaded, Options,
    ReadOptions, WriteBatch,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sonya_meta::config::Queue as QueueOptions;
use sonya_meta::message::{
    Event, RequestSequence, RequestSequenceId, SequenceEvent, SequenceId, UniqIdEvent,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::ErrorKind;
use tokio::sync::broadcast::{channel, Receiver, Sender};

pub type QueueMap = DBWithThreadMode<MultiThreaded>;

#[derive(Debug)]
pub struct Queue<T> {
    map: QueueMap,
    max_key_updates: Option<usize>,
    queue_meta: DashMap<String, QueueBroadcast<T>>,
}

impl<'a, T> Queue<T>
where
    T: 'a + Send + DeserializeOwned + Serialize + Debug + Event + Clone,
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

                let r_result = std::fs::remove_dir_all(&temp);

                if matches!(&r_result, Err(e) if e.kind() != ErrorKind::NotFound) {
                    r_result.unwrap_or_else(|_| {
                        panic!("fail to clear temp directory: {:?}", temp.to_str())
                    });
                }

                temp
            }
            Some(dp) => dp,
        };

        let opts = create_rocks_opts();

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
        let opts = create_rocks_opts();

        self.map
            .create_cf(queue_name, &opts)
            .map(|_| ())
            .map_err(QueueError::from)
    }

    pub fn delete_queue(&self, queue_name: String, id: String) -> QueueResult<()> {
        let queue = get_queue_broadcast(queue_name.clone(), &self.queue_meta);
        queue.keys.remove(&id);

        let handle = match self.map.cf_handle(queue_name.as_str()) {
            None => return Ok(()),
            Some(h) => h,
        };

        self.map
            .delete_range_cf(
                &handle,
                get_id(id.as_str(), u64::MIN),
                get_id(id.as_str(), u64::MAX),
            )
            .map_err(QueueError::from)
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

        let mut prev_items = get_prev_items::<T>(&self.map, &handle, &id, sequence)?;

        if let Some(last) = prev_items.as_mut().and_then(|vec| vec.last_mut()) {
            last.set_last(true)
        }

        let prev_len = prev_items.as_ref().map(|i| i.len());

        let queue = get_queue_broadcast(queue_name, &self.queue_meta);
        let key_sender = get_key_broadcast(id, &queue);

        let recv = key_sender.sender.subscribe();

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

        let mut prev_items = get_prev_all_items::<T>(&self.map, &handle, sequence)?;

        if let Some(last) = prev_items.as_mut().and_then(|vec| vec.last_mut()) {
            last.set_last(true)
        }

        let prev_len = prev_items.as_ref().map(|i| i.len());

        let queue = get_queue_broadcast(queue_name, &self.queue_meta);

        let recv = queue.sender.subscribe();

        Ok(Subscription {
            stream: Some(prepare_stream(recv, prev_items)),
            preloaded_count: prev_len,
        })
    }

    pub fn send_to_queue(
        &self,
        queue_name: String,
        mut value: T,
    ) -> QueueResult<(bool, Option<SequenceId>)> {
        let handle = match self.map.cf_handle(queue_name.as_str()) {
            None => return Ok((false, None)),
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

            batch.put_cf(&handle, id, rmp_serde::to_vec(&value)?);

            if let Some(m) = self.max_key_updates {
                let mut opts = ReadOptions::default();
                opts.set_iterate_lower_bound(get_id(value.get_id(), u64::MIN));
                opts.set_iterate_upper_bound(get_id(value.get_id(), u64::MAX));

                self.map
                    .snapshot()
                    .iterator_cf_opt(&handle, opts, IteratorMode::End)
                    .skip(m - 1)
                    .try_for_each::<_, QueueResult<()>>(|r| {
                        let (k, _) = r?;
                        batch.delete_cf(&handle, k);
                        Ok(())
                    })?;
            }

            self.map.write(batch)?
        }

        let queue = get_queue_broadcast(queue_name, &self.queue_meta);
        if let Err(e) = queue.sender.send(value.clone()) {
            error!("broadcast message to queue subscribers error: {}", e)
        }

        let key_sender = get_key_broadcast(value.get_id().to_string(), &queue);
        if let Err(e) = key_sender.sender.send(value) {
            error!("broadcast message to key subscribers error: {}", e)
        }

        Ok((true, SequenceId::new(sequence)))
    }

    pub fn close_queue(&self, queue_name: String) -> QueueResult<bool> {
        self.queue_meta.remove(&queue_name);

        self.map
            .drop_cf(queue_name.as_str())
            .map_err(QueueError::from)
            .map(|_| true)
    }

    fn generate_next_id(&self, queue_name: &str, id: &str) -> QueueResult<SequenceId> {
        let mut key = Vec::from("id_");
        key.extend_from_slice(queue_name.as_bytes());
        key.extend_from_slice(id.as_bytes());

        let queue = get_queue_broadcast(queue_name.to_string(), &self.queue_meta);
        let mut key_sender = get_key_broadcast_mut(id.to_string(), &queue);
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
    opts.set_ignore_range_deletions(true);

    let snapshot = map.snapshot();

    let iter: Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>> =
        match sequence_id {
            RequestSequenceId::Id(s) => {
                opts.set_iterate_lower_bound(get_id(id, s.get()));
                opts.set_iterate_upper_bound(get_id(id, u64::MAX));
                Box::new(snapshot.iterator_cf_opt(cf_handle, opts, IteratorMode::Start))
            }
            RequestSequenceId::Last => {
                opts.set_iterate_lower_bound(get_id(id, u64::MIN));
                opts.set_iterate_upper_bound(get_id(id, u64::MAX));

                Box::new(
                    snapshot
                        .iterator_cf_opt(cf_handle, opts, IteratorMode::End)
                        .take(1),
                )
            }
            RequestSequenceId::First => {
                opts.set_iterate_lower_bound(get_id(id, u64::MIN));
                opts.set_iterate_upper_bound(get_id(id, u64::MAX));

                Box::new(snapshot.iterator_cf_opt(cf_handle, opts, IteratorMode::Start))
            }
        };

    iter.map(|r| {
        r.map(|(_, v)| v)
            .map_err(QueueError::from)
            .and_then(|v| rmp_serde::from_slice(&v).map_err(QueueError::from))
    })
    .collect()
}

fn get_prev_all_items<T: DeserializeOwned + SequenceEvent + UniqIdEvent>(
    map: &QueueMap,
    cf_handle: &impl AsColumnFamilyRef,
    sequence: RequestSequence,
) -> QueueResult<Option<Vec<T>>> {
    sequence
        .map(|sequence_id| {
            let mut opts = ReadOptions::default();
            opts.set_ignore_range_deletions(true);

            let i = map
                .iterator_cf_opt(cf_handle, opts, IteratorMode::Start)
                .map(|v| {
                    v.map_err(QueueError::from)
                        .and_then(|(_, v)| rmp_serde::from_slice(&v).map_err(QueueError::from))
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
    Encode(rmp_serde::encode::Error),
    Decode(rmp_serde::decode::Error),
    #[display(fmt = "sequence must be more then 0")]
    ZeroSequence,
    #[display(fmt = "these queue name is reserved by system")]
    SystemQueueName,
}

pub type QueueResult<T> = Result<T, QueueError>;

#[derive(Debug)]
struct QueueBroadcast<T> {
    sender: Sender<T>,
    keys: DashMap<String, KeyBroadcast<T>>,
}
#[derive(Debug)]
struct KeyBroadcast<T> {
    sender: Sender<T>,
    sequence: Option<SequenceId>,
}

// Potentially may be replaced with consistent entry and downgrade
fn get_queue_broadcast<T: Clone>(
    queue_name: String,
    queue_broadcasts: &DashMap<String, QueueBroadcast<T>>,
) -> Ref<'_, String, QueueBroadcast<T>> {
    if !queue_broadcasts.contains_key(&queue_name) {
        queue_broadcasts.insert(
            queue_name.clone(),
            QueueBroadcast {
                sender: channel(1024).0,
                keys: Default::default(),
            },
        );
    }

    queue_broadcasts
        .get(&queue_name)
        .expect("data race occurred, queue broadcast already dropped")
}

// Potentially may be replaced with consistent entry and downgrade
fn get_key_broadcast<T: Clone>(
    id: String,
    queue_broadcast: &QueueBroadcast<T>,
) -> Ref<'_, String, KeyBroadcast<T>> {
    if !queue_broadcast.keys.contains_key(&id) {
        queue_broadcast.keys.insert(
            id.clone(),
            KeyBroadcast {
                sender: channel(1024).0,
                sequence: None,
            },
        );
    }

    queue_broadcast
        .keys
        .get(&id)
        .expect("data race occurred, keys broadcast already dropped")
}

fn get_key_broadcast_mut<T: Clone>(
    id: String,
    queue_broadcast: &QueueBroadcast<T>,
) -> RefMut<'_, String, KeyBroadcast<T>> {
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

fn create_rocks_opts() -> Options {
    let mut opts = Options::default();
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Zstd);
    opts.set_enable_pipelined_write(true);
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_advise_random_on_open(false);

    opts
}
