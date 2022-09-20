use crate::queue::connection::BroadcastMessage;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use derive_more::{Display, Error, From};
use futures::stream::BoxStream;
use log::info;
use rocksdb::{
    AsColumnFamilyRef, BlockBasedIndexType, BlockBasedOptions, DBCompressionType, DBWithThreadMode,
    DataBlockIndexType, Direction, IteratorMode, MemtableFactory, MultiThreaded, Options,
    ReadOptions, SliceTransform, WriteBatch,
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
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

        let opts = create_rocks_opts(config.max_key_updates.unwrap_or(1000));

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
        let opts = create_rocks_opts(self.max_key_updates.unwrap_or(1000));

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
        let key_sender = get_key_broadcast(&self.map, &handle, &id, &queue)?;

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
        let time = Instant::now();
        let handle = match self.map.cf_handle(queue_name.as_str()) {
            None => return Ok((false, None)),
            Some(h) => h,
        };

        let handle_time = time.elapsed();

        let queue = get_queue_broadcast(queue_name, &self.queue_meta);
        let key_sender = get_key_broadcast(&self.map, &handle, value.get_id(), &queue)?;

        let sequence = match value.get_sequence() {
            None => {
                let id = self.generate_next_id(&key_sender);

                value.set_sequence(id);

                id.get()
            }
            Some(s) => s.get(),
        };

        let sequence_time = time.elapsed();

        let mut write_time = Duration::default();
        let mut clear_time = Duration::default();

        if !matches!(self.max_key_updates, Some(0)) {
            let id = get_id(value.get_id(), sequence);

            let mut batch = WriteBatch::default();

            batch.put_cf(&handle, id, rmp_serde::to_vec(&value)?);

            if let Some(m) = self.max_key_updates {
                let min_id = get_id(value.get_id(), u64::MIN);
                let max_id = get_id(value.get_id(), sequence);

                let mut opts = ReadOptions::default();
                opts.set_iterate_lower_bound(min_id);
                opts.set_prefix_same_as_start(true);

                self.map
                    .snapshot()
                    .iterator_cf_opt(
                        &handle,
                        opts,
                        IteratorMode::From(&max_id, Direction::Reverse),
                    )
                    .skip(m - 1)
                    .try_for_each::<_, QueueResult<()>>(|r| {
                        let (k, _) = r?;
                        batch.delete_cf(&handle, k);
                        Ok(())
                    })?;

                clear_time = time.elapsed();
            }

            self.map.write(batch)?;

            write_time = time.elapsed();
        }

        let _ = queue.sender.send(value.clone());
        let _ = key_sender.sender.send(value);

        let broadcast_time = time.elapsed();

        info!(
            "handle: {}, sequence: {}, clear: {}, write: {}, broadcast: {}",
            handle_time.as_millis(),
            sequence_time.as_millis() - handle_time.as_millis(),
            clear_time
                .as_millis()
                .checked_div(sequence_time.as_millis())
                .unwrap_or(0),
            write_time
                .as_millis()
                .checked_div(clear_time.max(sequence_time).as_millis())
                .unwrap_or(0),
            broadcast_time.as_millis() - write_time.max(sequence_time).as_millis(),
        );

        Ok((true, SequenceId::new(sequence)))
    }

    pub fn close_queue(&self, queue_name: String) -> QueueResult<bool> {
        self.queue_meta.remove(&queue_name);

        self.map
            .drop_cf(queue_name.as_str())
            .map_err(QueueError::from)
            .map(|_| true)
    }

    fn generate_next_id(&self, key_sender: &KeyBroadcast<T>) -> SequenceId {
        SequenceId::new(key_sender.sequence.fetch_add(1, Ordering::SeqCst)).unwrap()
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
    let mut key = Vec::with_capacity(id.as_bytes().len() + std::mem::size_of::<SequenceId>());
    key.extend_from_slice(id.as_bytes());
    key.extend_from_slice(&sequence.to_be_bytes());

    key
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

    let min_id = get_id(id, u64::MIN);
    let max_id = get_id(id, u64::MAX);

    opts.set_ignore_range_deletions(true);
    opts.set_prefix_same_as_start(true);

    let snapshot = map.snapshot();

    let iter: Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>> =
        match sequence_id {
            RequestSequenceId::Id(s) => {
                opts.set_iterate_upper_bound(max_id.clone());

                Box::new(snapshot.iterator_cf_opt(
                    cf_handle,
                    opts,
                    IteratorMode::From(&get_id(id, s.get()), Direction::Forward),
                ))
            }
            RequestSequenceId::Last => {
                opts.set_iterate_lower_bound(min_id.clone());

                Box::new(
                    snapshot
                        .iterator_cf_opt(
                            cf_handle,
                            opts,
                            IteratorMode::From(&max_id, Direction::Reverse),
                        )
                        .take(1),
                )
            }
            RequestSequenceId::First => {
                opts.set_iterate_upper_bound(max_id.clone());

                Box::new(snapshot.iterator_cf_opt(
                    cf_handle,
                    opts,
                    IteratorMode::From(&min_id, Direction::Forward),
                ))
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
    sequence: AtomicU64,
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
fn get_key_broadcast<'a, T: Clone + SequenceEvent + DeserializeOwned>(
    map: &'a QueueMap,
    cf_handle: &'a impl AsColumnFamilyRef,
    id: &str,
    queue_broadcast: &'a QueueBroadcast<T>,
) -> QueueResult<Ref<'a, String, KeyBroadcast<T>>> {
    if !queue_broadcast.keys.contains_key(id) {
        let mut opts = ReadOptions::default();
        opts.set_ignore_range_deletions(true);
        opts.set_iterate_lower_bound(get_id(id, u64::MIN));
        opts.set_iterate_upper_bound(get_id(id, u64::MAX));

        let last: Option<T> = map
            .iterator_cf_opt(cf_handle, opts, IteratorMode::End)
            .next()
            .transpose()
            .map_err(QueueError::from)
            .and_then(|b| match b {
                None => Ok(None),
                Some((_, v)) => rmp_serde::from_slice::<T>(&v)
                    .map_err(QueueError::from)
                    .map(Some),
            })?;

        let sequence = last
            .and_then(|v| v.get_sequence())
            .map(|s| AtomicU64::new(s.get()))
            .unwrap_or_else(|| AtomicU64::new(1));

        queue_broadcast.keys.insert(
            id.to_string(),
            KeyBroadcast {
                sender: channel(1024).0,
                sequence,
            },
        );
    }

    Ok(queue_broadcast
        .keys
        .get(id)
        .expect("data race occurred, keys broadcast already dropped"))
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

fn create_rocks_opts(max_keys: usize) -> Options {
    let mut opts = Options::default();
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Zstd);
    opts.set_enable_pipelined_write(true);
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_advise_random_on_open(false);

    opts.set_prefix_extractor(SliceTransform::create(
        "events_prefix_extractor",
        prefix_extractor,
        Some(is_valid_domain),
    ));

    opts.increase_parallelism(num_cpus::get_physical() as i32);

    let mut factory = BlockBasedOptions::default();
    factory.set_index_type(BlockBasedIndexType::BinarySearch);
    factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
    factory.set_bloom_filter(10., true);
    factory.set_hybrid_ribbon_filter(10., 2);

    opts.set_block_based_table_factory(&factory);

    let factory = MemtableFactory::HashSkipList {
        bucket_count: max_keys * 100,
        height: 12,
        branching_factor: 4,
    };

    opts.set_allow_concurrent_memtable_write(false);
    opts.set_memtable_factory(factory);

    opts.optimize_universal_style_compaction(64 * 1024 * 1024 * 1024);

    opts
}

fn prefix_extractor(prefix: &[u8]) -> &[u8] {
    &prefix[0..prefix.len() - 1 - std::mem::size_of::<SequenceId>()]
}

fn is_valid_domain(prefix: &[u8]) -> bool {
    prefix.len() > std::mem::size_of::<SequenceId>()
}
