use crate::queue::connection::BroadcastMessage;
use derive_more::{Display, Error, From};
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::{Batch, Event, IVec, Subscriber, Tree};
use sonya_meta::config::Queue as QueueOptions;
use sonya_meta::message::{RequestSequence, RequestSequenceId, SequenceId, UniqId};
use std::convert::TryInto;
use std::fmt::Debug;

pub type QueueMap = sled::Db;

#[derive(Debug)]
pub struct Queue {
    map: QueueMap,
    max_key_updates: Option<usize>,
}

impl Queue {
    pub fn new(config: QueueOptions) -> QueueResult<Self> {
        let db_config = match config.db_path {
            None => sled::Config::new().temporary(true),
            Some(dp) => sled::Config::new().path(dp).use_compression(true),
        };

        let map = db_config.open()?;

        let this = Self {
            map,
            max_key_updates: config.max_key_updates,
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

    pub fn subscribe_queue_by_id<'a, T: 'a + Send + DeserializeOwned + Debug>(
        &self,
        queue_name: String,
        id: String,
        sequence: RequestSequence,
    ) -> QueueResult<Option<BoxStream<'a, BroadcastMessage<T>>>> {
        if !self.check_tree_exists(&queue_name) {
            return Ok(None);
        }
        let tree = self.map.open_tree(queue_name.as_bytes())?;

        let prev_items = get_prev_items::<T>(&tree, &id, sequence)?;
        let subscription = tree.watch_prefix(id.as_bytes());

        Ok(Some(prepare_stream(subscription, prev_items)))
    }

    pub fn subscribe_queue<'a, T: 'a + Send + DeserializeOwned>(
        &self,
        queue_name: String,
    ) -> QueueResult<Option<BoxStream<'a, BroadcastMessage<T>>>> {
        if !self.check_tree_exists(&queue_name) {
            return Ok(None);
        }
        let tree = self.map.open_tree(queue_name.as_bytes())?;

        let subscription = tree.watch_prefix([]);

        Ok(Some(prepare_stream(subscription, None)))
    }

    pub fn send_to_queue<T: 'static + Clone + Serialize + UniqId>(
        &self,
        queue_name: String,
        mut value: T,
    ) -> QueueResult<bool> {
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
            let mut batch = Batch::default();

            batch.insert(id, serde_json::to_vec(&value)?);

            if let Some(m) = self.max_key_updates {
                tree.scan_prefix(value.get_id().as_bytes())
                    .rev()
                    .skip(m)
                    .try_for_each::<_, QueueResult<()>>(|r| {
                        let (k, _) = r?;
                        batch.remove(k);
                        Ok(())
                    })?;
            }

            tree.apply_batch(batch)?;
        }

        Ok(true)
    }

    pub fn close_queue(&self, queue_name: String) -> QueueResult<bool> {
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

fn prepare_stream<'a, T: 'a + DeserializeOwned + Send>(
    mut subscriber: Subscriber,
    prev_items: Option<Vec<T>>,
) -> BoxStream<'a, BroadcastMessage<T>> {
    Box::pin(async_stream::stream! {
        if let Some(pi) = prev_items {
            let mut iter = pi.into_iter();
            while let Some(e) = iter.next() {
                yield BroadcastMessage::Message(e)
            }
        }
        while let Some(event) = (&mut subscriber).await {
            if let Event::Insert { value, .. } = event {
                if let Ok(m) = serde_json::from_slice(&value) {
                    yield BroadcastMessage::Message(m)
                }
            }
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
        RequestSequenceId::First => Box::new(tree.scan_prefix(id.as_bytes()).take(1)),
    }
}

#[derive(Debug, Display, From, Error)]
pub enum QueueError {
    Db(sled::Error),
    Encode(serde_json::Error),
    #[display(fmt = "sequence must be more then 0")]
    ZeroSequence,
}

pub type QueueResult<T> = Result<T, QueueError>;
