use crate::queue::connection::BroadcastMessage;
use derive_more::{Display, Error, From};
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::{Batch, Event, Subscriber};
use sonya_meta::config::DefaultQueues;
use sonya_meta::message::UniqId;
use std::num::NonZeroU128;
use std::time::{SystemTime, SystemTimeError};

pub type QueueMap = sled::Db;

#[derive(Debug)]
pub struct Queue {
    map: QueueMap,
}

impl Queue {
    pub fn new(map: QueueMap, default: DefaultQueues) -> QueueResult<Self> {
        let this = Self { map };

        default.into_iter().try_for_each(|q| this.create_queue(q))?;

        Ok(this)
    }
    pub fn create_queue(&self, queue_name: String) -> QueueResult<()> {
        self.map
            .open_tree(queue_name.as_bytes())
            .map(|_| ())
            .map_err(QueueError::from)
    }

    pub fn subscribe_queue_by_id<'a, T: 'a + Send + DeserializeOwned>(
        &self,
        queue_name: String,
        id: String,
    ) -> QueueResult<Option<BoxStream<'a, BroadcastMessage<T>>>> {
        if !self.check_tree_exists(&queue_name) {
            return Ok(None);
        }
        let tree = self.map.open_tree(queue_name.as_bytes())?;

        let subscription = tree.watch_prefix(id.as_bytes());

        Ok(Some(prepare_stream(subscription)))
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

        Ok(Some(prepare_stream(subscription)))
    }

    pub fn send_to_queue<T: 'static + Clone + Serialize + UniqId>(
        &self,
        queue_name: String,
        mut value: T,
    ) -> QueueResult<bool> {
        if !self.check_tree_exists(&queue_name) {
            return Ok(false);
        }

        let sequence = match value.get_sequence() {
            None => {
                let s = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_nanos();

                match NonZeroU128::new(s) {
                    None => return Err(QueueError::ZeroSequence),
                    Some(s) => value.set_sequence(s),
                };

                s
            }
            Some(s) => s.get(),
        };

        let id = get_id(value.get_id(), sequence);

        let tree = self.map.open_tree(queue_name.as_bytes())?;
        let mut batch = Batch::default();

        batch.insert(id, serde_json::to_vec(&value)?);
        tree.scan_prefix(value.get_id().as_bytes())
            .rev()
            .skip(10)
            .try_for_each::<_, QueueResult<()>>(|r| {
                let (k, _) = r?;
                batch.remove(k);
                Ok(())
            })?;

        tree.apply_batch(batch)?;

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
}

fn prepare_stream<'a, T: 'a + DeserializeOwned + Send>(
    mut subscriber: Subscriber,
) -> BoxStream<'a, BroadcastMessage<T>> {
    Box::pin(async_stream::stream! {
        while let Some(event) = (&mut subscriber).await {
            if let Event::Insert { value, .. } = event {
                if let Ok(m) = serde_json::from_slice(&value) {
                    yield BroadcastMessage::Message(m)
                }
            }
        }
    })
}

fn get_id(id: &str, sequence: u128) -> Vec<u8> {
    let mut id = Vec::from(id.as_bytes());
    id.extend_from_slice(&sequence.to_be_bytes());

    id
}

#[derive(Debug, Display, From, Error)]
pub enum QueueError {
    Db(sled::Error),
    Encode(serde_json::Error),
    Time(SystemTimeError),
    #[display(fmt = "sequence must be more then 0")]
    ZeroSequence,
}

pub type QueueResult<T> = Result<T, QueueError>;
