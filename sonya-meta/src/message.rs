use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::num::NonZeroU64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMessage {
    pub id: String,
    pub sequence: Sequence,
    pub payload: Value,
}

pub type Sequence = Option<SequenceId>;
pub type SequenceId = NonZeroU64;

impl UniqId for EventMessage {
    fn get_id(&self) -> &str {
        &self.id
    }
    fn get_sequence(&self) -> Sequence {
        self.sequence
    }

    fn set_sequence(&mut self, sequence: NonZeroU64) -> Sequence {
        self.sequence.replace(sequence)
    }
}

pub trait UniqId {
    fn get_id(&self) -> &str;
    fn get_sequence(&self) -> Sequence;
    fn set_sequence(&mut self, sequence: SequenceId) -> Sequence;
}
