use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::num::NonZeroU128;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMessage {
    pub id: String,
    pub sequence: Option<NonZeroU128>,
    pub payload: Value,
}

impl UniqId for EventMessage {
    fn get_id(&self) -> &str {
        &self.id
    }
    fn get_sequence(&self) -> Option<NonZeroU128> {
        self.sequence
    }

    fn set_sequence(&mut self, sequence: NonZeroU128) -> Option<NonZeroU128> {
        self.sequence.replace(sequence)
    }
}

pub trait UniqId {
    fn get_id(&self) -> &str;
    fn get_sequence(&self) -> Option<NonZeroU128>;
    fn set_sequence(&mut self, sequence: NonZeroU128) -> Option<NonZeroU128>;
}
