use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMessage {
    pub id: String,
    pub timestamp: Option<usize>,
    pub payload: Value,
}

impl UniqId for EventMessage {
    fn get_id(&self) -> &String {
        &self.id
    }
}

pub trait UniqId {
    fn get_id(&self) -> &String;
}
