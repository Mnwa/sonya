use crate::message::SequenceId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct BaseQueueResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub sequence_id: Option<SequenceId>,
}
