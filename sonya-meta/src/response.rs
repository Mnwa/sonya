use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct BaseQueueResponse {
    pub success: bool,
}
