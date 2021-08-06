use actix::prelude::*;
use log::info;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct RegistryActor {
    registry: Vec<String>,
}

impl Actor for RegistryActor {
    type Context = SyncContext<Self>;
}

impl RegistryActor {
    pub fn new(registry: Vec<String>) -> Addr<Self> {
        SyncArbiter::start(num_cpus::get(), move || Self {
            registry: registry.clone(),
        })
    }
}

impl Handler<GetAddress> for RegistryActor {
    type Result = MessageResult<GetAddress>;

    fn handle(&mut self, msg: GetAddress, _ctx: &mut Self::Context) -> Self::Result {
        let mut hasher = DefaultHasher::new();
        msg.hash(&mut hasher);
        let hash = hasher.finish();
        let shard = self.registry[(hash % self.registry.len() as u64) as usize].clone();
        info!(
            "chosen new shard for queue: {} and id: {}, shard: {}",
            msg.0, msg.1, shard
        );
        MessageResult(shard)
    }
}

impl Handler<GetAllAddresses> for RegistryActor {
    type Result = MessageResult<GetAllAddresses>;

    fn handle(&mut self, _msg: GetAllAddresses, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.registry.clone())
    }
}

#[derive(Message, Hash, Debug)]
#[rtype(result = "String")]
pub struct GetAddress(String, String);

#[derive(Message, Hash)]
#[rtype(result = "Vec<String>")]
pub struct GetAllAddresses;

pub async fn get_address(registry: &Addr<RegistryActor>, queue_name: String, id: String) -> String {
    registry
        .send(GetAddress(queue_name, id))
        .await
        .expect("registry failed")
}

pub async fn get_all_addresses(registry: &Addr<RegistryActor>) -> Vec<String> {
    registry
        .send(GetAllAddresses)
        .await
        .expect("registry failed")
}
