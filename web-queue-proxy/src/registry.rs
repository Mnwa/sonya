use actix::prelude::*;
use log::info;
use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub type RegistryList = Vec<String>;
type RegistryStore = Arc<RwLock<RegistryList>>;

pub struct RegistryActor {
    registry: RegistryStore,
}

impl Actor for RegistryActor {
    type Context = SyncContext<Self>;
}

impl RegistryActor {
    pub fn new(registry: RegistryList) -> Addr<Self> {
        let registry = RegistryStore::new(registry.into());
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
        let registry = self.registry.read();
        let shard = registry[(hash % registry.len() as u64) as usize].clone();
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
        MessageResult(Vec::clone(self.registry.read().as_ref()))
    }
}

impl Handler<UpdateRegistry> for RegistryActor {
    type Result = MessageResult<UpdateRegistry>;

    fn handle(
        &mut self,
        UpdateRegistry(new_registry): UpdateRegistry,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mut registry = self.registry.write();
        *registry = new_registry;
        MessageResult(())
    }
}

#[derive(Message, Hash, Debug)]
#[rtype(result = "String")]
pub struct GetAddress(String, String);

#[derive(Message, Hash)]
#[rtype(result = "RegistryList")]
pub struct GetAllAddresses;

pub async fn get_address(registry: &Addr<RegistryActor>, queue_name: String, id: String) -> String {
    registry
        .send(GetAddress(queue_name, id))
        .await
        .expect("registry failed")
}

pub async fn get_all_addresses(registry: &Addr<RegistryActor>) -> RegistryList {
    registry
        .send(GetAllAddresses)
        .await
        .expect("registry failed")
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct UpdateRegistry(pub RegistryList);
