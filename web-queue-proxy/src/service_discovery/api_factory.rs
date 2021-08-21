use crate::registry::RegistryList;
use futures::channel::mpsc;

pub fn api_factory() -> (
    mpsc::Sender<RegistryList>,
    Box<dyn FnOnce() -> mpsc::Receiver<RegistryList>>,
) {
    let (sender, receiver) = mpsc::channel::<RegistryList>(1);

    (sender, Box::new(move || receiver))
}
