use crate::registry::RegistryList;
use futures::channel::mpsc;

pub fn factory() -> (
    mpsc::Sender<RegistryList>,
    Box<dyn FnOnce() -> mpsc::Receiver<RegistryList>>,
) {
    let (sender, receiver) = mpsc::channel::<RegistryList>(1);

    (sender, Box::new(move || receiver))
}
