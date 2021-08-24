#[cfg(feature = "api")]
pub mod api;
#[cfg(feature = "etcd")]
pub mod etcd;

use crate::registry::{RegistryActor, RegistryList, UpdateRegistry};
use actix::prelude::*;
use futures::{Future, StreamExt};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Poll, Waker};

pub struct ServiceDiscoveryActor {
    registry: Addr<RegistryActor>,
    broadcaster: Arc<ServiceDiscoveryUpdateBroadcast>,
    closer: Option<futures::channel::oneshot::Sender<()>>,
}

impl ServiceDiscoveryActor {
    pub fn new<F, T>(
        factory: F,
        registry: Addr<RegistryActor>,
        closer: futures::channel::oneshot::Sender<()>,
    ) -> Addr<Self>
    where
        F: FnOnce() -> T,
        T: 'static + Stream<Item = RegistryList>,
    {
        Self::create(|ctx| {
            ctx.add_stream(factory().map(UpdateRegistry));
            Self {
                registry,
                broadcaster: Default::default(),
                closer: Some(closer),
            }
        })
    }
}

impl StreamHandler<UpdateRegistry> for ServiceDiscoveryActor {
    fn handle(&mut self, msg: UpdateRegistry, _ctx: &mut Self::Context) {
        self.registry.do_send(msg);
        let broadcaster = std::mem::take(&mut self.broadcaster);
        broadcaster.state.store(true, Ordering::SeqCst);
        broadcaster
            .waiters
            .lock()
            .iter()
            .for_each(|waiter| waiter.wake_by_ref());
    }
}

impl Handler<SubscribeUpdates> for ServiceDiscoveryActor {
    type Result = MessageResult<SubscribeUpdates>;

    fn handle(&mut self, _msg: SubscribeUpdates, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.broadcaster.subscribe())
    }
}

impl Actor for ServiceDiscoveryActor {
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(closer) = self.closer.take() {
            let _ = closer.send(());
        }
    }
}

#[derive(Message)]
#[rtype(result = "ServiceDiscoveryUpdateSubscribeFuture")]
pub struct SubscribeUpdates;

pin_project! {
    /// stop stream after receiving event
    pub struct ServiceDiscoveryReloadingStream<S> {
        #[pin]
        stream: S,
        #[pin]
        receiver: ServiceDiscoveryUpdateSubscribeFuture,
    }
}

impl<S> ServiceDiscoveryReloadingStream<S> {
    fn new(stream: S, receiver: ServiceDiscoveryUpdateSubscribeFuture) -> Self {
        Self { stream, receiver }
    }
}

impl<S: 'static + Unpin + Stream<Item = O>, O> Stream for ServiceDiscoveryReloadingStream<S> {
    type Item = O;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Poll::Ready(()) = this.receiver.poll(cx) {
            return Poll::Ready(None);
        }
        this.stream.poll_next(cx)
    }
}

#[derive(Default)]
struct ServiceDiscoveryUpdateBroadcast {
    waiters: Mutex<Vec<Waker>>,
    state: AtomicBool,
}

impl ServiceDiscoveryUpdateBroadcast {
    fn subscribe(self: &Arc<Self>) -> ServiceDiscoveryUpdateSubscribeFuture {
        ServiceDiscoveryUpdateSubscribeFuture::from(self.clone())
    }
}

pub struct ServiceDiscoveryUpdateSubscribeFuture {
    broadcaster: Arc<ServiceDiscoveryUpdateBroadcast>,
    registered: bool,
}

impl From<Arc<ServiceDiscoveryUpdateBroadcast>> for ServiceDiscoveryUpdateSubscribeFuture {
    fn from(broadcaster: Arc<ServiceDiscoveryUpdateBroadcast>) -> Self {
        Self {
            broadcaster,
            registered: false,
        }
    }
}

impl Future for ServiceDiscoveryUpdateSubscribeFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if !self.registered {
            self.broadcaster.waiters.lock().push(cx.waker().clone());
            self.registered = true;
        }
        match self.broadcaster.state.load(Ordering::SeqCst) {
            true => Poll::Ready(()),
            false => Poll::Pending,
        }
    }
}

pub async fn reloading_stream_factory<S>(
    stream: S,
    addr: Addr<ServiceDiscoveryActor>,
) -> Option<ServiceDiscoveryReloadingStream<S>> {
    match addr.send(SubscribeUpdates).await {
        Ok(s) => Some(ServiceDiscoveryReloadingStream::new(stream, s)),
        Err(_) => None,
    }
}
