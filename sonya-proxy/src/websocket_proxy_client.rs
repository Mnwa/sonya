use crate::registry::{get_address, get_all_addresses, RegistryActor};
use crate::service_discovery::{reloading_stream_factory, ServiceDiscoveryActor};
use actix::prelude::*;
use actix_web::http::HeaderMap;
use awc::{
    error::{WsClientError, WsProtocolError},
    ws::Frame,
    Client,
};
use derive_more::{Display, Error, From};
use futures::StreamExt;
use log::{error, info};
use sonya_meta::api::{sleep_between_reconnects, MAX_RECONNECT_ATTEMPTS};
use sonya_meta::message::RequestSequence;
use std::{collections::HashMap, pin::Pin, sync::Arc, task::Poll, time::Duration};
use tokio::sync::{broadcast, RwLock};

#[derive(Debug, Clone, Hash, PartialOrd, PartialEq, Eq)]
pub struct WebSocketProxyClientsStorageKey(String, Option<String>);

impl WebSocketProxyClientsStorageKey {
    pub fn new(queue: String, id: Option<String>) -> Self {
        Self(queue, id)
    }
}

#[derive(Default)]
pub struct WebSocketProxyClientsStorage(
    RwLock<HashMap<WebSocketProxyClientsStorageKey, Addr<WebSocketProxyClient>>>,
);

impl WebSocketProxyClientsStorage {
    async fn get_addr(
        &self,
        key: WebSocketProxyClientsStorageKey,
        headers: HeaderMap,
        registry: Addr<RegistryActor>,
        service_discovery: Addr<ServiceDiscoveryActor>,
        garbage_interval: u64,
        access_token: Option<String>,
        sequence: RequestSequence,
    ) -> Addr<WebSocketProxyClient> {
        if sequence.is_none() {
            let addr = {
                let guard = self.0.read().await;
                guard.get(&key).filter(|a| a.connected()).cloned()
            };

            match addr {
                Some(a) => a,
                None => {
                    let mut guard = self.0.write().await;
                    let WebSocketProxyClientsStorageKey(queue_name, id) = key.clone();
                    let addr = WebSocketProxyClient::new(
                        headers,
                        registry,
                        service_discovery,
                        queue_name,
                        id,
                        garbage_interval,
                        access_token,
                        sequence,
                    );

                    guard.insert(key, addr.clone());

                    addr
                }
            }
        } else {
            let WebSocketProxyClientsStorageKey(queue_name, id) = key;

            WebSocketProxyClient::new(
                headers,
                registry,
                service_discovery,
                queue_name,
                id,
                garbage_interval,
                access_token,
                sequence,
            )
        }
    }

    pub async fn subscribe(
        &self,
        key: WebSocketProxyClientsStorageKey,
        headers: HeaderMap,
        registry: Addr<RegistryActor>,
        service_discovery: Addr<ServiceDiscoveryActor>,
        garbage_interval: u64,
        access_token: Option<String>,
        sequence: RequestSequence,
    ) -> Option<broadcast::Receiver<WebSocketActorResponse>> {
        let addr = self
            .get_addr(
                key,
                headers,
                registry,
                service_discovery,
                garbage_interval,
                access_token,
                sequence,
            )
            .await;

        let receiver = addr.send(Subscribe).await;
        receiver.ok()
    }

    /// Clear unconnected actors
    pub async fn clear(&self) {
        let mut storage = self.0.write().await;
        storage.retain(|_k, v| v.connected());
    }
}

pub struct WebSocketProxyClient {
    headers: HeaderMap,
    registry: Addr<RegistryActor>,
    service_discovery: Addr<ServiceDiscoveryActor>,
    queue_name: String,
    id: Option<String>,
    garbage_interval: u64,
    attempts: u8,
    sender: broadcast::Sender<WebSocketActorResponse>,
    access_token: Option<String>,
    sequence: RequestSequence,
}

impl Actor for WebSocketProxyClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(
            "creating connection to queue {}, id {}",
            self.queue_name,
            self.id.clone().unwrap_or_else(|| String::from("None")),
        );
        match &self.id {
            Some(_) => ctx.add_stream(self.add_stream()),
            None => ctx.add_stream(self.add_stream_all()),
        };

        let garbage_interval = self.garbage_interval;

        ctx.add_stream(async_stream::stream! {
            let mut interval = actix::clock::interval(Duration::from_secs(garbage_interval));

            loop {
                interval.tick().await;
                yield CloseEmptyConnection
            }
        });
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        let _ = self.sender.send(WebSocketActorResponse::Stopped);
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if self.attempts >= MAX_RECONNECT_ATTEMPTS {
            error!(
                "close connection after {} attempts to reconnect queue {}, id {}",
                self.attempts,
                self.queue_name,
                self.id.clone().unwrap_or_else(|| String::from("None")),
            )
        } else {
            info!(
                "stopping connection to proxy, queue: {}, id: {}",
                self.queue_name,
                self.id.clone().unwrap_or_else(|| String::from("None"))
            )
        }
    }
}

impl Handler<Subscribe> for WebSocketProxyClient {
    type Result = MessageResult<Subscribe>;

    fn handle(&mut self, _: Subscribe, _: &mut Self::Context) -> Self::Result {
        MessageResult(self.sender.subscribe())
    }
}

impl StreamHandler<CloseEmptyConnection> for WebSocketProxyClient {
    fn handle(&mut self, _item: CloseEmptyConnection, ctx: &mut Self::Context) {
        if self.sender.receiver_count() == 0 {
            info!(
                "clearing unused connection queue: {}, id: {}",
                self.queue_name,
                self.id.clone().unwrap_or_else(|| String::from("None"))
            );
            ctx.stop()
        }
    }
}

impl StreamHandler<WebSocketClientResponse> for WebSocketProxyClient {
    fn handle(&mut self, item: WebSocketClientResponse, ctx: &mut Self::Context) {
        self.attempts = 0;

        match item {
            Ok(frame) => {
                if let Err(e) = self
                    .sender
                    .send(WebSocketActorResponse::Message(Arc::new(frame)))
                {
                    error!(
                        "message was not received, error {}, queue: {}, id: {}",
                        e,
                        self.queue_name,
                        self.id.clone().unwrap_or_else(|| String::from("None")),
                    );
                    ctx.stop();
                }
            }
            Err(e) => {
                error!(
                    "proxy error {}, queue: {}, id: {}",
                    e,
                    self.queue_name,
                    self.id.clone().unwrap_or_else(|| String::from("None")),
                );
                ctx.stop();
            }
        }
    }

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(
            "connected to queue {}, id {}, attempt: {}",
            self.queue_name,
            self.id.clone().unwrap_or_else(|| String::from("None")),
            self.attempts
        )
    }

    fn finished(&mut self, ctx: &mut Self::Context) {
        if self.attempts >= MAX_RECONNECT_ATTEMPTS {
            ctx.stop()
        } else {
            Actor::started(self, ctx);
        }
    }
}

impl WebSocketProxyClient {
    pub fn new(
        headers: HeaderMap,
        registry: Addr<RegistryActor>,
        service_discovery: Addr<ServiceDiscoveryActor>,
        queue_name: String,
        id: Option<String>,
        garbage_interval: u64,
        access_token: Option<String>,
        sequence: RequestSequence,
    ) -> Addr<Self> {
        Self {
            headers,
            registry,
            service_discovery,
            queue_name,
            id,
            garbage_interval,
            attempts: 0,
            sender: broadcast::channel(8).0,
            access_token,
            sequence,
        }
        .start()
    }

    fn add_stream(&self) -> impl Stream<Item = WebSocketClientResponse> {
        let registry = self.registry.clone();
        let service_discovery = self.service_discovery.clone();
        let queue_name = self.queue_name.clone();
        let id = self.id.clone().expect("empty id");
        let headers = self.headers.clone();
        let attempt = self.attempts;
        let client = Client::default();

        let path = match (self.access_token.as_ref(), self.sequence) {
            (Some(at), Some(s)) => format!(
                "/queue/listen/ws/{}/{}?access_token={}&sequence={}",
                queue_name, id, at, s
            ),
            (Some(at), None) => {
                format!("/queue/listen/ws/{}/{}?access_token={}", queue_name, id, at)
            }
            (None, Some(s)) => format!("/queue/listen/ws/{}/{}?&sequence={}", queue_name, id, s),
            (None, None) => format!("/queue/listen/ws/{}/{}", queue_name, id),
        };

        async_stream::try_stream! {
            sleep_between_reconnects(attempt).await;

            let address = get_address(&registry, queue_name, id).await;

            let mut request = client.ws(address.clone() + &path);
            for (key, value) in headers.into_iter() {
                request = request.set_header(key, value.clone());
            }

            let (_, codec) = request.connect().await?;

            if let Some(mut service_disovery_stream) = reloading_stream_factory(codec, service_discovery).await {
                while let Some(r_frame) = service_disovery_stream.next().await {
                    let frame = r_frame?;
                    yield frame
                }
            }
        }
    }

    fn add_stream_all(&self) -> impl Stream<Item = WebSocketClientResponse> {
        let registry = self.registry.clone();
        let service_discovery = self.service_discovery.clone();
        let headers = self.headers.clone();
        let attempt = self.attempts;
        let client = Client::default();

        let path = match (self.access_token.as_ref(), self.sequence) {
            (Some(at), Some(s)) => format!(
                "/queue/listen/ws/{}?access_token={}&sequence={}",
                self.queue_name, at, s
            ),
            (Some(at), None) => {
                format!("/queue/listen/ws/{}?access_token={}", self.queue_name, at)
            }
            (None, Some(s)) => format!("/queue/listen/ws/{}?&sequence={}", self.queue_name, s),
            (None, None) => format!("/queue/listen/ws/{}", self.queue_name),
        };

        async_stream::try_stream! {
            sleep_between_reconnects(attempt).await;

            let addresses = get_all_addresses(&registry).await;

            let requests = addresses.into_iter().map(|address| {
                let mut request = client.ws(address.clone() + &path);
                for (key, value) in headers.clone().into_iter() {
                    request = request.set_header(key, value.clone());
                }
               request.connect()
            });
            let results: Result<Vec<_>, _> = futures::future::join_all(requests)
                .await
                .into_iter()
                .map(|r| r.map(|(_, codec)| codec))
                .collect();
            let connections = results?;

            let stream = ConnectionsAggregator::new(connections);

            if let Some(mut service_disovery_stream) = reloading_stream_factory(stream, service_discovery).await {
                while let Some(r_frame) = service_disovery_stream.next().await {
                    let frame = r_frame?;
                    yield frame
                }
            }
        }
    }
}

#[derive(Debug, Display, From, Error)]
pub enum ProxyError {
    #[display(fmt = "protocol error: {}", "_0")]
    ProtocolError(WsProtocolError),
    #[display(fmt = "client error: {}", "_0")]
    ClientError(WsClientError),
}

unsafe impl Send for ProxyError {}
unsafe impl Sync for ProxyError {}

pub type WebSocketClientResponse = Result<Frame, ProxyError>;

#[derive(Clone, Debug)]
pub enum WebSocketActorResponse {
    Message(Arc<Frame>),
    Stopped,
}

#[derive(Message)]
#[rtype(result = "broadcast::Receiver<WebSocketActorResponse>")]
pub struct Subscribe;

struct CloseEmptyConnection;

struct ConnectionsAggregator<S> {
    streams: Vec<S>,
}

impl<S> ConnectionsAggregator<S> {
    fn new(streams: Vec<S>) -> Self {
        Self { streams }
    }
}

impl<S: Stream<Item = O> + Unpin, O> Stream for ConnectionsAggregator<S> {
    type Item = O;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        for s in self.streams.iter_mut() {
            match s.poll_next_unpin(cx) {
                Poll::Ready(Some(r)) => return Poll::Ready(Some(r)),
                // if one of this connections is broken, close them all
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => continue,
            }
        }
        Poll::Pending
    }
}
