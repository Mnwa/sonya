use crate::registry::{get_address, get_all_addresses, RegistryActor};
use crate::service_discovery::{reloading_stream_factory, ServiceDiscoveryActor};
use actix::prelude::*;
use actix_web::dev::RequestHead;
use actix_web::http::{HeaderMap, Uri};
use actix_web_actors::ws;
use actix_web_actors::ws::{CloseCode, CloseReason, Frame, ProtocolError};
use awc::error::WsClientError;
use awc::Client;
use futures::StreamExt;
use log::{info, warn};
use std::pin::Pin;
use std::task::Poll;
use web_queue_meta::api::{sleep_between_reconnects, MAX_RECONNECT_ATTEMPTS};

pub struct WebSocketProxyActor {
    headers: HeaderMap,
    uri: Uri,
    registry: Addr<RegistryActor>,
    service_discovery: Addr<ServiceDiscoveryActor>,
    queue_name: String,
    id: Option<String>,
    attempts: u8,
}

impl Actor for WebSocketProxyActor {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.address().do_send(CreateConnection)
    }
}

impl WebSocketProxyActor {
    pub fn new(
        headers: &RequestHead,
        registry: Addr<RegistryActor>,
        service_discovery: Addr<ServiceDiscoveryActor>,
        queue_name: String,
        id: Option<String>,
    ) -> Self {
        Self {
            headers: headers.headers().clone(),
            uri: headers.uri.clone(),
            registry,
            service_discovery,
            queue_name,
            id,
            attempts: 0,
        }
    }
}

impl Handler<ProxyRequest> for WebSocketProxyActor {
    type Result = ();

    fn handle(&mut self, ProxyRequest(frame): ProxyRequest, ctx: &mut Self::Context) {
        match frame {
            Ok(Ok(f)) => {
                info!(
                    "accepted new frame from shard queue: {}, id: {}",
                    self.queue_name,
                    self.id.clone().unwrap_or_else(|| "none".to_owned())
                );
                self.attempts = 0;
                match f {
                    Frame::Text(b) => ctx.text(
                        std::str::from_utf8(&b)
                            .map(|r| r.to_string())
                            .unwrap_or_else(|e| {
                                warn!(
                                    "invalid utf message sent from shard queue: {}, id: {}, error: {}",
                                    self.queue_name, self.id.clone().unwrap_or_else(|| "none".to_owned()), e
                                );
                                e.to_string()
                            }),
                    ),
                    Frame::Binary(b) => ctx.binary(b),
                    Frame::Continuation(_) => {}
                    Frame::Ping(p) => ctx.ping(&p),
                    Frame::Pong(p) => ctx.pong(&p),
                    Frame::Close(r) => {
                        ctx.close(r);
                        ctx.stop()
                    }
                }
            }
            error => {
                let address = ctx.address();
                match error {
                    Err(e) => warn!(
                        "connect to shard error, queue: {}, id: {}, err: {:#?}",
                        self.queue_name,
                        self.id.clone().unwrap_or_else(|| "none".to_owned()),
                        e
                    ),
                    Ok(Err(e)) => warn!(
                        "protocol to shard error, queue: {}, id: {}, err: {:#?}",
                        self.queue_name,
                        self.id.clone().unwrap_or_else(|| "none".to_owned()),
                        e
                    ),
                    _ => {}
                }
                address.do_send(CreateConnection);
            }
        }
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocketProxyActor {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Err(_) => {
                ctx.close(Some(CloseReason::from(CloseCode::Error)));
                ctx.stop();
            }
            _ => (),
        }
    }
}

impl Handler<CreateConnection> for WebSocketProxyActor {
    type Result = ();

    fn handle(&mut self, _: CreateConnection, ctx: &mut Self::Context) -> Self::Result {
        if self.attempts >= MAX_RECONNECT_ATTEMPTS {
            warn!(
                "too much attempts to create stream queue: {}, id: {}",
                self.queue_name,
                self.id.clone().unwrap_or_else(|| "none".to_owned())
            );
            ctx.close(Some(CloseReason::from(CloseCode::Error)));
            ctx.stop();
            return;
        }
        info!(
            "create connection for queue: {}, id: {}, attempt: {}",
            self.queue_name,
            self.id.clone().unwrap_or_else(|| "none".to_owned()),
            self.attempts
        );
        match &self.id {
            None => ctx.add_message_stream(create_stream_all(
                self.registry.clone(),
                self.service_discovery.clone(),
                self.uri.clone(),
                self.headers.clone(),
                ctx.address(),
                self.attempts,
            )),
            Some(id) => ctx.add_message_stream(create_stream(
                self.registry.clone(),
                self.service_discovery.clone(),
                self.queue_name.clone(),
                id.clone(),
                self.uri.clone(),
                self.headers.clone(),
                ctx.address(),
                self.attempts,
            )),
        };

        self.attempts += 1;
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct ProxyRequest(Result<Result<ws::Frame, ProtocolError>, WsClientError>);

#[derive(Message)]
#[rtype(result = "()")]
struct CreateConnection;

fn create_stream(
    registry: Addr<RegistryActor>,
    service_discovery: Addr<ServiceDiscoveryActor>,
    queue_name: String,
    id: String,
    uri: Uri,
    headers: HeaderMap,
    ws_actor: Addr<WebSocketProxyActor>,
    attempt: u8,
) -> impl Stream<Item = ProxyRequest> {
    let client = Client::default();
    let stream = async_stream::try_stream! {
        sleep_between_reconnects(attempt).await;

        let address = get_address(&registry, queue_name, id).await;

        let mut request = client.ws(address.clone() + uri.path());
        for (key, value) in headers.into_iter() {
            request = request.set_header(key, value.clone());
        }

        let (_, codec) = request.connect().await?;

        if let Some(mut s) = reloading_stream_factory(codec, service_discovery).await {
            while let Some(c) = s.next().await {
                yield c
            }
        }

        ws_actor.do_send(CreateConnection);
    };

    stream.map(ProxyRequest)
}

fn create_stream_all(
    registry: Addr<RegistryActor>,
    service_discovery: Addr<ServiceDiscoveryActor>,
    uri: Uri,
    headers: HeaderMap,
    ws_actor: Addr<WebSocketProxyActor>,
    attempt: u8,
) -> impl Stream<Item = ProxyRequest> {
    let client = Client::default();

    let stream = async_stream::try_stream! {
        sleep_between_reconnects(attempt).await;

        let addresses = get_all_addresses(&registry).await;

        let requests = addresses.into_iter().map(|address| {
            let mut request = client.ws(address.clone() + uri.path());
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

        if let Some(mut s) = reloading_stream_factory(stream, service_discovery).await {
            while let Some(c) = s.next().await {
                yield c
            }
        }

        ws_actor.do_send(CreateConnection);
    };

    stream.map(ProxyRequest)
}

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
