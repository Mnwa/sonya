use crate::registry::{get_address, get_all_addresses, RegistryActor};
use actix::clock::sleep;
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
use std::time::Duration;

const MAX_RECONNECT_ATTEMPTS: u8 = 10;

pub struct WebSocketProxyActor {
    headers: HeaderMap,
    uri: Uri,
    registry: Addr<RegistryActor>,
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
        queue_name: String,
        id: Option<String>,
    ) -> Self {
        Self {
            headers: headers.headers().clone(),
            uri: headers.uri.clone(),
            registry,
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
                let attempts = self.attempts;
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
                ctx.spawn(
                    async move {
                        sleep(Duration::from_secs((attempts as f32).sqrt() as u64)).await;
                        address.do_send(CreateConnection)
                    }
                    .into_actor(self),
                );
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
        self.attempts += 1;
        info!(
            "create connection for queue: {}, id: {}, attempt: {}",
            self.queue_name,
            self.id.clone().unwrap_or_else(|| "none".to_owned()),
            self.attempts
        );
        match self.id.clone() {
            None => ctx.add_message_stream(create_stream_all(
                self.registry.clone(),
                self.uri.clone(),
                self.headers.clone(),
            )),
            Some(id) => ctx.add_message_stream(create_stream(
                self.registry.clone(),
                self.queue_name.clone(),
                id,
                self.uri.clone(),
                self.headers.clone(),
            )),
        };
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
    queue_name: String,
    id: String,
    uri: Uri,
    headers: HeaderMap,
) -> impl Stream<Item = ProxyRequest> {
    let client = Client::default();
    let stream = async_stream::try_stream! {
        let address = get_address(&registry, queue_name, id).await;

        let mut request = client.ws(address.clone() + uri.path());
        for (key, value) in headers.into_iter() {
            request = request.set_header(key, value.clone());
        }

        let (_, mut codec) = request.connect().await?;

        while let Some(c) = codec.next().await {
            yield c
        }
    };

    stream.map(ProxyRequest)
}

fn create_stream_all(
    registry: Addr<RegistryActor>,
    uri: Uri,
    headers: HeaderMap,
) -> impl Stream<Item = ProxyRequest> {
    let client = Client::default();

    let stream = async_stream::try_stream! {
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

        let mut stream = FlattenConcurrent::new(connections);

        while let Some(c) = stream.next().await {
            yield c
        }
    };

    stream.map(ProxyRequest)
}

struct FlattenConcurrent<S> {
    streams: Vec<S>,
}

impl<S> FlattenConcurrent<S> {
    fn new(streams: Vec<S>) -> Self {
        Self { streams }
    }
}

impl<S: Stream<Item = O> + Unpin, O> Stream for FlattenConcurrent<S> {
    type Item = O;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut empty_res_count = 0;
        for s in self.streams.iter_mut() {
            match s.poll_next_unpin(cx) {
                Poll::Ready(Some(r)) => return Poll::Ready(Some(r)),
                Poll::Ready(None) => empty_res_count += 1,
                Poll::Pending => continue,
            }
        }
        if empty_res_count == self.streams.len() {
            return Poll::Ready(None);
        }
        Poll::Pending
    }
}
