use crate::websocket_proxy_client::WebSocketActorResponse;
use actix::prelude::*;
use actix_web_actors::ws;
use actix_web_actors::ws::{CloseCode, CloseReason, Frame};
use log::{info, warn};
use std::net::SocketAddr;
use tokio::sync::broadcast;

pub struct WebSocketProxyActor {
    receiver: Option<broadcast::Receiver<WebSocketActorResponse>>,
    ip: SocketAddr,
}

impl Actor for WebSocketProxyActor {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("new client {} connected", self.ip);
        match self.receiver.take() {
            Some(mut receiver) => {
                let stream = async_stream::stream! {
                   while let Ok(message) = receiver.recv().await {
                        yield message
                    }
                };
                ctx.add_stream(stream);
            }
            None => {
                warn!("client {} was aborted, empty receiver", self.ip);
                ctx.close(Some(CloseReason::from(CloseCode::Error)));
                ctx.stop()
            }
        }
    }
}

impl WebSocketProxyActor {
    pub fn new(
        receiver: Option<broadcast::Receiver<WebSocketActorResponse>>,
        ip: SocketAddr,
    ) -> Self {
        Self { receiver, ip }
    }
}

impl StreamHandler<WebSocketActorResponse> for WebSocketProxyActor {
    fn handle(&mut self, response: WebSocketActorResponse, ctx: &mut Self::Context) {
        match response {
            WebSocketActorResponse::Message(f) => match f.as_ref() {
                Frame::Text(b) => ctx.text(
                    std::str::from_utf8(b)
                        .map(|r| r.to_string())
                        .unwrap_or_else(|e| {
                            warn!(
                                "invalid utf message sent from queue, error: {}, client ip: {}",
                                e, self.ip
                            );
                            e.to_string()
                        }),
                ),
                Frame::Binary(b) => ctx.binary(b.clone()),
                Frame::Continuation(_) => {}
                Frame::Ping(p) => ctx.ping(p),
                Frame::Pong(p) => ctx.pong(p),
                Frame::Close(r) => {
                    ctx.close(r.clone());
                    ctx.stop()
                }
            },
            WebSocketActorResponse::Stopped => {
                warn!("closed connection {}", self.ip);
                ctx.close(Some(CloseReason::from(CloseCode::Error)));
                ctx.stop()
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
