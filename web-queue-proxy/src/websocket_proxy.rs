use actix::prelude::*;
use actix_web_actors::ws;
use actix_web_actors::ws::{CloseCode, CloseReason, Frame, ProtocolError};
use futures::StreamExt;
use log::{info, warn};

pub struct WebSocketProxyActor<T> {
    stream: Option<T>,
    shard: String,
}

impl<T> Actor for WebSocketProxyActor<T>
where
    T: 'static + Unpin + Stream<Item = Result<ws::Frame, ProtocolError>>,
{
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let stream = self.stream.take().expect("stream is none");
        ctx.add_stream(stream.map(ProxyRequest));
    }
}

impl<T> WebSocketProxyActor<T>
where
    T: 'static + Unpin + Stream<Item = Result<ws::Frame, ProtocolError>>,
{
    pub fn new(stream: T, shard: String) -> Self {
        Self {
            stream: Some(stream),
            shard,
        }
    }
}

impl<T> StreamHandler<ProxyRequest> for WebSocketProxyActor<T>
where
    T: 'static + Unpin + Stream<Item = Result<ws::Frame, ProtocolError>>,
{
    fn handle(&mut self, ProxyRequest(frame): ProxyRequest, ctx: &mut Self::Context) {
        info!("accepted new frame from shard {}", self.shard);
        match frame {
            Ok(f) => match f {
                Frame::Text(b) => ctx.text(
                    std::str::from_utf8(&b)
                        .map(|r| r.to_string())
                        .unwrap_or_else(|e| {
                            warn!(
                                "invalid utf message sent from shard {}, error: {}",
                                self.shard, e
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
            },
            Err(_) => {
                ctx.close(Some(CloseReason::from(CloseCode::Error)));
                ctx.stop()
            }
        }
    }
}

impl<T> StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocketProxyActor<T>
where
    T: 'static + Unpin + Stream<Item = Result<ws::Frame, ProtocolError>>,
{
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

#[derive(Message)]
#[rtype(result = "()")]
struct ProxyRequest(Result<ws::Frame, ProtocolError>);
