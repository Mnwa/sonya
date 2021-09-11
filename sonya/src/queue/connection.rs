use actix::prelude::*;
use actix_web_actors::ws;
use actix_web_actors::ws::{CloseCode, CloseReason};
use log::{error, info};
use serde::Serialize;
use sonya_meta::message::UniqId;

pub struct QueueConnection<S> {
    id: Option<String>,
    queue_name: String,
    queue: Option<S>,
}

impl<S> QueueConnection<S> {
    pub fn new(id: Option<String>, queue_name: String, queue: S) -> Self {
        Self {
            id,
            queue_name,
            queue: Some(queue),
        }
    }
}

impl<S, T> Actor for QueueConnection<S>
where
    S: 'static + Stream<Item = BroadcastMessage<T>> + Unpin,
    T: 'static + Clone + Serialize + UniqId,
{
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.add_stream(self.queue.take().expect("queue is none"));

        info!(
            "created connection for queue: {}, id: {}",
            self.queue_name,
            self.id.clone().unwrap_or_else(|| "none".to_owned())
        );
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(
            "closed connection for queue: {}, id: {}",
            self.queue_name,
            self.id.clone().unwrap_or_else(|| "none".to_owned())
        );
    }
}

impl<S, T> StreamHandler<Result<ws::Message, ws::ProtocolError>> for QueueConnection<S>
where
    S: 'static + Stream<Item = BroadcastMessage<T>> + Unpin,
    T: 'static + Clone + Serialize + UniqId,
{
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Err(_) => ctx.stop(),
            _ => (),
        }
    }
}

impl<S, T> StreamHandler<BroadcastMessage<T>> for QueueConnection<S>
where
    S: 'static + Stream<Item = BroadcastMessage<T>> + Unpin,
    T: 'static + Clone + Serialize + UniqId,
{
    fn handle(&mut self, message: BroadcastMessage<T>, ctx: &mut Self::Context) {
        match message {
            BroadcastMessage::Message(m) => {
                let serialized = serde_json::to_string(&m);
                match serialized {
                    Ok(s) => {
                        info!(
                            "accepted message for queue: {}, id: {}, message: {}",
                            self.queue_name,
                            self.id.clone().unwrap_or_else(|| "none".to_owned()),
                            s
                        );
                        ctx.text(s)
                    }
                    Err(err) => {
                        error!(
                            "serialization error for queue: {}, id: {}, error: {}",
                            self.queue_name,
                            self.id.clone().unwrap_or_else(|| "none".to_owned()),
                            err
                        );
                        ctx.close(Some(CloseReason::from((CloseCode::Error, err.to_string()))));
                        ctx.stop()
                    }
                }
            }
            BroadcastMessage::Close => {
                ctx.close(Some(CloseReason::from(CloseCode::Normal)));
                ctx.stop()
            }
        }
    }
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub enum BroadcastMessage<T> {
    Message(T),
    Close,
}
