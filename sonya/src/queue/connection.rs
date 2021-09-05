use actix::prelude::*;
use actix_web_actors::ws;
use actix_web_actors::ws::{CloseCode, CloseReason};
use log::{error, info};
use serde::Serialize;
use sonya_meta::message::UniqId;
use tokio::sync::broadcast;

pub struct QueueConnection<T> {
    id: Option<String>,
    queue_name: String,
    queue: Option<broadcast::Receiver<BroadcastMessage<T>>>,
}

impl<T> QueueConnection<T> {
    pub fn new(
        id: Option<String>,
        queue_name: String,
        queue: broadcast::Receiver<BroadcastMessage<T>>,
    ) -> Self {
        Self {
            id,
            queue_name,
            queue: Some(queue),
        }
    }
}

impl<T: 'static + Clone + Serialize + UniqId> Actor for QueueConnection<T> {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let mut queue = self.queue.take().expect("queue is none");
        let stream = async_stream::stream! {
             while let Ok(message) = queue.recv().await {
                yield message
            }
        };

        ctx.add_stream(stream);

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

impl<T: 'static + Clone + Serialize + UniqId> StreamHandler<Result<ws::Message, ws::ProtocolError>>
    for QueueConnection<T>
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

impl<T: 'static + Clone + Serialize + UniqId> StreamHandler<BroadcastMessage<T>>
    for QueueConnection<T>
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
