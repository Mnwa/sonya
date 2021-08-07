use crate::queue::connection::{BroadcastMessage, QueueConnection};
use crate::queue::map::Queue;
use actix_web::middleware::Logger;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use std::time::{SystemTime, UNIX_EPOCH};
use web_queue_meta::api::queue_scope_factory;
use web_queue_meta::message::EventMessage;
use web_queue_meta::response::BaseQueueResponse;

pub mod queue;

async fn subscribe_queue_ws(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<RuntimeQueue>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    let guard = srv.read().await;
    let queue_connection = guard.subscribe_queue(queue_name);
    match queue_connection {
        None => Err(actix_web::error::ErrorNotFound("queue not created")),
        Some(qc) => ws::start(QueueConnection::new(id, qc), &req, stream),
    }
}

async fn subscribe_queue_longpoll(
    srv: web::Data<RuntimeQueue>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    let guard = srv.read().await;
    let queue_connection = guard.subscribe_queue(queue_name);
    match queue_connection {
        None => Err(actix_web::error::ErrorNotFound("Queue not found")),
        Some(mut qc) => loop {
            let message = qc.recv().await;
            match message {
                Ok(BroadcastMessage::Message(s)) if s.id == id => {
                    return Ok(HttpResponse::Ok().json(s))
                }
                Ok(BroadcastMessage::Message(_)) => continue,
                _ => return Err(actix_web::error::ErrorGone("Queue was closed")),
            }
        },
    }
}

async fn create_queue(srv: web::Data<RuntimeQueue>, info: web::Path<String>) -> impl Responder {
    let queue_name = info.into_inner();
    let mut guard = srv.write().await;
    match guard.create_queue(queue_name) {
        None => Err(actix_web::error::ErrorConflict("Queue already created")),
        Some(_) => Ok(HttpResponse::Created().json(BaseQueueResponse { success: true })),
    }
}

async fn send_to_queue(
    srv: web::Data<RuntimeQueue>,
    info: web::Path<String>,
    message: web::Json<EventMessage>,
) -> impl Responder {
    let queue_name = info.into_inner();
    let guard = srv.read().await;
    let mut message = message.into_inner();
    message.timestamp.get_or_insert(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("fail to get time")
            .as_secs() as usize,
    );
    match guard.send_to_queue(queue_name, message) {
        false => Err(actix_web::error::ErrorNotFound("Queue not found")),
        true => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
    }
}

async fn close_queue(srv: web::Data<RuntimeQueue>, info: web::Path<String>) -> impl Responder {
    let queue_name = info.into_inner();
    let mut guard = srv.write().await;
    match guard.close_queue(queue_name) {
        false => Err(actix_web::error::ErrorNotFound("Queue not found")),
        true => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
    }
}

#[actix_web::main]
async fn main() -> tokio::io::Result<()> {
    let address = std::env::var("ADDR").unwrap_or_else(|_| String::from("0.0.0.0:8080"));
    env_logger::init();

    let queue = web::Data::new(RuntimeQueue::default());
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(queue.clone())
            .service(queue_scope_factory(
                create_queue,
                send_to_queue,
                close_queue,
                subscribe_queue_longpoll,
                subscribe_queue_ws,
            ))
    })
    .bind(address)?
    .run()
    .await
}

type RuntimeQueue = tokio::sync::RwLock<Queue<EventMessage>>;
