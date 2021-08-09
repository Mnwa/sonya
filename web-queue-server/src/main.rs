use crate::queue::connection::{BroadcastMessage, QueueConnection};
use crate::queue::map::Queue;
use actix_web::middleware::Logger;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use std::time::{SystemTime, UNIX_EPOCH};
use web_queue_meta::message::EventMessage;
use web_queue_meta::queue_scope_factory;
use web_queue_meta::response::BaseQueueResponse;

pub mod queue;

async fn subscribe_queue_by_id_ws(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<RuntimeQueue>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    let guard = srv.read().await;
    let queue_connection = guard
        .subscribe_queue_by_id(queue_name.clone(), id.clone())
        .await;
    ws_response_factory!(queue_connection, Some(id), queue_name, req, stream)
}

async fn subscribe_queue_by_id_longpoll(
    srv: web::Data<RuntimeQueue>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    let guard = srv.read().await;
    let queue_connection = guard.subscribe_queue_by_id(queue_name, id).await;
    longpoll_response_factory!(queue_connection)
}

async fn subscribe_queue_ws(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<RuntimeQueue>,
    info: web::Path<(String,)>,
) -> Result<HttpResponse, Error> {
    let queue_name = info.into_inner().0;
    let guard = srv.read().await;
    let queue_connection = guard.subscribe_queue(queue_name.clone());
    ws_response_factory!(queue_connection, None, queue_name, req, stream)
}

async fn subscribe_queue_longpoll(
    srv: web::Data<RuntimeQueue>,
    info: web::Path<(String,)>,
) -> Result<HttpResponse, Error> {
    let queue_name = info.into_inner().0;
    let guard = srv.read().await;
    let queue_connection = guard.subscribe_queue(queue_name);
    longpoll_response_factory!(queue_connection)
}

#[macro_export]
macro_rules! ws_response_factory {
    ($queue_connection:ident, $id:expr, $queue_name:ident, $req:ident, $stream:ident) => {
        match $queue_connection {
            None => Err(actix_web::error::ErrorNotFound("Queue not found")),
            Some(qc) => ws::start(QueueConnection::new($id, $queue_name, qc), &$req, $stream),
        }
    };
}

#[macro_export]
macro_rules! longpoll_response_factory {
    ($queue_connection:ident) => {
        match $queue_connection {
            None => Err(actix_web::error::ErrorNotFound("Queue not found")),
            Some(mut qc) => {
                let message = qc.recv().await;
                match message {
                    Ok(BroadcastMessage::Message(s)) => Ok(HttpResponse::Ok().json(s)),
                    _ => Err(actix_web::error::ErrorGone("Queue was closed")),
                }
            }
        }
    };
}

async fn create_queue(srv: web::Data<RuntimeQueue>, info: web::Path<String>) -> impl Responder {
    let queue_name = info.into_inner();
    let mut guard = srv.write().await;
    match guard.create_queue(queue_name) {
        false => Err(actix_web::error::ErrorConflict("Queue already created")),
        true => Ok(HttpResponse::Created().json(BaseQueueResponse { success: true })),
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
    match guard.send_to_queue(queue_name, message).await {
        false => Err(actix_web::error::ErrorNotFound("Queue not found")),
        true => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
    }
}

async fn close_queue(srv: web::Data<RuntimeQueue>, info: web::Path<String>) -> impl Responder {
    let queue_name = info.into_inner();
    let mut guard = srv.write().await;
    match guard.close_queue(queue_name).await {
        false => Err(actix_web::error::ErrorNotFound("Queue not found")),
        true => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
    }
}

#[actix_web::main]
async fn main() -> tokio::io::Result<()> {
    env_logger::init();

    let address = std::env::var("ADDR").unwrap_or_else(|_| String::from("0.0.0.0:8080"));
    let service_token = std::env::var("SERVICE_TOKEN").ok();

    let queue = web::Data::new(RuntimeQueue::default());
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(queue.clone())
            .service(queue_scope_factory!(
                create_queue,
                send_to_queue,
                close_queue,
                subscribe_queue_by_id_ws,
                subscribe_queue_by_id_longpoll,
                subscribe_queue_ws,
                subscribe_queue_longpoll,
                service_token.clone()
            ))
    })
    .bind(address)?
    .run()
    .await
}

type RuntimeQueue = tokio::sync::RwLock<Queue<EventMessage>>;
