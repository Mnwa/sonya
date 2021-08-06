mod registry;
mod websocket_proxy;

use crate::registry::{get_address, get_all_addresses, RegistryActor};
use crate::websocket_proxy::WebSocketProxyActor;
use actix::Addr;
use actix_web::client::Client;
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::{
    get, post, web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder, ResponseError,
};
use actix_web_actors::ws;
use futures::TryStreamExt;
use log::error;
use web_queue_meta::api::queue_scope_factory;
use web_queue_meta::message::EventMessage;
use web_queue_meta::response::BaseQueueResponse;

async fn subscribe_queue_ws(
    req: HttpRequest,
    stream: web::Payload,
    registry: web::Data<Addr<RegistryActor>>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();

    let client = Client::default();

    let address = get_address(registry.get_ref(), queue_name, id).await;

    let mut request = client.ws(address.clone() + req.path());
    for (key, value) in req.headers() {
        request = request.set_header(key.clone(), value.clone());
    }
    let response = request.connect().await;

    match response {
        Ok((_r, s)) => ws::start(WebSocketProxyActor::new(s, address.clone()), &req, stream),
        Err(e) => {
            error!(
                "subscribe queue web socket proxy error ({}): {:#?}",
                address, e
            );
            Err(actix_web::error::ErrorGone(
                "One of shards is not responding",
            ))
        }
    }
}

#[get("/listen/longpoll/{queue_name}/{uniq_id}")]
async fn subscribe_queue_longpoll(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();

    let client = Client::default();

    let address = get_address(registry.get_ref(), queue_name, id).await;

    let response = client
        .request_from(address.clone() + req.path(), req.head())
        .send()
        .await;

    match response {
        Ok(r) => {
            let mut back_rsp = HttpResponse::build(r.status());
            for (key, value) in r.headers() {
                back_rsp.set_header(key.clone(), value.clone());
            }

            let back_rsp = back_rsp.streaming(r.into_stream());
            Ok(back_rsp)
        }
        Err(err) if err.status_code() == StatusCode::NOT_FOUND => {
            Err(actix_web::error::ErrorNotFound("Queue not found"))
        }
        Err(err) if err.status_code() == StatusCode::GONE => {
            Err(actix_web::error::ErrorGone("Queue was closed"))
        }
        Err(e) => {
            error!(
                "subscribe queue longpoll proxy error ({}): {:#?}",
                address, e
            );
            Err(actix_web::error::ErrorGone(
                "One of shards is not responding",
            ))
        }
    }
}

#[post("/create/{queue_name}")]
async fn create_queue(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
) -> impl Responder {
    let addresses = get_all_addresses(registry.get_ref()).await;

    let client = Client::default();

    let requests = addresses
        .into_iter()
        .map(|address| client.request_from(address + req.path(), req.head()).send());

    let result: Result<Vec<_>, _> = futures::future::join_all(requests)
        .await
        .into_iter()
        .collect();

    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
        Err(err) if err.status_code() == StatusCode::CONFLICT => {
            Err(actix_web::error::ErrorConflict("Queue already created"))
        }
        Err(e) => {
            error!("create queue proxy error: {:#?}", e);
            Err(actix_web::error::ErrorGone(
                "One of shards is not responding",
            ))
        }
    }
}

#[post("/send/{queue_name}")]
async fn send_to_queue(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
    info: web::Path<String>,
    message: web::Json<EventMessage>,
) -> impl Responder {
    let queue_name = info.into_inner();

    let client = Client::default();

    let address = get_address(registry.get_ref(), queue_name, message.id.clone()).await;

    let response = client
        .request_from(address.clone() + req.path(), req.head())
        .send_json(&message.into_inner())
        .await;

    match response {
        Ok(r) => {
            let mut back_rsp = HttpResponse::build(r.status());
            for (key, value) in r.headers() {
                back_rsp.set_header(key.clone(), value.clone());
            }

            let back_rsp = back_rsp.streaming(r.into_stream());
            Ok(back_rsp)
        }
        Err(err) if err.status_code() == StatusCode::NOT_FOUND => {
            Err(actix_web::error::ErrorNotFound("Queue not found"))
        }
        Err(e) => {
            error!("send to queue proxy error ({}): {:#?}", address, e);
            Err(actix_web::error::ErrorGone(
                "One of shards is not responding",
            ))
        }
    }
}

#[post("/close/{queue_name}")]
async fn close_queue(req: HttpRequest, registry: web::Data<Addr<RegistryActor>>) -> impl Responder {
    let addresses = get_all_addresses(registry.get_ref()).await;

    let client = Client::default();

    let requests = addresses
        .into_iter()
        .map(|address| client.request_from(address + req.path(), req.head()).send());

    let result: Result<Vec<_>, _> = futures::future::join_all(requests)
        .await
        .into_iter()
        .collect();

    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
        Err(err) if err.status_code() == StatusCode::NOT_FOUND => {
            Err(actix_web::error::ErrorNotFound("Queue not found"))
        }
        Err(e) => {
            error!("closing queue proxy error: {:#?}", e);
            Err(actix_web::error::ErrorGone(
                "One of shards is not responding",
            ))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let address = std::env::var("ADDR").unwrap_or_else(|_| String::from("0.0.0.0:8081"));
    env_logger::init();

    let shards = std::env::var("SHARDS")
        .expect("no one shard was not get from SHARDS env")
        .split(";")
        .map(String::from)
        .collect();

    let registry = RegistryActor::new(shards);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .data(registry.clone())
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
