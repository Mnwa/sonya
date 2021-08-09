mod registry;
mod websocket_proxy;

use crate::registry::{get_address, get_all_addresses, RegistryActor};
use crate::websocket_proxy::WebSocketProxyActor;
use actix::Addr;
use actix_web::client::Client;
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder, ResponseError};
use actix_web_actors::ws;
use futures::{FutureExt, TryStreamExt};
use log::error;
use web_queue_meta::message::EventMessage;
use web_queue_meta::queue_scope_factory;
use web_queue_meta::response::BaseQueueResponse;

async fn subscribe_queue_by_id_ws(
    req: HttpRequest,
    stream: web::Payload,
    registry: web::Data<Addr<RegistryActor>>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    ws::start(
        WebSocketProxyActor::new(req.head(), registry.as_ref().clone(), queue_name, Some(id)),
        &req,
        stream,
    )
}

async fn subscribe_queue_by_id_longpoll(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();

    let client = Client::default();

    let address = get_address(registry.get_ref(), queue_name, id).await;

    let response_result = client
        .request_from(address.clone() + req.path(), req.head())
        .send()
        .map(|r| (r, address))
        .await;

    logpoll_response_factory!(response_result)
}

async fn subscribe_queue_ws(
    req: HttpRequest,
    stream: web::Payload,
    registry: web::Data<Addr<RegistryActor>>,
    info: web::Path<(String,)>,
) -> Result<HttpResponse, Error> {
    let queue_name = info.into_inner().0;
    ws::start(
        WebSocketProxyActor::new(req.head(), registry.as_ref().clone(), queue_name, None),
        &req,
        stream,
    )
}

async fn subscribe_queue_longpoll(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
) -> Result<HttpResponse, Error> {
    let client = Client::default();

    let addresses = get_all_addresses(registry.get_ref()).await;

    let requests = addresses.into_iter().map(|address| {
        client
            .request_from(address.clone() + req.path(), req.head())
            .send()
            .map(|r| (r, address))
    });
    let (response_result, _, _) = futures::future::select_all(requests).await;

    logpoll_response_factory!(response_result)
}

#[macro_export]
macro_rules! logpoll_response_factory {
    ($response:ident) => {{
        let (response, address) = $response;
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
    }};
}

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
    env_logger::init();

    let address = std::env::var("ADDR").unwrap_or_else(|_| String::from("0.0.0.0:8081"));

    let shards = std::env::var("SHARDS")
        .expect("no one shard was not get from SHARDS env")
        .split(';')
        .map(String::from)
        .collect();

    let service_token = std::env::var("SERVICE_TOKEN").ok();

    let registry = RegistryActor::new(shards);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .data(registry.clone())
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
