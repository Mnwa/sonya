mod registry;
mod service_discovery;
mod websocket_proxy;

use crate::registry::{get_address, get_all_addresses, RegistryActor, RegistryList};
use crate::service_discovery::ServiceDiscoveryActor;
use crate::websocket_proxy::WebSocketProxyActor;
use actix::Addr;
use actix_web::http::Uri;
use actix_web::middleware::Logger;
use actix_web::{post, web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use awc::Client;
use futures::future::Either;
use futures::{FutureExt, SinkExt, TryStreamExt};
use log::{error, info};
use web_queue_meta::message::EventMessage;
use web_queue_meta::queue_scope_factory;
use web_queue_meta::response::BaseQueueResponse;

async fn subscribe_queue_by_id_ws(
    req: HttpRequest,
    stream: web::Payload,
    registry: web::Data<Addr<RegistryActor>>,
    service_discovery: web::Data<Addr<ServiceDiscoveryActor>>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    ws::start(
        WebSocketProxyActor::new(
            req.head(),
            registry.as_ref().clone(),
            service_discovery.as_ref().clone(),
            queue_name,
            Some(id),
        ),
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
    service_discovery: web::Data<Addr<ServiceDiscoveryActor>>,
    info: web::Path<(String,)>,
) -> Result<HttpResponse, Error> {
    let queue_name = info.into_inner().0;
    ws::start(
        WebSocketProxyActor::new(
            req.head(),
            registry.as_ref().clone(),
            service_discovery.as_ref().clone(),
            queue_name,
            None,
        ),
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
                    back_rsp.insert_header((key.clone(), value.clone()));
                }

                let back_rsp = back_rsp.streaming(r.into_stream());
                Ok(back_rsp)
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
                back_rsp.insert_header((key.clone(), value.clone()));
            }

            let back_rsp = back_rsp.streaming(r.into_stream());
            Ok(back_rsp)
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

    let shards = std::env::var("DEFAULT_SHARDS")
        .unwrap_or_default()
        .split(';')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();

    let service_token = std::env::var("SERVICE_TOKEN").ok();
    let service_discovery_backend = std::env::var("SERVICE_DISCOVERY_BACKEND").ok();

    let registry = web::Data::new(RegistryActor::new(shards));

    #[cfg(feature = "api")]
    let mut registry_api_updater: Option<web::Data<RegistryApiUpdater>> = None;

    let (cx, rx) = futures::channel::oneshot::channel();

    let service_discovery: web::Data<Addr<ServiceDiscoveryActor>> = match service_discovery_backend
        .map(|s| Uri::from_maybe_shared(s).expect("invalid uri for service discovery backend"))
    {
        #[cfg(feature = "api")]
        None => {
            info!("chosen api service discovery");
            let (sender, factory) = service_discovery::api::factory();
            let service_discovery = web::Data::new(ServiceDiscoveryActor::new(
                factory,
                registry.get_ref().clone(),
                cx,
            ));
            registry_api_updater = Some(web::Data::new(RegistryApiUpdater(sender)));
            service_discovery
        }
        #[cfg(feature = "etcd")]
        Some(backend) if backend.scheme_str() == Some("etcd") => {
            info!("chosen etcd service discovery");
            web::Data::new(ServiceDiscoveryActor::new(
                service_discovery::etcd::factory(backend).await,
                registry.get_ref().clone(),
                cx,
            ))
        }
        s => panic!(
            "Invalid service discovery strategy accepted: {}",
            s.unwrap()
        ),
    };

    let result = futures::future::select(
        rx,
        HttpServer::new(move || {
            let mut app = App::new()
                .wrap(Logger::default())
                .app_data(registry.clone())
                .app_data(service_discovery.clone())
                .service(queue_scope_factory!(
                    create_queue,
                    send_to_queue,
                    close_queue,
                    subscribe_queue_by_id_ws,
                    subscribe_queue_by_id_longpoll,
                    subscribe_queue_ws,
                    subscribe_queue_longpoll,
                    service_token.clone()
                ));

            #[cfg(feature = "api")]
            if let Some(registry_updater) = registry_api_updater.clone() {
                app = app.app_data(registry_updater).service(service_registry_api);
            }
            app
        })
        .bind(address)?
        .run(),
    )
    .await;

    match result {
        Either::Left((l, _)) => match l {
            Ok(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "service discovery is down",
            )),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e)),
        },
        Either::Right((r, _)) => r,
    }
}

#[cfg(feature = "api")]
#[post("/registry")]
async fn service_registry_api(
    updater: web::Data<RegistryApiUpdater>,
    list: web::Json<RegistryList>,
) -> impl Responder {
    let mut updater = RegistryApiUpdater::clone(&updater);
    match updater.0.send(list.into_inner()).await {
        Ok(_) => Ok("UPDATED"),
        Err(_) => Err(actix_web::error::ErrorInternalServerError(
            "Service discovery was broken",
        )),
    }
}

#[derive(Clone)]
struct RegistryApiUpdater(futures::channel::mpsc::Sender<RegistryList>);
