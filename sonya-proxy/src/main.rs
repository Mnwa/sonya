mod registry;
mod service_discovery;
mod websocket_proxy;
mod websocket_proxy_client;

use crate::{
    registry::{get_address, get_all_addresses, RegistryActor, RegistryList},
    service_discovery::ServiceDiscoveryActor,
    websocket_proxy::WebSocketProxyActor,
    websocket_proxy_client::{
        WebSocketActorResponse, WebSocketProxyClientsStorage, WebSocketProxyClientsStorageKey,
    },
};
use actix::Addr;
use actix_web::{
    dev::RequestHead, http::header::HeaderMap, middleware::Logger, web, App, Error, HttpRequest,
    HttpResponse, HttpServer, Responder,
};
use actix_web_actors::ws;
use awc::{
    http::header::{HeaderName, HeaderValue},
    ws::Frame,
    Client,
};
use futures::{future::Either, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use log::{error, info};
use serde::Deserialize;
use serde_json::Value;
use sonya_meta::message::RequestSequence;
use sonya_meta::{
    api::extract_any_data_from_query,
    api::service_token_guard,
    config::{get_config, Config, ServiceDiscovery},
    message::EventMessage,
    queue_scope_factory,
    response::BaseQueueResponse,
    tls::get_options_from_config,
};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

async fn subscribe_queue_by_id_ws(
    req: HttpRequest,
    stream: web::Payload,
    registry: web::Data<Addr<RegistryActor>>,
    service_discovery: web::Data<Addr<ServiceDiscoveryActor>>,
    info: web::Path<(String, String)>,
    proxies_storage: web::Data<WebSocketProxyClientsStorage>,
    config: web::Data<Config>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    let receiver = create_receiver(
        queue_name,
        id.into(),
        proxies_storage.as_ref(),
        config.as_ref(),
        req.head(),
        registry.as_ref(),
        service_discovery.as_ref(),
        get_sequence_from_req(&req),
    )
    .await;

    ws::start(
        WebSocketProxyActor::new(receiver, req.peer_addr().unwrap()),
        &req,
        stream,
    )
}

async fn subscribe_queue_by_id_longpoll(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
    info: web::Path<(String, String)>,
) -> impl Responder {
    let (queue_name, id) = info.into_inner();
    let address = get_address(registry.get_ref(), queue_name, id).await;

    let client = Client::default();

    let response = client
        .request_from(address + prepare_path(&req).as_str(), req.head())
        .send()
        .await;

    match response {
        Ok(mut r) => {
            let body = r.body().await;

            let headers = r.headers();

            match body {
                Ok(b) => {
                    let mut response = HttpResponse::Ok().body(b);

                    *response.headers_mut() = headers.clone();

                    Ok(response)
                }
                Err(e) => Err(actix_web::error::ErrorGone(e)),
            }
        }
        Err(e) => Err(actix_web::error::ErrorGone(e)),
    }
}

async fn subscribe_queue_ws(
    req: HttpRequest,
    stream: web::Payload,
    registry: web::Data<Addr<RegistryActor>>,
    service_discovery: web::Data<Addr<ServiceDiscoveryActor>>,
    info: web::Path<(String,)>,
    proxies_storage: web::Data<WebSocketProxyClientsStorage>,
    config: web::Data<Config>,
) -> Result<HttpResponse, Error> {
    let queue_name = info.into_inner().0;
    let receiver = create_receiver(
        queue_name,
        None,
        proxies_storage.as_ref(),
        config.as_ref(),
        req.head(),
        registry.as_ref(),
        service_discovery.as_ref(),
        get_sequence_from_req(&req),
    )
    .await;

    ws::start(
        WebSocketProxyActor::new(receiver, req.peer_addr().unwrap()),
        &req,
        stream,
    )
}

async fn subscribe_queue_longpoll(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
) -> Result<HttpResponse, Error> {
    let addresses = get_all_addresses(registry.get_ref()).await;

    let client = Client::default();

    let responses = futures::stream::iter(addresses)
        .then(|address| {
            client
                .request_from(address + prepare_path(&req).as_str(), req.head())
                .send()
        })
        .map_err(actix_web::error::ErrorGone)
        .and_then(|mut r| r.json::<Vec<Value>>().map_err(actix_web::error::ErrorGone))
        .try_collect::<Vec<Vec<Value>>>()
        .await?;

    let mut result = Vec::new();

    for response in responses {
        result.extend_from_slice(&response)
    }

    Ok(HttpResponse::Ok().json(result))
}

fn get_sequence_from_req(req: &HttpRequest) -> SequenceQuery {
    extract_any_data_from_query(req.head()).unwrap_or_default()
}

fn create_ws_header_map(config: &Config, r_headers: &RequestHead) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("connection"),
        HeaderValue::from_static("Upgrade"),
    );
    headers.insert(
        HeaderName::from_static("upgrade"),
        HeaderValue::from_static("websocket"),
    );
    headers.insert(
        HeaderName::from_static("sec-websocket-version"),
        HeaderValue::from_str(config.websocket.version.as_str())
            .expect("invalid Sec-WebSocket-Version"),
    );
    headers.insert(
        HeaderName::from_static("sec-websocket-key"),
        HeaderValue::from_str(config.websocket.key.as_str()).expect("invalid Sec-WebSocket-Key"),
    );
    if let Some(token) = r_headers.headers.get("authorization") {
        headers.insert(HeaderName::from_static("authorization"), token.clone());
    }
    headers
}

async fn create_receiver(
    queue_name: String,
    id: Option<String>,
    proxies_storage: &WebSocketProxyClientsStorage,
    config: &Config,
    head: &RequestHead,
    registry: &Addr<RegistryActor>,
    service_discovery: &Addr<ServiceDiscoveryActor>,
    SequenceQuery {
        access_token,
        sequence,
    }: SequenceQuery,
) -> Option<tokio::sync::broadcast::Receiver<WebSocketActorResponse>> {
    proxies_storage
        .subscribe(
            WebSocketProxyClientsStorageKey::new(queue_name, id),
            create_ws_header_map(config, head),
            Addr::clone(registry),
            Addr::clone(service_discovery),
            config.garbage_collector.interval,
            access_token,
            sequence,
        )
        .await
}

#[derive(Deserialize, Default)]
struct SequenceQuery {
    sequence: RequestSequence,
    access_token: Option<String>,
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
        .request_from(address.clone() + prepare_path(&req).as_str(), req.head())
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

async fn create_queue(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
) -> impl Responder {
    base_diagonal_proxy(req, registry).await
}

async fn delete_from_queue(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
) -> impl Responder {
    base_diagonal_proxy(req, registry).await
}

async fn close_queue(req: HttpRequest, registry: web::Data<Addr<RegistryActor>>) -> impl Responder {
    base_diagonal_proxy(req, registry).await
}

async fn base_diagonal_proxy(
    req: HttpRequest,
    registry: web::Data<Addr<RegistryActor>>,
) -> impl Responder {
    let addresses = get_all_addresses(registry.get_ref()).await;

    let client = Client::default();

    let requests = addresses.into_iter().map(|address| {
        client
            .request_from(address + prepare_path(&req).as_str(), req.head())
            .send()
    });

    let result: Result<Vec<_>, _> = futures::future::join_all(requests)
        .await
        .into_iter()
        .collect();

    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
        Err(e) => {
            error!("queue proxy error: {:#?}", e);
            Err(actix_web::error::ErrorGone(
                "One of shards is not responding",
            ))
        }
    }
}

fn prepare_path(req: &HttpRequest) -> String {
    let uri = req.head().uri.clone();

    match uri.path_and_query() {
        None => uri.path().to_string(),
        Some(p) => p.to_string(),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let config = get_config();
    let shared_config = web::Data::new(config.clone());

    let address = config
        .addr
        .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8081));

    let secure = config.secure;

    let registry = web::Data::new(RegistryActor::new(
        match &config.service_discovery {
            #[cfg(feature = "api")]
            Some(ServiceDiscovery::Api { default }) => default.clone(),
            #[cfg(feature = "etcd")]
            Some(ServiceDiscovery::Etcd { default, .. }) => default.clone(),
            _ => None,
        }
        .unwrap_or_default(),
    ));

    #[cfg(feature = "api")]
    let mut registry_api_updater: Option<web::Data<RegistryApiUpdater>> = None;

    let (cx, rx) = futures::channel::oneshot::channel();

    let service_discovery: web::Data<Addr<ServiceDiscoveryActor>> = match config.service_discovery {
        #[cfg(feature = "api")]
        None | Some(ServiceDiscovery::Api { .. }) => {
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
        Some(ServiceDiscovery::Etcd { hosts, prefix, .. }) => {
            info!("chosen etcd service discovery");
            web::Data::new(ServiceDiscoveryActor::new(
                service_discovery::etcd::factory(hosts, prefix),
                registry.get_ref().clone(),
                cx,
            ))
        }
        #[cfg(not(feature = "api"))]
        t => panic!("Invalid service discovery type accepted: {}", t.unwrap()),
    };

    let web_socket_proxies = web::Data::new(WebSocketProxyClientsStorage::default());
    let wsp = web_socket_proxies.clone();

    let garbage_interval = config.garbage_collector.interval;
    actix::spawn(async move {
        let mut interval = actix::clock::interval(Duration::from_secs(garbage_interval));

        loop {
            interval.tick().await;
            wsp.clear().await;
            info!("clearing websocket proxies storage")
        }
    });

    let server = HttpServer::new(move || {
        let mut app = App::new()
            .wrap(Logger::default())
            .app_data(registry.clone())
            .app_data(service_discovery.clone())
            .app_data(web_socket_proxies.clone())
            .app_data(shared_config.clone())
            .service(queue_scope_factory!(
                create_queue,
                delete_from_queue,
                send_to_queue,
                close_queue,
                subscribe_queue_by_id_ws,
                subscribe_queue_by_id_longpoll,
                subscribe_queue_ws,
                subscribe_queue_longpoll,
                &secure,
            ));

        #[cfg(feature = "api")]
        if let Some(registry_updater) = registry_api_updater.clone() {
            let route = match &secure {
                None => web::post().to(service_registry_api),
                Some(s) => web::post()
                    .guard(service_token_guard(s))
                    .to(service_registry_api),
            };

            app = app
                .app_data(registry_updater)
                .service(web::resource("/registry").route(route));
        }
        app
    });

    let result = futures::future::select(rx, {
        match config.tls {
            None => server.bind(address)?,
            Some(opts) => server.bind_openssl(address, get_options_from_config(opts))?,
        }
        .run()
    })
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
async fn service_registry_api(
    updater: web::Data<RegistryApiUpdater>,
    list: web::Json<RegistryList>,
) -> impl Responder {
    let mut updater = RegistryApiUpdater::clone(&updater);
    match updater.0.send(list.into_inner()).await {
        Ok(_) => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
        Err(_) => Err(actix_web::error::ErrorInternalServerError(
            "Service discovery was broken",
        )),
    }
}

#[derive(Clone)]
struct RegistryApiUpdater(futures::channel::mpsc::Sender<RegistryList>);
