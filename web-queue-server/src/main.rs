use crate::queue::connection::{BroadcastMessage, QueueConnection};
use crate::queue::map::Queue;
use actix_web::middleware::Logger;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use futures::future::Either;
use futures::FutureExt;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{SystemTime, UNIX_EPOCH};
use web_queue_meta::config::{
    get_config, DefaultQueues, ServiceDiscovery, ServiceDiscoveryInstanceOptions,
};
use web_queue_meta::message::EventMessage;
use web_queue_meta::queue_scope_factory;
use web_queue_meta::response::BaseQueueResponse;
use web_queue_meta::tls::get_options_from_config;

pub mod queue;
mod service_discovery;

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
    let config = get_config();

    let address = config
        .addr
        .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080));
    let secure = config.secure;
    let standard_queues: DefaultQueues = config.queue.map(|q| q.default).unwrap_or_default();

    let (cx, rx) = futures::channel::oneshot::channel();

    match config.service_discovery {
        #[cfg(feature = "api")]
        None | Some(ServiceDiscovery::Api { .. }) => {}
        #[cfg(feature = "etcd")]
        Some(ServiceDiscovery::Etcd {
            hosts,
            prefix,
            instance_opts,
            ..
        }) => {
            info!("chosen etcd service discovery");

            let ServiceDiscoveryInstanceOptions {
                instance_addr,
                instance_id,
            } = instance_opts.expect("expected instance addr in config");

            actix::spawn(
                service_discovery::etcd::register_instance(
                    hosts,
                    prefix.unwrap_or_default(),
                    instance_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                    instance_addr,
                )
                .inspect(|_| {
                    let _ = cx.send(());
                }),
            );
        }
        #[cfg(not(feature = "api"))]
        t => panic!("Invalid service discovery type accepted: {}", t.unwrap()),
    };

    let queue = web::Data::new(RuntimeQueue::new(Queue::from(standard_queues)));

    let server = HttpServer::new(move || {
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
                &secure,
            ))
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

type RuntimeQueue = tokio::sync::RwLock<Queue<EventMessage>>;
