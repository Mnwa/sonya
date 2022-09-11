use crate::queue::connection::{BroadcastMessage, QueueConnection};
use crate::queue::map::{Queue, QueueResult, Subscription};
use actix_web::middleware::Logger;
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use futures::future::Either;
use futures::{FutureExt, StreamExt, TryStreamExt};
use log::{error, info};
use serde::{Deserialize, Serialize};
use sonya_meta::api::extract_any_data_from_query;
use sonya_meta::config::{get_config, ServiceDiscovery, ServiceDiscoveryInstanceOptions};
use sonya_meta::message::{EventMessage, RequestSequence, UniqId};
use sonya_meta::queue_scope_factory;
use sonya_meta::response::BaseQueueResponse;
use sonya_meta::tls::get_options_from_config;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub mod queue;
mod service_discovery;

async fn subscribe_queue_by_id_ws(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Queue<EventMessage>>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    let sequence = get_sequence_from_req(&req);
    let queue_connection = srv.subscribe_queue_by_id(queue_name.clone(), id.clone(), sequence);
    ws_response_factory(queue_connection, queue_name, Some(id), &req, stream).await
}

async fn subscribe_queue_by_id_longpoll(
    req: HttpRequest,
    srv: web::Data<Queue<EventMessage>>,
    info: web::Path<(String, String)>,
) -> Result<HttpResponse, Error> {
    let (queue_name, id) = info.into_inner();
    let sequence = get_sequence_from_req(&req);
    let queue_connection = srv.subscribe_queue_by_id(queue_name, id, sequence);
    longpoll_response_factory(queue_connection).await
}

fn get_sequence_from_req(req: &HttpRequest) -> RequestSequence {
    let SequenceQuery { sequence } = extract_any_data_from_query(req.head()).unwrap_or_default();
    sequence
}

async fn subscribe_queue_ws(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Queue<EventMessage>>,
    info: web::Path<(String,)>,
) -> Result<HttpResponse, Error> {
    let queue_name = info.into_inner().0;
    let sequence = get_sequence_from_req(&req);
    let queue_connection = srv.subscribe_queue(queue_name.clone(), sequence);
    ws_response_factory(queue_connection, queue_name, None, &req, stream).await
}

async fn subscribe_queue_longpoll(
    req: HttpRequest,
    srv: web::Data<Queue<EventMessage>>,
    info: web::Path<(String,)>,
) -> Result<HttpResponse, Error> {
    let queue_name = info.into_inner().0;
    let sequence = get_sequence_from_req(&req);
    let queue_connection = srv.subscribe_queue(queue_name, sequence);
    longpoll_response_factory(queue_connection).await
}

async fn ws_response_factory<T>(
    queue: QueueResult<Subscription<'static, T>>,
    queue_name: String,
    id: Option<String>,
    req: &HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error>
where
    T: 'static + Serialize + UniqId,
{
    match queue {
        Ok(Subscription {
            stream: Some(q),
            preloaded_count: _,
        }) => ws::start(QueueConnection::new(id, queue_name, q), req, stream),
        Ok(Subscription {
            stream: None,
            preloaded_count: _,
        }) => Err(actix_web::error::ErrorNotFound("Queue Not Found")),
        Err(e) => {
            error!("websocket subscribe error {:?}", e);
            Err(actix_web::error::ErrorInternalServerError(
                "Subscription error",
            ))
        }
    }
}

async fn longpoll_response_factory<T>(
    queue: QueueResult<Subscription<'static, T>>,
) -> Result<HttpResponse, Error>
where
    T: 'static + Serialize,
{
    match queue {
        Ok(Subscription {
            stream: Some(q),
            preloaded_count: prev_len,
        }) => {
            let messages: Result<Vec<_>, _> = q
                .take(prev_len.unwrap_or(1).max(1))
                .map(|m| match m {
                    BroadcastMessage::Message(s) => Ok(s),
                    _ => Err(actix_web::error::ErrorGone("Queue was closed")),
                })
                .try_collect()
                .await;

            messages.map(|m| HttpResponse::Ok().json(m))
        }
        Ok(Subscription {
            stream: None,
            preloaded_count: _,
        }) => Err(actix_web::error::ErrorNotFound("Queue Not Found")),
        Err(e) => {
            error!("longpoll subscribe error {:?}", e);
            Err(actix_web::error::ErrorInternalServerError(
                "Subscription error",
            ))
        }
    }
}

async fn create_queue(
    srv: web::Data<Queue<EventMessage>>,
    info: web::Path<String>,
) -> impl Responder {
    let queue_name = info.into_inner();
    match srv.create_queue(queue_name) {
        Err(e) => {
            error!("creating queue error {}", e);
            Err(actix_web::error::ErrorInternalServerError(
                "Queue was not created",
            ))
        }
        Ok(_) => Ok(HttpResponse::Created().json(BaseQueueResponse { success: true })),
    }
}

async fn delete_from_queue(
    srv: web::Data<Queue<EventMessage>>,
    info: web::Path<(String, String)>,
) -> impl Responder {
    let (queue_name, id) = info.into_inner();
    match srv.delete_queue(queue_name, id) {
        Err(e) => {
            error!("deleting queue error {}", e);
            Err(actix_web::error::ErrorInternalServerError(
                "Queue was not created",
            ))
        }
        Ok(_) => Ok(HttpResponse::Ok().json(BaseQueueResponse { success: true })),
    }
}

#[derive(Deserialize, Default)]
struct SequenceQuery {
    sequence: RequestSequence,
}

async fn send_to_queue(
    srv: web::Data<Queue<EventMessage>>,
    info: web::Path<String>,
    message: web::Json<EventMessage>,
) -> impl Responder {
    let queue_name = info.into_inner();
    let message = message.into_inner();
    match srv.send_to_queue(queue_name, message) {
        Err(e) => {
            error!("sending message error {}", e);
            Err(actix_web::error::ErrorInternalServerError(
                "Message was not sent",
            ))
        }
        Ok(success) => Ok(HttpResponse::Ok().json(BaseQueueResponse { success })),
    }
}

async fn close_queue(
    srv: web::Data<Queue<EventMessage>>,
    info: web::Path<String>,
) -> impl Responder {
    let queue_name = info.into_inner();
    match srv.close_queue(queue_name) {
        Ok(success) => Ok(HttpResponse::Ok().json(BaseQueueResponse { success })),
        Err(e) => {
            error!("close queue error {}", e);
            Err(actix_web::error::ErrorInternalServerError(
                "Queue was not closed",
            ))
        }
    }
}

#[actix_web::main]
async fn main() -> tokio::io::Result<()> {
    let config = get_config();

    let address = config
        .addr
        .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 8080));
    let secure = config.secure;
    let queue_options = config.queue;

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
                    prefix,
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

    let queue = web::Data::new(Queue::<EventMessage>::new(queue_options).unwrap());

    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(queue.clone())
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
