use actix_web::dev::{Factory, HttpServiceFactory};
use actix_web::{web, FromRequest, Responder, Scope};
use std::future::Future;

pub fn queue_scope_factory<CREATE, SEND, CLOSE, LONGPOLL, WEBSOCKET, I, R, U>(
    create_queue: CREATE,
    send_to_queue: SEND,
    close_queue: CLOSE,
    subscribe_queue_longpoll: LONGPOLL,
    subscribe_queue_ws: WEBSOCKET,
) -> Scope
where
    CREATE: HttpServiceFactory + 'static,
    SEND: HttpServiceFactory + 'static,
    CLOSE: HttpServiceFactory + 'static,
    LONGPOLL: HttpServiceFactory + 'static,
    WEBSOCKET: Factory<I, R, U> + 'static,
    I: FromRequest + 'static,
    R: Future<Output = U> + 'static,
    U: Responder + 'static,
{
    web::scope("/queue")
        .service(create_queue)
        .service(send_to_queue)
        .service(close_queue)
        .service(subscribe_queue_longpoll)
        .service(web::resource("/listen/ws/{queue_name}/{uniq_id}").to(subscribe_queue_ws))
}
