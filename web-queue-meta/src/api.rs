use actix_web::dev::Factory;
use actix_web::{web, FromRequest, Responder, Scope};
use std::future::Future;

pub fn queue_scope_factory<
    CREATE: Factory<ICR, RCR, UCR> + 'static,
    ICR: FromRequest + 'static,
    RCR: Future<Output = UCR> + 'static,
    UCR: Responder + 'static,
    SEND: Factory<IS, RS, US> + 'static,
    IS: FromRequest + 'static,
    RS: Future<Output = US> + 'static,
    US: Responder + 'static,
    CLOSE: Factory<ICL, RCL, UCL> + 'static,
    ICL: FromRequest + 'static,
    RCL: Future<Output = UCL> + 'static,
    UCL: Responder + 'static,
    LONGPOLL: Factory<IL, RL, UL> + 'static,
    IL: FromRequest + 'static,
    RL: Future<Output = UL> + 'static,
    UL: Responder + 'static,
    WEBSOCKET: Factory<IW, RW, UW> + 'static,
    IW: FromRequest + 'static,
    RW: Future<Output = UW> + 'static,
    UW: Responder + 'static,
>(
    create_queue: CREATE,
    send_to_queue: SEND,
    close_queue: CLOSE,
    subscribe_queue_longpoll: LONGPOLL,
    subscribe_queue_ws: WEBSOCKET,
) -> Scope {
    web::scope("/queue")
        .route("/create/{queue_name}", web::post().to(create_queue))
        .route("/send/{queue_name}", web::post().to(send_to_queue))
        .route("/close/{queue_name}", web::post().to(close_queue))
        .service(
            web::scope("/listen")
                .route(
                    "/longpoll/{queue_name}/{uniq_id}",
                    web::get().to(subscribe_queue_longpoll),
                )
                .service(web::resource("/ws/{queue_name}/{uniq_id}").to(subscribe_queue_ws)),
        )
}
