use actix_web::dev::{Factory, HttpServiceFactory};
use actix_web::guard::Guard;
use actix_web::http::HeaderValue;
use actix_web::{web, FromRequest, HttpResponse, Responder, Scope};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::{Duration, SystemTime};

const BEARER: &'static str = "Bearer ";

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
    service_token: Option<String>,
) -> Scope {
    match service_token {
        None => web::scope("/queue")
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
            ),
        Some(st) => web::scope("/queue")
            .service(
                web::scope("")
                    .guard(service_token_guard(st.clone()))
                    .route("/create/{queue_name}", web::post().to(create_queue))
                    .route("/send/{queue_name}", web::post().to(send_to_queue))
                    .route("/close/{queue_name}", web::post().to(close_queue))
                    .service(generate_jwt_method_factory(st.clone())),
            )
            .service(
                web::scope("/listen")
                    .guard(jwt_token_guard(st.clone()))
                    .route(
                        "/longpoll/{queue_name}/{uniq_id}",
                        web::get().to(subscribe_queue_longpoll),
                    )
                    .service(web::resource("/ws/{queue_name}/{uniq_id}").to(subscribe_queue_ws)),
            ),
    }
}

fn service_token_guard(service_token: String) -> impl Guard {
    let service_token_head_value = HeaderValue::from_str(&format!("{}{}", BEARER, service_token))
        .expect("invalid header value");
    actix_web::guard::fn_guard(move |head| {
        head.headers
            .get("Authorization")
            .filter(|token| *token == service_token_head_value)
            .is_some()
    })
}

fn jwt_token_guard(service_token: String) -> impl Guard {
    actix_web::guard::fn_guard(move |head| {
        head.headers
            .get("Authorization")
            .and_then(|token: &HeaderValue| {
                let token = token
                    .to_str()
                    .ok()
                    .filter(|t| t.starts_with(BEARER))?
                    .trim_start_matches(BEARER);
                decode::<Claims>(
                    token,
                    &DecodingKey::from_secret(service_token.as_bytes()),
                    &Validation::default(),
                )
                .ok()
            })
            .is_some()
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
    iss: String,
}

fn generate_jwt_method_factory(service_token: String) -> impl HttpServiceFactory {
    web::resource("/generate_jwt/{queue}/{uniq_id}")
        .data(service_token)
        .route(web::post().to(
            |service_token: web::Data<String>, info: web::Path<(String, String)>| async move {
                let (queue_name, id) = info.into_inner();
                let expiration_res = SystemTime::now()
                    .checked_add(Duration::from_secs(60))
                    .expect("could not to add minute to current time")
                    .duration_since(SystemTime::UNIX_EPOCH);

                let expiration = match expiration_res {
                    Ok(e) => e.as_secs() as usize,
                    Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
                };

                let claims = Claims {
                    sub: id,
                    iss: queue_name,
                    exp: expiration,
                };
                let token = encode(
                    &Header::default(),
                    &claims,
                    &EncodingKey::from_secret(service_token.as_bytes()),
                );
                match token {
                    Ok(token) => {
                        Ok(HttpResponse::Ok().json(JwtTokenResponse { token, expiration }))
                    }
                    Err(e) => Err(actix_web::error::ErrorForbidden(e)),
                }
            },
        ))
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtTokenResponse {
    token: String,
    expiration: usize,
}
