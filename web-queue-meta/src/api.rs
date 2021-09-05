use crate::config::Secure;
use actix_web::dev::HttpServiceFactory;
use actix_web::guard::Guard;
use actix_web::http::HeaderValue;
use actix_web::rt::time::sleep;
use actix_web::web::Data;
use actix_web::{web, HttpResponse};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::{Duration, SystemTime};

const BEARER: &str = "Bearer ";

#[macro_export]
macro_rules! queue_scope_factory {
    (   $create_queue:ident,
        $send_to_queue:ident,
        $close_queue:ident,
        $subscribe_queue_by_id_ws:ident,
        $subscribe_queue_by_id_longpoll:ident,
        $subscribe_queue_ws:ident,
        $subscribe_queue_longpoll:ident,
        $secure:expr,
    ) => {
        match $secure {
            None => web::scope("/queue")
                .route("/create/{queue_name}", web::post().to($create_queue))
                .route("/send/{queue_name}", web::post().to($send_to_queue))
                .route("/close/{queue_name}", web::post().to($close_queue))
                .service(
                    web::scope("/listen")
                        .route(
                            "/longpoll/{queue_name}",
                            web::get().to($subscribe_queue_longpoll),
                        )
                        .service(web::resource("/ws/{queue_name}").to($subscribe_queue_ws))
                        .route(
                            "/longpoll/{queue_name}/{uniq_id}",
                            web::get().to($subscribe_queue_by_id_longpoll),
                        )
                        .service(
                            web::resource("/ws/{queue_name}/{uniq_id}")
                                .to($subscribe_queue_by_id_ws),
                        ),
                ),
            Some(st) => web::scope("/queue")
                .route(
                    "/create/{queue_name}",
                    web::post()
                        .guard($crate::api::service_token_guard(st))
                        .to($create_queue),
                )
                .route(
                    "/send/{queue_name}",
                    web::post()
                        .guard($crate::api::service_token_guard(st))
                        .to($send_to_queue),
                )
                .route(
                    "/close/{queue_name}",
                    web::post()
                        .guard($crate::api::service_token_guard(st))
                        .to($close_queue),
                )
                .service(
                    web::scope("/listen")
                        .route(
                            "/longpoll/{queue_name}",
                            web::get()
                                .guard($crate::api::service_token_guard(st))
                                .to($subscribe_queue_longpoll),
                        )
                        .service(
                            web::resource("/ws/{queue_name}")
                                .guard($crate::api::service_token_guard(st))
                                .to($subscribe_queue_ws),
                        )
                        .route(
                            "/longpoll/{queue_name}/{uniq_id}",
                            web::get()
                                .guard($crate::api::jwt_token_guard(st.clone()))
                                .to($subscribe_queue_by_id_longpoll),
                        )
                        .service(
                            web::resource("/ws/{queue_name}/{uniq_id}")
                                .guard($crate::api::jwt_token_guard(st.clone()))
                                .to($subscribe_queue_by_id_ws),
                        ),
                ),
        }
    };
}

pub fn service_token_guard(secure: &Secure) -> impl Guard {
    let service_token_head_value =
        HeaderValue::from_str(&format!("{}{}", BEARER, secure.service_token))
            .expect("invalid header value");
    actix_web::guard::fn_guard(move |head| {
        head.headers
            .get("Authorization")
            .filter(|token| *token == service_token_head_value)
            .is_some()
    })
}

pub fn jwt_token_guard(secure: Secure) -> impl Guard {
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
                    &DecodingKey::from_secret(secure.service_token.as_bytes()),
                    &Validation::default(),
                )
                .ok()
                .filter(|c| {
                    head.uri
                        .path()
                        .ends_with(&format!("/{}/{}", c.claims.iss, c.claims.sub))
                })
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

pub fn generate_jwt_method_factory(secure: Secure) -> impl HttpServiceFactory {
    web::resource("/generate_jwt/{queue}/{uniq_id}")
        .guard(service_token_guard(&secure))
        .app_data(Data::new(secure))
        .route(web::post().to(
            move |secure: web::Data<Secure>, info: web::Path<(String, String)>| async move {
                let (queue_name, id) = info.into_inner();
                let expiration_res = SystemTime::now()
                    .checked_add(Duration::from_secs(secure.jwt_token_expiration))
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
                    &EncodingKey::from_secret(secure.service_token.as_bytes()),
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

pub const MAX_RECONNECT_ATTEMPTS: u8 = 10;

/// Calculate sleep time with formula `seconds = 1.5 * sqrt(attempts)`
/// To getting increasing time intervals between reconnections.
pub fn sleep_between_reconnects(attempt: u8) -> impl Future<Output = ()> {
    sleep(Duration::from_secs((1.5 * (attempt as f32)).sqrt() as u64))
}
