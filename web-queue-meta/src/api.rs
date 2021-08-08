use actix_web::dev::HttpServiceFactory;
use actix_web::guard::Guard;
use actix_web::http::HeaderValue;
use actix_web::{web, HttpResponse};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

const BEARER: &'static str = "Bearer ";

#[macro_export]
macro_rules! queue_scope_factory {
    (   $create_queue:ident,
        $send_to_queue:ident,
        $close_queue:ident,
        $subscribe_queue_longpoll:ident,
        $subscribe_queue_ws:ident,
        $service_token:expr
    ) => {{
        match $service_token {
            None => web::scope("/queue")
                .route("/create/{queue_name}", web::post().to($create_queue))
                .route("/send/{queue_name}", web::post().to($send_to_queue))
                .route("/close/{queue_name}", web::post().to($close_queue))
                .service(
                    web::scope("/listen")
                        .route(
                            "/longpoll/{queue_name}/{uniq_id}",
                            web::get().to($subscribe_queue_longpoll),
                        )
                        .service(
                            web::resource("/ws/{queue_name}/{uniq_id}").to($subscribe_queue_ws),
                        ),
                ),
            Some(st) => web::scope("/queue")
                .service(
                    web::scope("")
                        .guard($crate::api::service_token_guard(st.clone()))
                        .route("/create/{queue_name}", web::post().to($create_queue))
                        .route("/send/{queue_name}", web::post().to($send_to_queue))
                        .route("/close/{queue_name}", web::post().to($close_queue))
                        .service($crate::api::generate_jwt_method_factory(st.clone())),
                )
                .service(
                    web::scope("/listen")
                        .guard($crate::api::jwt_token_guard(st.clone()))
                        .route(
                            "/longpoll/{queue_name}/{uniq_id}",
                            web::get().to($subscribe_queue_longpoll),
                        )
                        .service(
                            web::resource("/ws/{queue_name}/{uniq_id}").to($subscribe_queue_ws),
                        ),
                ),
        }
    }};
}

pub fn service_token_guard(service_token: String) -> impl Guard {
    let service_token_head_value = HeaderValue::from_str(&format!("{}{}", BEARER, service_token))
        .expect("invalid header value");
    actix_web::guard::fn_guard(move |head| {
        head.headers
            .get("Authorization")
            .filter(|token| *token == service_token_head_value)
            .is_some()
    })
}

pub fn jwt_token_guard(service_token: String) -> impl Guard {
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

pub fn generate_jwt_method_factory(service_token: String) -> impl HttpServiceFactory {
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
