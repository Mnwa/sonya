FROM rust:1-slim as planner
WORKDIR /usr/src/web-queue
RUN cargo install cargo-chef
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM rust:1-slim as cacher
WORKDIR /usr/src/web-queue
RUN cargo install cargo-chef
COPY --from=planner /usr/src/web-queue/recipe.json recipe.json
RUN apt update -y && apt -y install pkg-config libssl-dev clang protobuf-compiler
RUN cargo chef cook --release --recipe-path recipe.json

FROM rust:1-slim as builder
WORKDIR /usr/src/web-queue
COPY . .
COPY --from=cacher /usr/src/web-queue/target target
COPY --from=cacher $CARGO_HOME $CARGO_HOME
RUN apt update -y && apt -y install pkg-config libssl-dev clang protobuf-compiler
RUN cargo build --release --bin sonya-proxy

FROM debian:buster-slim
COPY --from=builder /usr/src/web-queue/target/release/sonya-proxy /usr/local/bin/sonya-proxy
RUN apt update -y && apt -y install pkg-config libssl-dev
CMD ["sonya-proxy"]