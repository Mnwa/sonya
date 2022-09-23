FROM rust:1-slim as planner
WORKDIR /usr/src/web-queue
RUN cargo install cargo-chef
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM rust:1-slim as cacher
WORKDIR /usr/src/web-queue
RUN apt update -y && apt -y install pkg-config protobuf-compiler
RUN cargo install cargo-chef
COPY --from=planner /usr/src/web-queue/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

FROM rust:1-slim as builder
WORKDIR /usr/src/web-queue
RUN apt update -y && apt -y install pkg-config protobuf-compiler
COPY . .
COPY --from=cacher /usr/src/web-queue/target target
COPY --from=cacher $CARGO_HOME $CARGO_HOME
RUN cargo build --release --bin sonya

FROM debian:buster-slim
COPY --from=builder /usr/src/web-queue/target/release/sonya /usr/local/bin/sonya
CMD ["sonya"]