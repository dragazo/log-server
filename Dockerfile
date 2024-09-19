FROM rust:latest as build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:stable-slim
WORKDIR /app
COPY --from=build /app/target/release/log-server .
ENTRYPOINT [ "./log-server" ]
