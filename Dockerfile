from rust:1.81 as build
copy ./ ./
run cargo build --release

from debian:stable-slim
copy --from=build ./target/release/log-server .
cmd ["./log-server"]
