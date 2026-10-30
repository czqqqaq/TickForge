FROM rust:1.95.0-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /app/target/release/yino /usr/local/bin/yino
WORKDIR /data
ENTRYPOINT ["yino"]
CMD ["--help"]
