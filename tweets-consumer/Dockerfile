FROM rust as build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:buster-slim
RUN apt-get update && apt-get install -y openssl && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/redis-streams-producer /usr/local/bin
CMD ["redis-streams-producer"]