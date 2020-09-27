# Build Stage
FROM rust:1.46.0 AS builder
WORKDIR /usr/src/

RUN USER=root cargo new memson
WORKDIR /usr/src/memson
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

COPY src ./src
RUN cargo install --path .

# Bundle Stage
FROM scratch
COPY --from=builder /usr/local/cargo/bin/memson .
CMD ["./memson"]