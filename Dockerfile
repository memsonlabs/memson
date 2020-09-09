FROM rust:alpine

RUN apk add libc-dev

WORKDIR /usr/src/memson
COPY Cargo.toml .
COPY Cargo.lock .
COPY src src

EXPOSE 8686

RUN cargo install --path .

CMD ["memson"]