//! A "tiny database" and accompanying protocol
//!
//! This example shows the usage of shared state amongst all connected clients,
//! namely a database of key/value pairs. Each connected client can send a
//! series of GET/SET commands to query the current value of a key or set the
//! value of a key.
//!
//! This example has a simple protocol you can use to interact with the server.
//! To run, first run this in one terminal window:
//!
//!     cargo run --example tinydb
//!
//! and next in another windows run:
//!
//!     cargo run --example connect 127.0.0.1:8080
//!
//! In the `connect` window you can type in commands where when you hit enter
//! you'll get a response from the server for that command. An example session
//! is:
//!
//!
//!     $ cargo run --example connect 127.0.0.1:8080
//!     GET foo
//!     foo = bar
//!     GET FOOBAR
//!     error: no key FOOBAR
//!     SET FOOBAR my awesome string
//!     set FOOBAR = `my awesome string`, previous: None
//!     SET foo tokio
//!     set foo = `tokio`, previous: Some("bar")
//!     GET foo
//!     foo = tokio
//!
//! Namely you can issue two forms of commands:
//!
//! * `GET $key` - this will fetch the value of `$key` from the database and
//!   return it. The server's database is initially populated with the key `foo`
//!   set to the value `bar`
//! * `SET $key $value` - this will set the value of `$key` to `$value`,
//!   returning the previous value, if any.

use crate::cmd::Cmd;
use crate::db::InMemDb;
use crate::err::Error;
use crate::json::Json;
use actix_web::{middleware, web, App, HttpResponse, HttpServer};
use serde::Serialize;
use serde_json::json;
use std::env;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

mod cmd;
mod db;
mod err;
mod eval;
mod json;

type Memson = Arc<RwLock<InMemDb>>;

fn http_resp<T: Debug + Serialize>(r: Result<T, Error>) -> HttpResponse {
    match r {
        Ok(val) => HttpResponse::Ok().json(val),
        Err(_) => HttpResponse::InternalServerError().into(),
    }
}

async fn summary(db: web::Data<Memson>) -> HttpResponse {
    let mut db = match db.write() {
        Ok(db) => db,
        Err(_) => return HttpResponse::InternalServerError().into(),
    };
    let res = db.eval(Cmd::Summary);
    http_resp(res)
}

async fn eval(db: web::Data<Memson>, cmd: web::Json<Json>) -> HttpResponse {
    let cmd = match Cmd::parse(cmd.0) {
        Ok(cmd) => cmd,
        Err(err) => return HttpResponse::InternalServerError().json(err.to_string()),
    };
    // Send message to `DbExecutor` actor
    let res = if cmd.is_read() {
        let db = match db.read() {
            Ok(db) => db,
            Err(_) => return HttpResponse::InternalServerError().into(),
        };
        db.eval_read(cmd)
    } else {
        let mut db = match db.write() {
            Ok(db) => db,
            Err(_) => return HttpResponse::InternalServerError().into(),
        };
        db.eval(cmd)
    };
    http_resp(res)
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8686".to_string());

    let addr = host + ":" + &port;
    println!("memson is starting on {}", addr);

    let mut db = InMemDb::new();
    db.set(
        "orders".to_string() + &i.to_string(),
        json!([
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]),
    );

    let db_data = Arc::new(RwLock::new(db));
    HttpServer::new(move || {
        App::new()
            //enable logger
            .wrap(middleware::Logger::default())
            .data(db_data.clone())
            .service(web::resource("/cmd").route(web::post().to(eval)))
            .service(web::resource("/").route(web::get().to(summary)))
    })
    .bind(addr)?
    .run()
    .await
}
