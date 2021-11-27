#![forbid(unsafe_code)]

pub mod apply;
pub mod cmd;
pub mod db;
pub mod err;
pub mod eval;
pub mod inmem;
pub mod json;
pub mod ondisk;
pub mod query;

use crate::cmd::Cmd;
use crate::db::Memson;
use crate::err::Error;

use actix_web::web::Json;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use std::sync::Arc;
use std::sync::RwLock;

pub const DEFAULT_PORT: &str = "8888";

// This struct represents state
#[derive(Clone)]
struct AppState {
    db: Arc<RwLock<Memson>>,
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[get("/pulse")]
async fn pulse() -> impl Responder {
    HttpResponse::Ok()
}

#[post("/query")]
async fn web_query(data: web::Data<AppState>, req_body: Json<Cmd>) -> impl Responder {
    let cmd = req_body.into_inner();
    let mut db = match data.db.write() {
        Ok(db) => db,
        Err(_) => return Err(Error::BadIO),
    };
    match db.eval(cmd) {
        Ok(val) => Ok(Json(val)),
        Err(err) => return Err(err),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let memson = Arc::new(RwLock::new(Memson::open("data").unwrap()));
    HttpServer::new(move || {
        App::new()
            .data(AppState { db: memson.clone() })
            .service(hello)
            .service(web_query)
            .service(pulse)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
