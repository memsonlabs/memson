use crate::cmd::Cmd;
use crate::err::Error;
use crate::inmemdb::InMemDb;
use actix_web::{middleware, web, App, HttpResponse, HttpServer};
use serde::Serialize;
use std::env;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

mod cmd;
mod err;
mod eval;
mod inmemdb;
mod json;
mod keyed;

/// This is state where we will store *DbExecutor* address.
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

async fn eval(db: web::Data<Memson>, cmd: web::Json<Cmd>) -> HttpResponse {
    // Send message to `DbExecutor` actor
    let res =  if cmd.is_read() {
        let db = match db.read() {
            Ok(db) => db,
            Err(_) => return HttpResponse::InternalServerError().into(),
        };

        db.eval_read(&cmd.0)
    } else {
        let mut db = match db.write() {
            Ok(db) => db,
            Err(_) => return HttpResponse::InternalServerError().into(),
        };
        db.eval(cmd.0)
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
    println!("deploying on {:?}", addr);

    let db = InMemDb::new();
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
