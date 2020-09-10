use crate::cmd::Cmd;
use crate::err::Error;
use crate::inmemdb::InMemDb;
use actix_web::{middleware, web, App, HttpResponse, HttpServer};
use serde::Serialize;
use serde_json::json;
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
        Err(err) => http_err(err.to_string()),
    }
}

fn http_err<T:Serialize>(err: T) -> HttpResponse {
    HttpResponse::InternalServerError().json(err)
}

async fn summary(db: web::Data<Memson>) -> HttpResponse {
    let mut db = match db.write() {
        Ok(db) => db,
        Err(_) => return HttpResponse::InternalServerError().into(),
    };
    let res = db.eval(Cmd::Summary);
    http_resp(res)
}

async fn eval(db: web::Data<Memson>, payload: web::Json<serde_json::Value>) -> HttpResponse {
    // Send message to `DbExecutor` actor
    let cmd = match Cmd::parse(payload.0) {
        Ok(cmd) => cmd,
        Err(_) => return http_err("bad json"),
    };
    let res =  if cmd.is_read() {
        let db = match db.read() {
            Ok(db) => db,
            Err(err) => return http_err(err.to_string()),
        };
        db.eval_read(cmd)
    } else {
        return http_err("cannot mutate data in demo");
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

    let mut db = InMemDb::new();
    db.set("orders", json!([
        { "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
        { "customer": "ania", "qty": 2, "price": 2.0 },
        { "customer": "misha", "qty": 4, "price": 1.0 },
        { "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
        { "customer": "james", "qty": 1, "price": 16.0 },
    ]));
    db.set("x", json!(1));
    db.set("y", json!(2));
    db.set("james", json!({ "name": "james perry", "age": 30 }));    

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
