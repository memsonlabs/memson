use crate::db::Db;
use crate::cmd::Cmd;
use crate::inmemdb::InMemDb;
use crate::err::Error;
use crate::json::Json;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder};
use clap::{Arg, App as Clap};
use serde::Serialize;
use std::sync::{Arc, RwLock};
use std::env;

mod aggregate;
mod cmd;
mod db;
mod err;
mod inmemdb;
mod json;
mod ondiskdb;
mod query;

type MemsonDB = Arc<RwLock<InMemDb>>;

fn http_resp<T: Serialize>(r: Result<T, Error>) -> HttpResponse {
    match r {
        Ok(val) => HttpResponse::Ok().json(val),
        Err(err) => HttpResponse::BadRequest().json(err.to_string()),
    }
}

async fn summary(db: web::Data<MemsonDB>) -> impl Responder {
    let mut db = db.write().unwrap();
    let summary = db.eval(Cmd::Summary).unwrap();
    HttpResponse::Ok().json(summary)
}

async fn eval(json: web::Json<Json>, db: web::Data<MemsonDB>) -> impl Responder {
    println!("{:?}", json);
    let cmd = match Cmd::parse(json.0) {
        Ok(cmd) => cmd,
        Err(err) => return HttpResponse::BadRequest().json(err.to_string()),        
    };
    let mut db = db.write().unwrap();
    let r = db.eval(cmd);
    println!("r={:?}", r);
    http_resp(r)
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let matches = Clap::new("memson")
        .version("0.1")
        .author("memson.io <hello@memson.io>")
        .about("in-memory json data store")
        .arg(Arg::with_name("host")
            .short("h")
            .long("host")
            .value_name("0.0.0.0")
            .help("Sets the host to use")
            .takes_value(true))

        .arg(Arg::with_name("port")
            .short("")
            .long("port")
            .help("Sets the port to use"))
        .arg(Arg::with_name("dbpath")
            .short("db")
            .value_name("PATH")
            .help("Sets the path for memson to store data"))
        .get_matches();
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let host = matches.value_of("host").unwrap_or("0.0.0.0").to_string();
    let port = env::var("PORT").unwrap_or_else(|_| "8088".to_string());

    let addr = host + ":" + &port;
    println!("deploying on {:?}", addr);
    let db = InMemDb::new();
    let db: MemsonDB = Arc::new(RwLock::new(db));
    HttpServer::new(move || {
        App::new()
            //enable logger
            .wrap(middleware::Logger::default())
            .data(db.clone())
            .service(web::resource("/cmd").route(web::post().to(eval)))
            .service(web::resource("/").route(web::get().to(summary)))
    })
    .bind(addr)?
    .run()
    .await
}
