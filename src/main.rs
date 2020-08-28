use crate::cmd::Cmd;
use crate::db::Memson;
use crate::err::Error;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder};
use clap::{Arg, App as Clap};
use serde::Serialize;
use std::sync::{Arc, RwLock};
use std::env;

mod cmd;
mod db;
mod err;
mod inmemdb;
mod json;
mod ondiskdb;
mod query;

type MemsonServer = Arc<RwLock<Memson>>;

fn http_resp<T: Serialize>(r: Result<T, Error>) -> HttpResponse {
    match r {
        Ok(val) => HttpResponse::Ok().json(val),
        Err(err) => HttpResponse::Ok().json(err.to_string()),
    }
}

fn eval(db: web::Data<MemsonServer>, cmd: Cmd) -> HttpResponse {
    let mut db = db.write().unwrap();
    http_resp(db.eval(cmd))
}

async fn summary(db: web::Data<MemsonServer>) -> impl Responder {
    let db = db.read().unwrap();
    let summary = db.summary();
    HttpResponse::Ok().json(summary)
}

async fn query(cmd: web::Json<Cmd>, db: web::Data<MemsonServer>) -> impl Responder {
    println!("{:?}", &cmd);
    eval(db, cmd.0)
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

    let db_path = matches.value_of("dbpath").unwrap_or(".");

    let host = matches.value_of("host").unwrap_or("0.0.0.0").to_string();
    let port = env::var("PORT").unwrap_or_else(|_| "8088".to_string());

    println!("db_path={:?}\nhost={:?}\nport={:?}", db_path, host, port);
    let addr = host + ":" + &port;
    println!("addr={:?}", addr);
    let db = match Memson::open(db_path) {

        Ok(db) => db,
        Err(err) => {
            eprintln!("{:?}", err);
            panic!("cannot open memson");
        }
    };
    let db = Arc::new(RwLock::new(db));
    HttpServer::new(move || {
        App::new()
            //enable logger
            .wrap(middleware::Logger::default())
            .data(db.clone())
            .service(web::resource("/cmd").route(web::post().to(query)))
            .service(web::resource("/").route(web::get().to(summary)))
    })
    .bind(addr)?
    .run()
    .await
}
