use crate::cmd::Cmd;
use crate::err::Error;
use crate::inmemdb::InMemDb;
use actix_web::{
    middleware, web, App, HttpResponse, HttpServer,
};
use clap::{App as Clap, Arg};
use serde::Serialize;
use std::env;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

mod aggregate;
mod cmd;
mod err;
mod eval;
mod inmemdb;
mod json;
mod keyed;
mod query;

/// This is state where we will store *DbExecutor* address.
type Memson = Arc<RwLock<InMemDb>>;

fn http_resp<T: Debug + Serialize>(r: Result<T, Error>) -> HttpResponse {
    println!("r={:?}", r);
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
    println!("eval({:?})", cmd.0);
    let mut db = match db.write() {
        Ok(db) => db,
        Err(_) => return HttpResponse::InternalServerError().into(),
    };
    let res = db.eval(cmd.0);
    http_resp(res)
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let matches = Clap::new("memson")
        .version("0.1")
        .author("memson.io <hello@memson.io>")
        .about("in-memory json data store")
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("0.0.0.0")
                .help("Sets the host to use")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("")
                .long("port")
                .help("Sets the port to use"),
        )
        .arg(
            Arg::with_name("dbpath")
                .short("db")
                .value_name("PATH")
                .help("Sets the path for memson to store data"),
        )
        .get_matches();
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    let host = matches.value_of("host").unwrap_or("0.0.0.0").to_string();
    let port = env::var("PORT").unwrap_or_else(|_| "8088".to_string());

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
