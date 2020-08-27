use crate::cmd::{Cmd, QueryCmd};
use crate::db::Memson;
use crate::err::Error;
use actix_web::{middleware, web, App, HttpResponse, HttpServer, Responder};
use clap::{Arg, App as Clap};
use serde::Serialize;
use serde_json::Value as Json;
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

fn http_opt_resp<T: Serialize>(r: Result<Option<T>, Error>) -> HttpResponse {
    match r {
        Ok(Some(val)) => HttpResponse::Ok().json(val),
        Ok(None) => HttpResponse::Ok().json(Json::Null),
        Err(err) => HttpResponse::Ok().json(err.to_string()),
    }
}

fn eval(db: web::Data<MemsonServer>, cmd: Cmd) -> HttpResponse {
    match cmd {
        Cmd::Add(lhs, rhs) => {
            let db = db.read().unwrap();
            http_resp(db.add(&lhs, &rhs))
        }
        Cmd::Avg(key) => {
            let db = db.read().unwrap();
            http_resp(db.avg(&key))
        }
        Cmd::Append(key, val) => {
            let mut db = db.write().unwrap();
            http_resp(db.append(key, val))
        }
        Cmd::Count(key) => {
            let db = db.read().unwrap();
            http_resp(db.count(&key))
        }
        Cmd::Delete(key) => {
            let mut db = db.write().unwrap();
            http_opt_resp(db.delete(&key))
        }
        Cmd::Dev(key) => {
            let db = db.read().unwrap();
            http_resp(db.dev(&key))
        }
        Cmd::Div(lhs, rhs) => {
            let db = db.read().unwrap();
            http_resp(db.div(&lhs, &rhs))
        }
        Cmd::First(key) => {
            let db = db.read().unwrap();
            http_resp(db.first(&key))
        }
        Cmd::Last(key) => {
            let db = db.read().unwrap();
            http_resp(db.last(&key))
        }
        Cmd::Max(key) => {
            let db = db.read().unwrap();
            http_resp(db.max(&key))
        }
        Cmd::Min(key) => {
            let db = db.read().unwrap();
            http_resp(db.min(&key))
        }
        Cmd::Insert(key, val) => {
            let mut db = db.write().unwrap();
            http_resp(db.insert(key, val))
        }
        Cmd::Var(key) => {
            let db = db.read().unwrap();
            http_resp(db.var(&key))
        }
        Cmd::Get(key) => {
            let db = db.read().unwrap();
            http_resp(db.get(&key))
        }
        Cmd::Keys(page) => {
            let db = db.read().unwrap();
            HttpResponse::Ok().json(db.keys(page))
        }
        Cmd::Mul(lhs, rhs) => {
            let db = db.read().unwrap();
            http_resp(db.mul(&lhs, &rhs))
        }
        Cmd::Pop(key) => {
            let mut db = db.write().unwrap();
            http_resp(db.pop(&key))
        }
        Cmd::Set(key, val) => {
            let mut db = db.write().unwrap();
            http_opt_resp(db.set(key, val))
        }
        Cmd::Sub(lhs, rhs) => {
            let db = db.read().unwrap();
            http_resp(db.sub(&lhs, &rhs))
        }
        Cmd::Sum(key) => {
            let db = db.read().unwrap();
            http_resp(db.sum(&key))
        }
        Cmd::Query(cmd) => {
            let db = db.read().unwrap();
            http_resp(db.query(cmd))
        }
        Cmd::Len => {
            let db = db.read().unwrap();
            HttpResponse::Ok().json(db.len())
        }
    }
}

async fn select(cmd: web::Json<QueryCmd>, db: web::Data<MemsonServer>) -> impl Responder {
    println!("{:?}", &cmd);
    let db = db.read().unwrap();
    http_resp(db.query(cmd.0))
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
            .service(web::resource("/select").route(web::post().to(select)))
    })
    .bind(addr)?
    .run()
    .await
}
