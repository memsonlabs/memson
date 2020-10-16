use actix::prelude::*;
use actix_web::{middleware, web, App, HttpResponse, HttpServer};
use memson::cmd::{Cmd, QueryCmd};
use memson::db::InMemDb;
use memson::json::Json;
use memson::Error;
use serde::Serialize;
use serde_json::json;
use std::env;
use std::fmt::Debug;

/// Define message
#[derive(Message)]
#[rtype(result = "Result<Json, Error>")]
enum Request {
    Command(Cmd),
    Query(QueryCmd),
}

// Define actor
struct DbActor {
    db: InMemDb,
}

impl Actor for DbActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        println!("Actor is alive");
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        println!("Actor is stopped");
    }
}

/// Define handler for `Ping` message
impl Handler<Request> for DbActor {
    type Result = Result<Json, Error>;

    fn handle(&mut self, req: Request, _: &mut Context<Self>) -> Self::Result {
        match req {
            Request::Command(cmd) => self.db.eval(cmd),
            Request::Query(qry) => self.db.query(qry),
        }
    }
}

fn http_resp<T: Debug + Serialize>(r: Result<Result<T, Error>, MailboxError>) -> HttpResponse {
    match r {
        Ok(Ok(val)) => HttpResponse::Ok().json(val),
        Ok(Err(err)) => HttpResponse::Ok().json(err.to_string()),
        Err(_) => HttpResponse::InternalServerError().into(),
    }
}

async fn summary(tx: web::Data<Addr<DbActor>>) -> HttpResponse {
    let res = tx.send(Request::Command(Cmd::Summary)).await;
    http_resp(res)
}

async fn eval2(db: web::Data<Addr<DbActor>>, cmd: web::Json<Json>) -> HttpResponse {
    let cmd = match Cmd::parse(cmd.0) {
        Ok(cmd) => cmd,
        Err(err) => return HttpResponse::InternalServerError().json(err.to_string()),
    };
    // Send message to `DbExecutor` actor
    let r = db.send(Request::Command(cmd)).await;
    http_resp(r)
}

async fn query2(db: web::Data<Addr<DbActor>>, cmd: web::Json<QueryCmd>) -> HttpResponse {
    // Send message to `DbExecutor` actor
    let r = db.send(Request::Query(cmd.0)).await;
    http_resp(r)
}

fn orders(n: usize) -> Json {
    Json::Array((0..n).map(|i| json!({"id": i, "qty": i % 100})).collect())
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");

    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8686".to_string());

    let addr = host + ":" + &port;
    println!("memson is starting on {}", addr);

    let mut db = InMemDb::new();
    db.set("orders", orders(4_000_000));

    let actor = DbActor { db };
    let actor_addr = actor.start();
    //let memson = Arc::new(RwLock::new(db));
    HttpServer::new(move || {
        App::new()
            //enable logger
            .wrap(middleware::Logger::default())
            .data(actor_addr.clone())
            .service(web::resource("/cmd").route(web::post().to(eval2)))
            .service(web::resource("/query").route(web::post().to(query2)))
            .service(web::resource("/").route(web::get().to(summary)))
    })
    .bind(addr.clone())?
    .run()
    .await
}
