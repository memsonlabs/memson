mod codec;
mod db;
mod json;
mod server;
mod session;

use db::{Db, Reply};
use server::MemsonServer;
use codec::MemsonCodec;
use session::MemsonSession;
use std::io;
use std::io::Write;
use std::net;
use std::str::FromStr;

use actix::prelude::*;
use futures_util::stream::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;

fn start_local_db() {
    let mut db = Db::new();
    let mut input = String::new();
    db.eval("{\"set\":[\"a\",[1,2,3,4,5]]}").unwrap();
    db.eval("{\"set\": [\"b\", {\"name\":\"james\"}]}").unwrap();
    loop {        
        print!("> ");
        io::stdout().flush().unwrap();
        if let Err(err) = io::stdin().read_line(&mut input) {
            println!("error: {}", err);
            continue;
        }
        if input.is_empty() {
            continue;
        }
        match db.eval(&input) {
            Ok(Reply::Ref(val)) => println!("{}", val),
            Ok(Reply::Val(val)) => println!("{}", val),
            Ok(Reply::Insert(n)) => println!("inserted {} entry", n),
            Ok(Reply::Update(_)) => println!("updated 1 entry"),
            Err(err) => println!("error: {:?}", err),
        };    
        input.clear();
    }    
}

/// Define tcp server that will accept incoming tcp connection and create
/// chat actors.
struct Server {
    db: Addr<MemsonServer>,
}

/// Make actor from `Server`
impl Actor for Server {
    /// Every actor has to provide execution `Context` in which it can run.
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "()")]
struct TcpConnect(pub TcpStream, pub net::SocketAddr);

/// Handle stream of TcpStream's
impl Handler<TcpConnect> for Server {
    /// this is response for message, which is defined by `ResponseType` trait
    /// in this case we just return unit.
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, _: &mut Context<Self>) {
        // For each incoming connection we create `ChatSession` actor
        // with out chat server address.
        let server = self.db.clone();
        MemsonSession::create(move |ctx| {
            let (r, w) = tokio::io::split(msg.0);
            MemsonSession::add_stream(FramedRead::new(r, MemsonCodec), ctx);
            MemsonSession::new(server, actix::io::FramedWrite::new(w, MemsonCodec, ctx))
        });
    }
}

#[actix_rt::main]
async fn main() {
    // Start Memson server actor
    let server = MemsonServer::default().start();

    // Create server listener
    let addr = net::SocketAddr::from_str("127.0.0.1:8888").unwrap();
    let listener = Box::new(TcpListener::bind(&addr).await.unwrap());

    // Our Memson server `Server` is an actor, first we need to start it
    // and then add stream on incoming tcp connections to it.
    // TcpListener::incoming() returns stream of the (TcpStream, net::SocketAddr)
    // items So to be able to handle this events `Server` actor has to implement
    // stream handler `StreamHandler<(TcpStream, net::SocketAddr), io::Error>`
    Server::create(move |ctx| {
        ctx.add_message_stream(Box::leak(listener).incoming().map(|st| {
            let st = st.unwrap();
            let addr = st.peer_addr().unwrap();
            TcpConnect(st, addr)
        }));
        Server { db: server }
    });

    println!("Running Memson server on 127.0.0.1:8888");

    tokio::signal::ctrl_c().await.unwrap();
    println!("Ctrl-C received, shutting down");
    System::current().stop();
}