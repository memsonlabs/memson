use std::str::FromStr;
use std::time::Duration;
use std::{io, net, thread};

use futures_util::future::FutureExt;

use actix::prelude::*;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio_util::codec::FramedRead;

mod codec;

#[actix_rt::main]
async fn main() {
    println!("Running Memson client");

    // Connect to server
    let addr = net::SocketAddr::from_str("127.0.0.1:8888").unwrap();
    Arbiter::spawn(TcpStream::connect(addr).then(|stream| {
        let stream = stream.unwrap();
        let addr = MemsonClient::create(|ctx| {
            let (r, w) = tokio::io::split(stream);
            ctx.add_stream(FramedRead::new(r, codec::ClientMemsonCodec));
            MemsonClient {
                framed: actix::io::FramedWrite::new(w, codec::ClientMemsonCodec, ctx),
            }
        });

        // start console loop
        thread::spawn(move || loop {
            let mut cmd = String::new();
            if io::stdin().read_line(&mut cmd).is_err() {
                println!("error");
                return;
            }
            println!("sending cmd={}", cmd);
            let r = addr.do_send(ClientCommand(cmd));
            println!("Result = {:?}", r.unwrap());
        });

        async {}
    }));

    tokio::signal::ctrl_c().await.unwrap();
    println!("Ctrl-C received, shutting down");
    System::current().stop();
}

struct MemsonClient {
    framed: actix::io::FramedWrite<
        codec::MemsonRequest,
        WriteHalf<TcpStream>,
        codec::ClientMemsonCodec,
    >,
}

#[derive(Message)]
#[rtype(result = "String")]
struct ClientCommand(String);

impl Actor for MemsonClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // start heartbeats otherwise server will disconnect after 10 seconds
        self.hb(ctx)
    }

    fn stopping(&mut self, _: &mut Context<Self>) -> Running {
        println!("Disconnected");

        // Stop application on disconnect
        System::current().stop();

        Running::Stop
    }
}

impl MemsonClient {
    fn hb(&self, ctx: &mut Context<Self>) {
        ctx.run_later(Duration::new(1, 0), |act, ctx| {
            act.framed.write(codec::MemsonRequest::Ping);
            act.hb(ctx);
        });
    }
}

impl actix::io::WriteHandler<io::Error> for MemsonClient {}

/// Handle stdin commands
impl Handler<ClientCommand> for MemsonClient {
    type Result = ();

    fn handle(&mut self, msg: ClientCommand, _: &mut Context<Self>) {
        let m = msg.0.trim();

        // we check for /sss type of messages
        self.framed.write(codec::MemsonRequest::Command(m.to_owned()));
    }
}

/// Server communication
impl StreamHandler<Result<codec::MemsonResponse, io::Error>> for MemsonClient {
    fn handle(
        &mut self,
        msg: Result<codec::MemsonResponse, io::Error>,
        _: &mut Context<Self>,
    ) {
        match msg {
            Ok(codec::MemsonResponse::Data(ref msg)) => {
                println!("data: {}", msg);
            }
            _ => (),
        }
    }
}