//! `ClientSession` is an actor, it manages peer tcp connection and
//! proxies commands from peer to `MemsonServer`.
use std::io;

use actix::clock::{Duration, Instant};
use actix::prelude::*;

use tokio::io::WriteHalf;
use tokio::net::TcpStream;

use crate::codec::{MemsonCodec, MemsonRequest, MemsonResponse};
use crate::server::{self, MemsonServer};

/// Memson server sends this messages to session
#[derive(Message)]
#[rtype(result = "()")]
pub struct Message(pub String);

/// `MemsonSession` actor is responsible for tcp peer communications.
pub struct MemsonSession {
    /// unique session id
    id: usize,
    /// this is address of Memson server
    addr: Addr<MemsonServer>,
    /// Client must send ping at least once per 10 seconds, otherwise we drop
    /// connection.
    hb: Instant,
    /// Framed wrapper
    framed: actix::io::FramedWrite<MemsonResponse, WriteHalf<TcpStream>, MemsonCodec>,
}

impl Actor for MemsonSession {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // we'll start heartbeat process on session start.
        self.hb(ctx);

        // register self in Memson server. `AsyncContext::wait` register
        // future within context, but context waits until this future resolves
        // before processing any other events.
        self.addr
            .send(server::Connect {
                addr: ctx.address(),
            })
            .into_actor(self)
            .then(move |res, act, _| {
                act.id = res.unwrap();
                async {}.into_actor(act)
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        // notify Memson server
        self.addr.do_send(server::Disconnect { id: self.id });
        Running::Stop
    }
}

impl actix::io::WriteHandler<io::Error> for MemsonSession {}

/// To use `Framed` with an actor, we have to implement `StreamHandler` trait
impl StreamHandler<Result<MemsonRequest, io::Error>> for MemsonSession {
    /// This is main event loop for client requests
    fn handle(&mut self, msg: Result<MemsonRequest, io::Error>, ctx: &mut Self::Context) {
        match msg {
            Ok(MemsonRequest::Command(cmd)) => {
                // send message to Memson server
                println!("Peer cmd: {}", cmd);
                self.addr.do_send(server::Query{
                    id: self.id,
                    cmd,
                })
            }
            // we update heartbeat time on ping from peer
            Ok(MemsonRequest::Ping) => self.hb = Instant::now(),
            _ => unimplemented!(),
        }
    }
}

/// Handler for Message, Memson server sends this message, we just send string to
/// peer
impl Handler<Message> for MemsonSession {
    type Result = ();

    fn handle(&mut self, msg: Message, _: &mut Self::Context) {
        // send message to peer
        self.framed.write(MemsonResponse::Data(msg.0));
    }
}

/// Helper methods
impl MemsonSession {
    pub fn new(
        addr: Addr<MemsonServer>,
        framed: actix::io::FramedWrite<MemsonResponse, WriteHalf<TcpStream>, MemsonCodec>,
    ) -> MemsonSession {
        MemsonSession {
            addr,
            framed,
            id: 0,
            hb: Instant::now(),
        }
    }

    /// helper method that sends ping to client every second.
    ///
    /// also this method check heartbeats from client
    fn hb(&self, ctx: &mut actix::Context<Self>) {
        ctx.run_later(Duration::new(1, 0), |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > Duration::new(10, 0) {
                // heartbeat timed out
                println!("Client heartbeat failed, disconnecting!");

                // notify Memson server
                act.addr.do_send(server::Disconnect { id: act.id });

                // stop actor
                ctx.stop();
            }

            act.framed.write(MemsonResponse::Ping);
            act.hb(ctx);
        });
    }
}