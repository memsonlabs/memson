//! `MemsonServer` is an actor. It maintains list of connection client session.
//! And manages available rooms. Peers send messages to other peers in same
//! room through `MemsonServer`.

use actix::prelude::*;
use rand::{self, Rng};
use std::collections::HashMap;
use serde_json::Value as Json;
use actix_derive::{Message, MessageResponse};

use crate::db::Db;
use crate::session;

/// Message for Memson server communications
#[derive(Message)]
#[rtype(result = "usize")]
/// New Memson session is created
pub struct Connect {
    pub addr: Addr<session::MemsonSession>,
}

/// Session is disconnected
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub id: usize,
}

#[derive(Message)]
#[rtype(result = "()")]
/// Send message to specific room
pub struct Query {
    /// Id of the client session
    pub id: usize,
    /// Peer message
    pub cmd: String,
}

/// `MemsonServer` manages Memson rooms and responsible for coordinating Memson
/// session. implementation is super primitive
pub struct MemsonServer {
    sessions: HashMap<usize, Addr<session::MemsonSession>>,
    db: Db,
}

impl Default for MemsonServer {
    fn default() -> MemsonServer {
        let mut db = Db::new();
        db.insert("a", Json::from(1));
        MemsonServer {
            db,
            sessions: HashMap::new(),
        }
    }
}

impl MemsonServer {
    fn eval_cmd(&mut self, qry: &Query) {
        let r = self.db.eval(qry.cmd.as_ref());
        println!("r = {:?}", r);
        let s= match r {
            Ok(data) => data.to_string(),
            Err(err) => err.to_string(),
        };

        let addr = self.sessions.get(&qry.id).unwrap();
        let r = addr.send(session::Message(s)).await.unwrap();
        println!("{:?}", r);
    }
}

/// Make actor from `MemsonServer`
impl Actor for MemsonServer {
    /// We are going to use simple Context, we just need ability to communicate
    /// with other actors.
    type Context = Context<Self>;
}

/// Handler for Connect message.
///
/// Register new session and assign unique id to this session
impl Handler<Connect> for MemsonServer {
    type Result = usize;

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
        println!("Someone connected");

        // register session with random id
        let id = rand::thread_rng().gen::<usize>();
        self.sessions.insert(id, msg.addr);

        // send id back
        id
    }
}

/// Handler for Disconnect message.
impl Handler<Disconnect> for MemsonServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        println!("Someone disconnected");        
    }
}

/// Handler for Message message.
impl Handler<Query> for MemsonServer {
    type Result = ();

    fn handle(&mut self, qry: Query, _: &mut Context<Self>)  {
        println!("query handler = {}", qry.cmd);
        self.eval_cmd(&qry)
        
        //self.send_message(&msg.room, msg.msg.as_str(), msg.id);
    }
}
