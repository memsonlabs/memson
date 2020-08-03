//! A "tiny database" and accompanying protocol
//!
//! This example shows the usage of shared state amongst all connected clients,
//! namely a database of key/value pairs. Each connected client can send a
//! series of GET/SET commands to query the current value of a key or set the
//! value of a key.
//!
//! This example has a simple protocol you can use to interact with the server.
//! To run, first run this in one terminal window:
//!
//!     cargo run --example tinydb
//!
//! and next in another windows run:
//!
//!     cargo run --example connect 127.0.0.1:8080
//!
//! In the `connect` window you can type in commands where when you hit enter
//! you'll get a response from the server for that command. An example session
//! is:
//!
//!
//!     $ cargo run --example connect 127.0.0.1:8080
//!     GET foo
//!     foo = bar
//!     GET FOOBAR
//!     error: no key FOOBAR
//!     SET FOOBAR my awesome string
//!     set FOOBAR = `my awesome string`, previous: None
//!     SET foo tokio
//!     set foo = `tokio`, previous: Some("bar")
//!     GET foo
//!     foo = tokio
//!
//! Namely you can issue two forms of commands:
//!
//! * `GET $key` - this will fetch the value of `$key` from the database and
//!   return it. The server's database is initially populated with the key `foo`
//!   set to the value `bar`
//! * `SET $key $value` - this will set the value of `$key` to `$value`,
//!   returning the previous value, if any.

#![warn(rust_2018_idioms)]

use tokio::net::TcpListener;
use tokio::stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use futures::SinkExt;
use std::env;
use std::error::Error;
use std::sync::{Arc, RwLock};
use crate::db::{Cmd, Db, Res};

mod db;
mod json;

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
pub struct DbServer {
    db: RwLock<Db>,
}

fn res_to_string(res: &Res<'_>) -> String {
    match res {
        Ok(val) => val.to_string(),
        Err(err) => err.to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let mut listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Arc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.
    let initial_db = Db::open("prod.memson").unwrap();
    let db = Arc::new(DbServer {
        db: RwLock::new(initial_db),
    });

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // After getting a new connection first we see a clone of the database
                // being created, which is creating a new reference for this connected
                // client to use.
                let db = db.clone();

                // Like with other small servers, we'll `spawn` this client to ensure it
                // runs concurrently with all other clients. The `move` keyword is used
                // here to move ownership of our db handle into the async closure.
                tokio::spawn(async move {
                    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
                    // to convert our stream of bytes, `socket`, into a `Stream` of lines
                    // as well as convert our line based responses into a stream of bytes.
                    let mut lines = Framed::new(socket, LinesCodec::new());

                    // Here for every line we get back from the `Framed` decoder,
                    // we parse the request, and if it's valid we generate a response
                    // based on the values in the database.
                    while let Some(result) = lines.next().await {
                        match result {
                            Ok(line) => {
                                let response = handle_request(&line, &db);

                                if let Err(e) = lines.send(response.as_str()).await {
                                    println!("error on sending response; error = {:?}", e);
                                }
                            }
                            Err(e) => {
                                println!("error on decoding from socket; error = {:?}", e);
                            }
                        }
                    }

                    // The connection will be closed at this point as `lines.next()` has returned `None`.
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

fn handle_request(line: &str, server: &Arc<DbServer>) -> String {
    let cmd: Cmd = match serde_json::from_str(line) {
        Ok(cmd) => cmd,
        Err(_) => return "error: bad json".to_string(),
    };
    if cmd.is_mutation() {
        let mut db = server.db.write().unwrap();
        let r = db.eval_write_cmd(cmd);
        res_to_string(&r)
    } else {
        let db = server.db.read().unwrap();
        let r = db.eval_read_cmd(cmd);
        res_to_string(&r)
    }
}