use crate::cmd::Cmd;
use crate::err::Error;
use crate::inmemdb::InMemDb;
use crate::json::Json;
use actix::prelude::*;

pub struct DbExecutor(pub InMemDb);

impl Actor for DbExecutor {
    type Context = SyncContext<Self>;
}

impl Message for Cmd {
    type Result = Result<Json, Error>;
}

impl Handler<Cmd> for DbExecutor {
    type Result = Result<Json, Error>;

    fn handle(&mut self, cmd: Cmd, _: &mut Self::Context) -> Self::Result {
        self.0.eval(cmd)
    }
}
