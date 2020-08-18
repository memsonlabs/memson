use crate::cmd::{ReadCmd, WriteCmd};
use crate::err::Error;
use crate::hdb::OnDiskDb;
use crate::rdb::InMemDb;
use serde_json::Value as Json;
use std::path::Path;

#[derive(Debug)]
pub struct Memson {
    rdb: InMemDb,
    hdb: OnDiskDb,
}

impl Memson {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let hdb = OnDiskDb::open(path)?;
        let rdb = InMemDb::new();
        Ok(Self { rdb, hdb })
    }

    fn read_eval(cmd: ReadCmd) -> Result<Json, Error> {
        unimplemented!()
    }

    fn write_eval(cmd: WriteCmd) -> Result<Json, Error> {
        unimplemented!()
    }
}
