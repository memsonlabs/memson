use crate::cmd::{ReadCmd, Response, WriteCmd};
use crate::err::Error;

use crate::cmd::Response::Val;
use crate::hdb::OnDiskDb;
use crate::rdb::InMemDb;
use crate::table::{Db, Table};
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug)]
struct Cache {
    rdb: InMemDb,
    hdb: OnDiskDb,
}

#[derive(Debug)]
pub struct Memson {
    cache: Cache,
    db: Db,
}

impl Memson {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let hdb = OnDiskDb::open(&path)?;
        let rdb = hdb.populate()?;
        let cache = Cache { rdb, hdb };
        let db = Db::open("tables")?;
        Ok(Self { cache, db })
    }

    fn read_eval<'a>(&'a self, cmd: ReadCmd) -> Result<Response<'a>, Error> {
        match cmd {
            ReadCmd::Get(key) => match self.cache.rdb.get(&key) {
                Some(val) => Ok(Response::Ref(val)),
                None => Err(Error::BadKey),
            },
        }
    }

    fn write_eval(&mut self, cmd: WriteCmd) -> Result<(), Error> {
        match cmd {
            WriteCmd::Set(key, val) => {
                self.cache.hdb.set(&key, &val)?;
                self.cache.rdb.set(key, val);
            }
            WriteCmd::Insert(key, rows) => {
                self.db.insert(key, rows);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cmd::Response;
    use serde_json::json;

    #[test]
    fn set_persists_ok() {
        let val = json!({"name": "james"});
        let path = "set_ok";
        {
            let mut db = Memson::open(path).unwrap();
            assert_eq!(
                Ok(()),
                db.write_eval(WriteCmd::Set("k".to_string(), val.clone()))
            );
        }
        {
            let db = Memson::open(path).unwrap();
            let exp = Ok(Response::Ref(&val));
            assert_eq!(exp, db.read_eval(ReadCmd::Get("k".to_string())));
        }
    }
}
