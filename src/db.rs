use crate::cmd::{ReadCmd, Response, WriteCmd, QueryCmd};
use crate::err::Error;

use crate::inmemdb::InMemDb;
use crate::json::{Json, JsonObj};
use crate::ondiskdb::OnDiskDb;
use std::path::Path;
use crate::query::Query;

#[derive(Debug)]
struct Cache {
    rdb: InMemDb,
    hdb: OnDiskDb,
}

#[derive(Debug)]
pub struct Memson {
    cache: InMemDb,
    db: OnDiskDb,
}

impl Memson {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = OnDiskDb::open(&path)?;
        let cache = db.populate()?;
        Ok(Self { cache, db })
    }

    fn read_eval<'a>(&'a self, cmd: ReadCmd) -> Result<Response<'a>, Error> {
        match cmd {
            ReadCmd::Get(key) => self.cache.get(key.clone()).map(Response::Ref),
            ReadCmd::Query(cmd) => self.query(cmd),
        }
    }

    fn query<'a>(&'a self, cmd: QueryCmd) -> Result<Response<'a>, Error> {
        let qry = Query::from(&self.cache, cmd);
        qry.exec().map(Response::Val)
    }

    fn write_eval(&mut self, cmd: WriteCmd) -> Result<(), Error> {
        match cmd {
            WriteCmd::Set(key, val) => {
                self.db.set(&key, &val)?;
                self.cache.set(key, val);
                Ok(())
            }
            WriteCmd::Insert(key, rows) => {
                if let Ok(val) = self.cache.get_mut(key.clone()) {
                    insert_rows(val, rows)
                } else {
                    self.db.add_table(&key, &rows)?;
                    self.cache.add_table(key, rows);
                    Ok(())
                }
            }
        }
    }
}

fn insert_rows(val: &mut Json, rows: Vec<JsonObj>) -> Result<(), Error> {
    unimplemented!()
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
