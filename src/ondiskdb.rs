use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sled::Db as Sled;
use std::path::Path;

use crate::err::Error;
use crate::inmemdb::InMemDb;
use crate::json::{into_json_obj, json_obj, JsonObj};

#[derive(Debug, Deserialize, Serialize)]
struct Meta {
    key: String,
    id: usize,
}

#[derive(Debug)]
pub struct OnDiskDb {
    db: Sled,
    meta: Vec<Meta>,
}

impl<'a> OnDiskDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = sled::open(path).map_err(|e| {
            eprintln!("{:?}", e);
            Error::BadIO
        })?;
        let meta = read_meta(&db)?;
        Ok(Self { db, meta })
    }

    pub fn set<S: Into<String>>(&mut self, key: S, val: &Json) -> Result<(), Error> {
        self.db
            .insert(key.into(), serde_json::to_vec(val).unwrap())
            .map_err(|_| Error::BadIO)?;
        self.db.flush().map_err(|_| Error::BadIO)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Json>, Error> {
        self.db
            .get(key)
            .map(|o| o.map(|v| serde_json::from_slice(v.as_ref()).unwrap()))
            .map_err(|_| Error::BadIO)
    }

    pub fn remove(&mut self, key: &str) -> Result<(), Error> {
        self.db.remove(key).map_err(|_| Error::BadIO)?;
        Ok(())
    }

    pub fn populate(&self) -> Result<InMemDb, Error> {
        let mut db = InMemDb::new();
        for r in self.db.iter() {
            let (k, v) = r.map_err(|_| Error::BadIO)?;
            let key = unsafe { String::from_utf8_unchecked(k.to_vec()) };
            let val: Json = serde_json::from_slice(&v).unwrap();
            db.set(key, val);
        }
        Ok(db)
    }

    pub(crate) fn insert(
        &mut self,
        _key: &str,
        _id: usize,
        _rows: &[JsonObj],
    ) -> Result<(), Error> {
        Ok(())
    }

    fn read_table_id(sled: &Sled, key: &str) -> Result<usize, Error> {
        let key = "_len_".to_string() + key;
        let v = sled.get(key).map_err(bad_io)?.ok_or(Error::BadIO)?;
        let val: usize = bincode::deserialize(v.as_ref()).unwrap();
        Ok(val)
    }

    fn read_row(sled: &Sled, key: &str, i: usize) -> Result<Option<JsonObj>, Error> {
        let key = "_id_".to_string() + key + "_" + &i.to_string();
        if let Some(v) = sled.get(&key).map_err(bad_io)? {
            let val = into_json_obj(serde_json::from_slice(v.as_ref()).unwrap())?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    fn write_row(&mut self, key: &str, i: usize, row: &JsonObj) -> Result<(), Error> {
        let key = "_id_".to_string() + key + "_" + &i.to_string();
        let val = serde_json::to_vec(row).map_err(|_| Error::BadIO)?;
        self.db.insert(key, val).map_err(bad_io);
        self.db.flush().map_err(bad_io)?;
        Ok(())
    }

    fn write_table_id(&mut self, key: &str, id: usize) -> Result<(), Error> {
        let key = "_len_".to_string() + key;
        let val: Vec<u8> = bincode::serialize(&id).map_err(|_| Error::BadIO)?;
        self.db.insert(key, val).map_err(bad_io)?;
        Ok(())
    }

    fn add_table_meta(&mut self, key: String, id: usize) -> Result<(), Error> {
        self.meta.push(Meta { key, id });
        self.write_table_meta()?;
        Ok(())
    }

    fn write_table_meta(&mut self) -> Result<(), Error> {
        self.db
            .insert(
                "_meta",
                bincode::serialize(&self.meta).map_err(|_| Error::BadIO)?,
            )
            .map_err(bad_io)?;
        Ok(())
    }

    pub fn add_table(&mut self, key: &str, rows: &[JsonObj]) -> Result<(), Error> {

        unimplemented!()
    }
}

fn read_meta(sled: &Sled) -> Result<Vec<Meta>, Error> {
    if let Some(v) = sled.get("_meta").map_err(bad_io)? {
        bincode::deserialize(v.as_ref()).map_err(|_| Error::BadIO)
    } else {
        Ok(Vec::new())
    }
}

fn bad_io(_: sled::Error) -> Error {
    Error::BadIO
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn set_get_ok() {
        let mut db = OnDiskDb::open("set_get").unwrap();
        let val = json!({"name": "james"});
        assert_eq!(Ok(()), db.set("k1", &val));
        assert_eq!(Ok(Some(val)), db.get("k1"));
    }

    #[test]
    fn set_remove_ok() {
        let mut db = OnDiskDb::open("set_remove").unwrap();
        let val = json!({"name": "james"});
        assert_eq!(Ok(()), db.set("k1", &val));
        assert_eq!(Ok(()), db.remove("k1"));
        assert_eq!(Ok(None), db.get("k1"));
    }
}
