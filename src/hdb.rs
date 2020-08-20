use serde_json::Value as Json;
use sled::Db as Sled;
use std::path::Path;

use crate::err::Error;
use crate::json::JsonObj;
use crate::rdb::InMemDb;

#[derive(Debug)]
pub struct OnDiskDb {
    db: Sled,
}

impl<'a> OnDiskDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = sled::open(path).map_err(|e| {
            eprintln!("{:?}", e);
            Error::BadIO
        })?;
        Ok(Self { db })
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

    pub(crate) fn insert(&mut self, key: &str, id: usize, rows: &[JsonObj]) -> Result<(), Error> {
        Ok(())
    }
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
