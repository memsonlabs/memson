use serde_json::Value as Json;
use sled::Db;
use std::path::Path;

use crate::err::Error;

#[derive(Debug)]
pub struct OnDiskDb {
    db: Db,
}

impl<'a> OnDiskDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = sled::open(path).map_err(|_| Error::BadIO)?;
        Ok(Self { db })
    }

    pub fn set<S: Into<String>>(&mut self, key: S, val: &Json) -> Result<(), Error> {
        self.db
            .insert(key.into(), serde_json::to_vec(val).unwrap())
            .map(|_| ())
            .map_err(|_| Error::BadIO)
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
    }
}
