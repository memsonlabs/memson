use serde_json::Value as Json;
use sled::{Config, Db};
use std::path::Path;

use crate::db::Error;

pub struct StorageEngine {
    db: Db,
}

impl<'a> StorageEngine {
    fn open<P: AsRef<Path>>(path: P) -> Result<Db, Error> {
        sled::open(path).map_err(|_| Error::BadIO)
    }

    fn write(&mut self, key: String, val: &Json) -> Result<(), Error> {
        self.db
            .insert(key, serde_json::to_vec(val).unwrap())
            .map(|_| ())
            .map_err(|_| Error::BadIO)
    }
}
