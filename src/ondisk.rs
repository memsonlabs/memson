use crate::err::Error;
use crate::json::Json;
use sled::Iter;
use std::path::Path;

pub struct OnDiskDb {
    pub sled: sled::Db,
}

impl OnDiskDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<OnDiskDb, Error> {
        let db = sled::open(path).map_err(|_| Error::BadIO)?;
        Ok(OnDiskDb { sled: db })
    }

    pub fn set(&self, key: &str, val: &Json) -> Result<Option<Json  >, Error> {
        let bytes = serde_json::to_vec(val).map_err(|_| Error::Serialize)?;
        let iv: Option<sled::IVec> = self
            .sled
            .insert(key.as_bytes(), bytes)
            .map_err(|_| Error::BadIO)?;
        match iv {
            Some(v) => ivec_to_json_opt(&v),
            None => Ok(None),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Json>, Error> {
        let val = self.sled.get(key).map_err(|_| Error::BadIO)?;
        match val {
            Some(v) => ivec_to_json_opt(&v),
            None => Ok(None),
        }
    }

    pub fn iter(&self) -> Iter {
        self.sled.iter()
    }
}

pub fn ivec_to_json_opt(ivec: &sled::IVec) -> Result<Option<Json>, Error> {
    ivec_to_json(ivec).map(Some)
}

pub fn ivec_to_json(ivec: &sled::IVec) -> Result<Json, Error> {
    let val: Json = serde_json::from_slice(ivec.as_ref()).map_err(|_| Error::Serialize)?;
    Ok(val)
}
