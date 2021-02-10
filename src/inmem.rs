use crate::cmd::{Cmd, QueryCmd, Range};
use crate::db::{Query, PAGE_SIZE};
use crate::err::Error;
use crate::eval::eval_cmd;
use crate::json::{json_get, Json};
use crate::ondisk::{ivec_to_json, OnDiskDb};
use crate::Res;
use serde_json::json;
use std::collections::BTreeMap;

pub type Cache = BTreeMap<String, Json>;

pub fn load_cache(db: &sled::Db) -> Result<Cache, Error> {
    let mut cache = Cache::new();
    for kv in db.iter() {
        let (key, val) = kv.map_err(|_| Error::BadIO)?;
        let key: String = bincode::deserialize(key.as_ref()).map_err(|_| Error::BadIO)?;
        let val: Json = bincode::deserialize(val.as_ref()).map_err(|_| Error::BadIO)?;
        cache.insert(key, val);
    }
    Ok(cache)
}

/// The in-memory database of memson
#[derive(Debug)]
pub struct InMemDb {
    cache: Cache,
}

impl InMemDb {
    /// retrieves the val of the ke/val entry
    fn key(&self, key: String) -> Result<Json, Error> {
        self.cache.get(&key).cloned().ok_or(Error::BadKey(key))
    }

    /// populate an in memory database from a on disk db
    ///
    pub fn load(on_disk_db: &OnDiskDb) -> Result<Self, Error> {
        let mut inmem_db = InMemDb::new();
        for kv in on_disk_db.sled.iter() {
            let (key, val) = kv.map_err(|_| Error::BadIO)?;
            let s = unsafe { String::from_utf8_unchecked(key.as_ref().to_vec()) };
            inmem_db.set(s, ivec_to_json(&val)?);
        }
        Ok(inmem_db)
    }

    /// the paginated keys of entried in memson
    pub fn keys(&self, range: Option<Range>) -> Vec<Json> {
        if let Some(range) = range {
            let start = range.start.unwrap_or(0);
            let size = range.size.unwrap_or(PAGE_SIZE);
            self.cache
                .keys()
                .skip(start)
                .take(size)
                .cloned()
                .map(Json::from)
                .collect()
        } else {
            self.cache
                .keys()
                .take(PAGE_SIZE)
                .cloned()
                .map(Json::from)
                .collect()
        }
    }

    /// delete an entry by key and return the previous value if exists
    pub fn delete(&mut self, key: &str) -> Option<Json> {
        self.cache.remove(key)
    }

    //TODO remove allocations
    /// eval key command that supports nesting
    pub fn eval_key(&self, key: String) -> Res {
        let mut it = key.split('.');
        let key = it.next().ok_or_else(|| Error::BadKey(key.clone()))?;
        let mut val = self.key(key.to_string())?;
        for key in it {
            if let Some(v) = json_get(key, &val) {
                val = v;
            } else {
                return Err(Error::BadKey(key.to_string()));
            }
        }
        Ok(val)
    }

    /// has checks if the key is contained in memson or not
    pub fn has(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    /// get a key/val entry; similar to key but takes a reference to a string
    pub fn get(&self, key: &str) -> Result<&Json, Error> {
        self.cache
            .get(key)
            .ok_or_else(|| Error::BadKey(key.to_string()))
    }

    /// get a key/val entry; similar to key but takes a reference to a string
    pub fn get_mut(&mut self, key: &str) -> Result<&mut Json, Error> {
        self.cache
            .get_mut(key)
            .ok_or_else(|| Error::BadKey(key.to_string()))
    }

    /// inserts a new key/val entry
    pub fn set<K: Into<String>>(&mut self, key: K, val: Json) -> Option<Json> {
        self.cache.insert(key.into(), val)
    }

    /// evaluate a command
    pub fn eval(&mut self, cmd: Cmd) -> Res {
        eval_cmd(self, cmd)
    }

    /// create a new instance of the in-memory database with no entries
    pub fn new() -> Self {
        Self {
            cache: Cache::new(),
        }
    }

    /// retrieves a key/val entry and if not present, it inserts an entry
    pub fn entry<K: Into<String>>(&mut self, key: K) -> &mut Json {
        self.cache.entry(key.into()).or_insert_with(|| Json::Null)
    }

    /// summary of keys stored and no. of entries
    pub fn summary(&self) -> Json {
        let no_entries = Json::from(self.cache.len());
        let keys: Vec<Json> = self
            .cache
            .keys()
            .map(|x| Json::String(x.to_string()))
            .collect();
        json!({"no_entries": no_entries, "keys": keys})
    }

    /// execute query
    pub fn query(&self, cmd: QueryCmd) -> Res {
        let qry = Query::from(&self, cmd);
        qry.exec()
    }
}

impl Default for InMemDb {
    fn default() -> Self {
        InMemDb::new()
    }
}
