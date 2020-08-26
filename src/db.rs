use crate::cmd::QueryCmd;
use crate::err::Error;
use crate::inmemdb::InMemDb;
use crate::json::*;
use crate::ondiskdb::OnDiskDb;
use crate::query::Query;
use std::path::Path;

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
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = OnDiskDb::open(&path)?;
        let cache = db.populate()?;
        Ok(Self { cache, db })
    }

    pub(crate) fn query(&self, cmd: QueryCmd) -> Result<Json, Error> {
        let qry = Query::from(&self.cache, cmd);
        qry.exec()
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn get(&self, key: &str) -> Result<&Json, Error> {
        self.cache.get(key)
    }

    pub fn set<K:Into<String>>(&mut self, key: K, val: Json) -> Result<Option<Json>, Error> {
        let key = key.into();
        self.db.set(&key, &val)?;
        Ok(self.cache.set(key, val))
    }

    pub fn sum(&self, key: &str) -> Result<Json, Error> {
        self.cache.sum(key)
    }

    pub fn add(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        self.cache.add(lhs, rhs)
    }

    pub fn avg(&self, key: &str) -> Result<Json, Error> {
        self.cache.avg(key)
    }

    pub fn append(&mut self, key: String, val: Json) -> Result<(), Error> {
        let entry = self.cache.entry(key.clone());
        json_append(entry, val);
        self.db.set(key, entry)?;
        Ok(())
    }

    pub fn count(&self, key: &str) -> Result<Json, Error> {
        self.cache.count(key)
    }

    pub fn last(&self, key: &str) -> Result<&Json, Error> {
        self.cache.last(key)
    }

    pub fn first(&self, key: &str) -> Result<&Json, Error> {
        self.cache.first(key)
    }

    pub fn max(&self, key: &str) -> Result<&Json, Error> {
        self.cache.max(key)
    }

    pub fn min(&self, key: &str) -> Result<&Json, Error> {
        self.cache.min(key)
    }

    pub fn mul(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        self.cache.mul(lhs, rhs)
    }

    pub fn insert<K: Into<String>>(&mut self, key: K, val: Json) -> Result<usize, Error> {
        let mut rows = to_rows(val)?;
        let key = key.into();
        self.db.insert(&key, &mut rows)?;
        self.cache.insert(key, rows)
    }

    pub fn sub(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        self.cache.sub(lhs, rhs)
    }

    pub fn div(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        self.cache.div(lhs, rhs)
    }

    pub fn delete(&mut self, key: &str) -> Result<Option<Json>, Error> {
        self.db.delete(key)?;
        Ok(self.cache.delete(key))
    }

    pub fn dev(&self, key: &str) -> Result<Json, Error> {
        self.cache.dev(key)
    }

    pub fn var(&self, key: &str) -> Result<Json, Error> {
        self.cache.var(key)
    }

    pub fn pop(&mut self, key: &str) -> Result<Json, Error> {
        self.cache.pop(key)
    }

    pub fn keys(&self, _page: Option<usize>) -> Option<Vec<&String>> {
        Some(self.cache.keys())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use std::fs;

    #[test]
    fn set_persists_ok() {
        let val = json!({"name": "james"});
        let path = "set_ok";

        {
            let mut db = Memson::open(path).unwrap();
            assert_eq!(Ok(None), db.set("k", val.clone()));
        }
        {
            let db = Memson::open(path).unwrap();
            let exp = Ok(&val);
            assert_eq!(exp, db.get("k"));
        }
        fs::remove_dir_all(path).unwrap();
    }
}
