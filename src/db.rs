
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

    pub fn get(&self, key: &str) -> Result<&Json, Error> {
        self.cache.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Result<&mut Json, Error> {
        self.cache.get_mut(key)
    }

    pub fn set(&mut self, key: String, val: Json) -> Result<Option<Json>, Error> {
        self.db.set(&key, &val)?;
        Ok(self.cache.set(key, val))
    }

    pub fn sum(&self, key: &str) -> Result<Option<Json>, Error> {
        let val = self.get(key)?;
        json_sum(val).map(Some)
    }

    pub fn add(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        let x = self.get(lhs)?;
        let y = self.get(rhs)?;
        json_add(x, y)
    }

    pub fn avg(&self, key: &str) -> Result<Json, Error> {
        let val = self.get(key)?;
        json_avg(val)
    }

    pub fn append(&mut self, key: String, val: Json) -> Result<(), Error> {
        let entry = self.cache.entry(key.clone());
        json_append(entry, val);
        self.db.set(key, entry)?;
        Ok(())
    }

    pub fn count(&self, key: &str) -> Result<Json, Error> {
        let val = self.get(key)?;
        Ok(Json::from(json_count(val)))
    }

    pub fn last(&self, key: &str) -> Result<&Json, Error> {
        let val = self.get(key)?;
        json_last(val)
    }

    pub fn first(&self, key: &str) -> Result<&Json, Error> {
        let val = self.get(key)?;
        json_first(val)
    }

    pub fn max(&self, key: &str) -> Result<&Json, Error> {
        let val = self.get(key)?;
        json_max(val)
    }

    pub fn min(&self, key: &str) -> Result<&Json, Error> {
        let val = self.get(key)?;
        json_min(val)
    }

    pub fn mul(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        let x = self.get(lhs)?;
        let y = self.get(rhs)?;
        json_mul(x, y)
    }

    pub fn insert(&mut self, key: String, val: Json) -> Result<usize, Error> {
        let mut rows = to_rows(val)?;
        self.db.insert(&key, &mut rows)?;
        self.cache.insert(key, rows)
    }

    pub fn sub(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        let x = self.get(lhs)?;
        let y = self.get(rhs)?;
        json_sub(x, y)
    }

    pub fn div(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        let x = self.get(lhs)?;
        let y = self.get(rhs)?;
        json_div(x, y)
    }

    pub fn delete(&mut self, key: &str) -> Result<Option<Json>, Error> {
        self.db.delete(key)?;
        Ok(self.cache.delete(key))
    }

    pub fn dev(&self, key: &str) -> Result<Json, Error> {
        let val = self.get(key)?;
        json_dev(val)
    }

    pub fn var(&self, key: &str) -> Result<Json, Error> {
        let val = self.get(key)?;
        json_var(val)
    }

    pub fn pop(&mut self, key: &str) -> Result<Json, Error> {
        let val = self.get_mut(key)?;
        json_pop(val)
    }

    pub fn keys(&self, _page: Option<usize>) -> Option<Vec<&String>> {
        Some(self.cache.keys())
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
