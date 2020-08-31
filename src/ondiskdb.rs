use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sled::Db as Sled;
use std::path::{Path, PathBuf};

use crate::err::Error;
use crate::inmemdb::InMemDb;

fn len_key(key: &str) -> String {
    "_len_".to_string() + key
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Meta {
    key: String,
    id: usize,
}

#[derive(Debug)]
pub struct OnDiskDb {
    db: Sled,
    meta_db: Sled,
    meta: Vec<Meta>,
}

fn row_key(key: &str, id: usize) -> String {
    "_id_".to_string() + key + "-" + &id.to_string()
}

fn db_path<P: AsRef<Path>>(path: P) -> PathBuf {
    let mut buf = PathBuf::new();
    buf.push(path);
    buf.push("memson");
    buf
}

fn meta_path<P: AsRef<Path>>(path: P) -> PathBuf {
    let mut buf = PathBuf::new();
    buf.push(path);
    buf.push("meta");
    buf
}

fn open_sled<P: AsRef<Path>>(path: P) -> Result<Sled, Error> {
    sled::open(db_path(&path)).map_err(|e| {
        eprintln!("{:?}", e);
        Error::Sled(e)
    })
}

fn get(db: &Sled, key: &str) -> Result<Option<Json>, Error> {
    if let Some(v) = db
        .get(key)
        .map_err(|e| Error::Sled(e))? {
        serde_json::from_slice(v.as_ref()).map_err(|_| Error::Serialize).map(Some)
    } else {
        Ok(None)
    }
}

impl<'a> OnDiskDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = open_sled(db_path(&path))?;
        let meta_db = open_sled(meta_path(path))?;
        let meta = read_meta(&meta_db)?;
        Ok(Self { db, meta_db, meta })
    }

    pub fn set<S: Into<String>>(&mut self, key: S, val: &Json) -> Result<(), Error> {
        self.db
            .insert(key.into(), serde_json::to_vec(val).unwrap())
            .map_err(|e| Error::Sled(e))?;
        self.db.flush().map_err(|e| Error::Sled(e))?;
        Ok(())
    }

    pub fn populate(&self) -> Result<InMemDb, Error> {
        unimplemented!()
        /*
        let mut db = InMemDb::new();
        for r in self.db.iter() {
            let (k, v) = r.map_err(|e| Error::Sled(e))?;
            let key = unsafe { String::from_utf8_unchecked(k.to_vec()) };
            let val: Json = serde_json::from_slice(&v).unwrap();
            db.set(key, val);
        }
        for meta in &self.meta {
            let val = self.read_table(meta)?;
            db.set(&meta.key, val);
        }
        Ok(db)
        */
    }

    pub(crate) fn insert(&mut self, key: &str, val: &mut [Json]) -> Result<(), Error> {
        self.insert_rows(key, val)
    }

    fn write_row(&mut self, key: &str, id: usize, row: &mut Json) -> Result<(), Error> {
        match row {
            Json::Object(ref mut obj) => {
                obj.entry("_id".to_string())
                    .or_insert_with(|| Json::from(id));
            }
            _ => return Err(Error::ExpectedObj),
        };
        let key = row_key(key, id);
        let val = serde_json::to_vec(row).map_err(|_| Error::Serialize)?;
        self.meta_db.insert(key, val).map_err(bad_io)?;
        self.meta_db.flush().map_err(bad_io)?;
        Ok(())
    }

    fn write_table_meta(&mut self) -> Result<(), Error> {
        self.meta_db
            .insert(
                "_meta",
                bincode::serialize(&self.meta).map_err(|_| Error::Serialize)?,
            )
            .map_err(bad_io)?;
        Ok(())
    }

    pub fn insert_rows(&mut self, key: &str, rows: &mut [Json]) -> Result<(), Error> {
        let id: usize = if let Some(meta) = self.meta.iter_mut().find(|x| x.key == key) {
            let prev_id = meta.id;
            meta.id += rows.len();
            prev_id
        } else {
            self.meta.push(Meta {
                key: key.to_string(),
                id: rows.len(),
            });
            0
        };
        self.write_table_meta()?;
        self.write_rows(key, rows, id)?;
        Ok(())
    }

    fn write_rows(&mut self, key: &str, rows: &mut [Json], id: usize) -> Result<(), Error> {
        for (i, row) in rows.iter_mut().enumerate() {
            self.write_row(key, id + i, row)?;
        }
        Ok(())
    }

    //TODO return Option<Json> instead
    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        if let Some(i) = self.meta.iter().position(|m| m.key == key) {
            let n = self.meta[i].id;
            self.meta.remove(i);
            self.write_table_meta()?;
            self.delete_table(key, n)
        } else {
            self.db.remove(key).map_err(bad_io)?;
            Ok(())
        }
    }

    fn delete_table(&mut self, key: &str, count: usize) -> Result<(), Error> {
        self.meta_db
            .remove(len_key(key))
            .map_err(|e| Error::Sled(e))?;
        for i in 0..count {
            self.meta_db
                .remove(row_key(key, i))
                .map_err(|e| Error::Sled(e))?;
        }
        Ok(())
    }

    fn read_table(&self, meta: &Meta) -> Result<Json, Error> {
        let mut rows = Vec::new();
        let n = meta.id;

        for i in 0..n {
            if let Some(val) = self.read_row(&meta.key, i)? {
                rows.push(val);
            }
        }
        Ok(Json::from(rows))
    }

    fn read_row(&self, key: &str, id: usize) -> Result<Option<Json>, Error> {
        let key = row_key(key, id);
        get(&self.meta_db, &key)
    }
}

fn read_meta(sled: &Sled) -> Result<Vec<Meta>, Error> {
    if let Some(v) = sled.get("_meta").map_err(bad_io)? {
        bincode::deserialize(v.as_ref()).map_err(|_| Error::Serialize)
    } else {
        Ok(Vec::new())
    }
}

fn bad_io(e: sled::Error) -> Error {
    Error::Sled(e)
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use serde_json::json;
    use std::fs::remove_dir_all;

    fn rows() -> Vec<Json> {
        vec![
            json!({"_id": 0, "name": "james", "age": 35}),
            json!({"_id": 1, "name": "ania", "age": 28, "job": "english teacher"}),
            json!({"_id": 2, "name": "misha", "age": 10}),
            json!({"_id": 3, "name": "ania", "age": 20}),
        ]
    }

    #[test]
    fn insert_rows_ok() {
        let path = "insert_rows";
        match remove_dir_all(path) {
            Ok(_) => (),
            Err(_) => (),
        };
        let key = "t";
        {
            let mut db = OnDiskDb::open(path).unwrap();
            let mut rows = rows();
            db.insert_rows(key, &mut rows).unwrap();
        }
        {
            let db = OnDiskDb::open(path).unwrap();
            let rows = rows();
            let id = rows.len();
            let exp = Json::Array(rows);
            assert_eq!(
                db.meta,
                vec![Meta {
                    key: key.to_string(),
                    id
                }]
            );
            assert_eq!(
                Ok(exp),
                db.read_table(&Meta {
                    key: key.to_string(),
                    id
                })
            );
        }

        remove_dir_all(path).unwrap();
    }

    #[test]
    fn set_get_ok() {
        let path = "set_get";
        let mut db = OnDiskDb::open(path).unwrap();
        let val = json!({"name": "james"});
        assert_eq!(Ok(()), db.set("k1", &val));
        assert_eq!(Ok(Some(val)), get(&db.db, "k1"));
        remove_dir_all(path).unwrap();
    }

    #[test]
    fn set_remove_ok() {
        let path = "set_remove";
        let mut db = OnDiskDb::open(path).unwrap();
        let val = json!({"name": "james"});
        assert_eq!(Ok(()), db.set("k1", &val));
        assert_eq!(Ok(()), db.delete("k1"));
        assert_eq!(Ok(None), get(&db.db, "k1"));
        remove_dir_all(path).unwrap();
    }
}
