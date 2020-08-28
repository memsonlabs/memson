use crate::err::Error;
use crate::json::*;
use serde_json::{Value as Json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct InMemDb {
    cache: BTreeMap<String, Json>,
}

impl InMemDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entry<K: Into<String>>(&mut self, key: K) -> &mut Value {
        self.cache.entry(key.into()).or_insert_with(|| Json::Null)
    }

    pub fn set<K: Into<String>>(&mut self, key: K, val: Json) -> Option<Json> {
        self.cache.insert(key.into(), val)
    }

    pub fn size(&self) -> usize {
        bincode::serialized_size(&self.cache).unwrap() as usize
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn pop(&mut self, key: &str) -> Result<Json, Error> {
        let val = self.get_mut(key)?;
        json_pop(val)
    }

    pub(crate) fn insert<K: Into<String>>(
        &mut self,
        key: K,
        arg: Vec<Json>,
    ) -> Result<usize, Error> {
        let val = self
            .cache
            .entry(key.into())
            .or_insert_with(|| Json::Array(Vec::new()));
        insert_rows(val, arg)
    }

    pub fn get(&self, key: &str) -> Result<&Json, Error> {
        self.cache
            .get(key)
            .ok_or(Error::UnknownKey(key.into()))
    }

    pub fn get_mut(&mut self, key: &str) -> Result<&mut Json, Error> {
        self.cache
            .get_mut(key)
            .ok_or(Error::UnknownKey(key.to_string()))
    }

    pub fn json_keys(&self) -> Json {
        let mut keys = Vec::new();
        for key in self.cache.keys() {
            keys.push(Json::from(key.to_string()));
        }
        Json::Array(keys)
    }

    pub fn keys(&self) -> Vec<&String> {
        self.cache.keys().collect()
    }

    pub fn delete(&mut self, key: &str) -> Option<Json> {
        self.cache.remove(key)
    }

}

#[cfg(test)]
pub mod tests {
    use super::*;
    use serde_json::json;

    fn insert_data(db: &mut InMemDb) {
        db.set("a", json!([1, 2, 3, 4, 5]));
        db.set("b", json!(true));
        db.set("i", json!(2));
        db.set("f", json!(3.3));
        db.set("ia", json!([1, 2, 3, 4, 5]));
        db.set("fa", json!([1.1, 2.2, 3.3, 4.4, 5.5]));
        db.set("x", json!(4));
        db.set("y", json!(5));
        db.set("s", json!("hello"));
        db.set("sa", json!(["a", "b", "c", "d"]));
        db.set(
            "t",
            json!([
                {"name": "james", "age": 35},
                {"name": "ania", "age": 28, "job": "English Teacher"},
                {"name": "misha", "age": 12},
            ]),
        );
    }

    fn test_db() -> InMemDb {
        let mut db = InMemDb::new();
        insert_data(&mut db);
        db
    }
    
    #[test]
    fn open_db() {
        let db = test_db();
        assert!(db.len() >= 10);
        assert_eq!(db.get("b"), Ok(&Json::Bool(true)));
        assert_eq!(
            db.get("ia"),
            Ok(&Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5),
            ]))
        );
        assert_eq!(db.get("i"), Ok(&Json::from(2)));
        assert_eq!(db.get("f"), Ok(&Json::from(3.3)));
        assert_eq!(
            db.get("fa"),
            Ok(&Json::Array(vec![
                Json::from(1.1),
                Json::from(2.2),
                Json::from(3.3),
                Json::from(4.4),
                Json::from(5.5),
            ]))
        );
        assert_eq!(db.get("f"), Ok(&Json::from(3.3)));
        assert_eq!(db.get("s"), Ok(&Json::from("hello")));
        assert_eq!(
            db.get("sa"),
            Ok(&Json::Array(vec![
                Json::from("a"),
                Json::from("b"),
                Json::from("c"),
                Json::from("d"),
            ]))
        );
    }

    #[test]
    fn test_get() {
        let db = test_db();
        assert_eq!(Ok(&Json::Bool(true)), db.get("b"));
        assert_eq!(Ok(&Json::Bool(true)), db.get("b"));
        assert_eq!(
            Ok(&Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5)
            ])),
            db.get("ia")
        );
        assert_eq!(Ok(&Json::from(2)), db.get("i"));
    }

    #[test]
    fn eval_get_ok() {
        let mut db = test_db();
        let val = vec![
            Json::from(1),
            Json::from(2),
            Json::from(3),
            Json::from(4),
            Json::from(5),
        ];
        assert_eq!(Ok(5), db.insert("t", val.clone()));
        assert_eq!(Ok(&Json::from(val)), db.get("a"));
    }

    #[test]
    fn eval_get_string_err_not_found() {
        let db = test_db();
        assert_eq!(Err(Error::UnknownKey("ania".to_string())), db.get("ania"));
    }

}
