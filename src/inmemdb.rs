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

    pub fn len(&self) -> usize {
        self.cache.len()
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

    pub fn div(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        let x = self.get(lhs)?;
        let y = self.get(rhs)?;
        json_div(x, y)
    }

    pub fn sub(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        let x = self.get(lhs)?;
        let y = self.get(rhs)?;
        json_sub(x, y)
    }

    pub fn mul(&self, lhs: &str, rhs: &str) -> Result<Json, Error> {
        let x = self.get(lhs)?;
        let y = self.get(rhs)?;
        json_mul(x, y)
    }

    pub fn sum(&self, key: &str) -> Result<Json, Error> {
        let val = self.get(key)?;
        json_sum(val)
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

    /*
    pub fn eval_read_cmd(&self, cmd: Cmd) -> Res<'_> {
        match cmd {
            Cmd::Get(key) => self.get(key),
            Cmd::Count(key) => self.count(key),
            Cmd::Max(key) => self.max(key),
            Cmd::Min(key) => self.min(key),
            Cmd::Avg(key) => self.avg(key),
            Cmd::Dev(key) => self.dev(key),
            Cmd::Add(lhs, rhs) => self.add(lhs, rhs),
            Cmd::Sub(lhs, rhs) => self.sub(lhs, rhs),
            Cmd::Mul(lhs, rhs) => self.eval_mul(lhs, rhs),
            Cmd::Div(lhs, rhs) => self.eval_div(lhs, rhs),
            Cmd::Sum(key) => self.eval_sum(key).map(Response::Val),
            Cmd::First(key) => self.eval_first(key),
            Cmd::Last(key) => self.eval_last(key),
            Cmd::Var(key) => self.eval_var(key),
            Cmd::Query(qry) => self.eval_query(qry),
            Cmd::Set(_, _) | Cmd::Append(_, _) | Cmd::Pop(_) | Cmd::Insert(_, _) => {
                Err(Error::BadState)
            }
        }
    }
    */

    /*
    pub fn eval_write_cmd(&mut self, cmd: Cmd) -> Res<'_> {
        match cmd {
            Cmd::Set(key, val) => self.eval_set(key, val),
            Cmd::Append(key, val) => self.eval_append(key, val),
            Cmd::Pop(key) => self.eval_pop(key),
            Cmd::Insert(key, val) => self.eval_insert(key, val),
            Cmd::Get(_)
            | Cmd::Count(_)
            | Cmd::Max(_)
            | Cmd::Min(_)
            | Cmd::Avg(_)
            | Cmd::Dev(_)
            | Cmd::Sum(_)
            | Cmd::Add(_, _)
            | Cmd::Sub(_, _)
            | Cmd::Mul(_, _)
            | Cmd::Div(_, _)
            | Cmd::First(_)
            | Cmd::Last(_)
            | Cmd::Var(_)
            | Cmd::Query(_) => Err(Error::BadState),
        }
    }
    */

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
    use assert_approx_eq::assert_approx_eq;
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

    type Res = Result<Json, Error>;

    fn json_f64(v: &Json) -> f64 {
        v.as_f64().unwrap()
    }

    fn bad_type<'a>() -> Res {
        Err(Error::BadType)
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
    fn test_first() {
        let db = test_db();
        assert_eq!(Ok(&Json::Bool(true)), db.first("b"));
        assert_eq!(Ok(&Json::Bool(true)), db.first("b"));
        assert_eq!(Ok(&Json::from(3.3)), db.first("f"));
        assert_eq!(Ok(&Json::from(2)), db.first("i"));
        assert_eq!(Ok(&Json::from(1.1)), db.first("fa"));
        assert_eq!(Ok(&Json::from(1)), db.first("ia"));
    }

    #[test]
    fn test_last() {
        let db = test_db();
        assert_eq!(Ok(&Json::from(true)), db.last("b"));
        assert_eq!(Ok(&Json::from(3.3)), db.last("f"));
        assert_eq!(Ok(&Json::from(2)), db.last("i"));
        assert_eq!(Ok(&Json::from(5.5)), db.last("fa"));
        assert_eq!(Ok(&Json::from(5)), db.last("ia"));
    }

    #[test]
    fn test_max() {
        let db = test_db();
        assert_eq!(Ok(&Json::Bool(true)), db.max("b"));
        assert_eq!(Ok(&Json::from(2)), db.max("i"));
        assert_eq!(Ok(&Json::from(3.3)), db.max("f"));
        assert_eq!(Ok(&Json::from(5)), db.max("ia"));
        assert_eq!(Err(Error::FloatCmp), db.max("fa"));
    }

    #[test]
    fn test_min() {
        let db = test_db();
        assert_eq!(Ok(&Json::Bool(true)), db.min("b"));
        assert_eq!(Ok(&Json::from(2)), db.min("i"));
        assert_eq!(Ok(&Json::from(3.3)), db.min("f"));
        assert_eq!(Err(Error::FloatCmp), db.min("fa"));
        assert_eq!(Ok(&Json::from(1)), db.min("ia"));
    }

    #[test]
    fn test_avg() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.avg("f"));
        assert_eq!(Ok(Json::from(2)), db.avg("i"));
        assert_eq!(Ok(Json::from(3.3)), db.avg("fa"));
        assert_eq!(Ok(Json::from(3.0)), db.avg("ia"));
    }

    #[test]
    fn test_var() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.var("f"));
        assert_eq!(Ok(Json::from(2)), db.var("i"));
        let val = db.var("fa").unwrap();
        assert_approx_eq!(3.10, json_f64(&val), 0.0249f64);
        let val = db.var("ia").unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
    }

    #[test]
    fn test_dev() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.dev("f"));
        assert_eq!(Ok(Json::from(2)), db.dev("i"));
        let val = db.dev("fa").unwrap();
        assert_approx_eq!(1.55, json_f64(&val), 0.03f64);
        let val = db.dev("ia").unwrap();
        assert_approx_eq!(1.414, json_f64(&val), 0.03f64);
    }

    #[test]
    fn test_add() {
        let db = test_db();
        assert_eq!(Ok(Json::from(9)), db.add("x", "y"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
                Json::from(8.0),
                Json::from(9.0),
            ])),
            db.add("x", "ia")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(4.0),
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
            ])),
            db.add("ia", "i")
        );

        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("ahello"),
                Json::from("bhello"),
                Json::from("chello"),
                Json::from("dhello"),
            ])),
            db.add("sa", "s")
        );
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("helloa"),
                Json::from("hellob"),
                Json::from("helloc"),
                Json::from("hellod"),
            ])),
            db.add("s", "sa")
        );

        assert_eq!(Ok(Json::from("hellohello")), db.add("s", "s"));
        assert_eq!(bad_type(), db.add("s", "f"));
        assert_eq!(bad_type(), db.add("f", "s"));
        assert_eq!(bad_type(), db.add("i", "s"));
        assert_eq!(bad_type(), db.add("s", "i"));
    }

    #[test]
    fn test_sub() {
        let db = test_db();
        assert_eq!(Ok(Json::from(-1.0)), db.sub("x", "y"));
        assert_eq!(Ok(Json::from(1.0)), db.sub("y", "x"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.0),
                Json::from(-1.0),
            ])),
            db.sub("x", "ia")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(-4.0),
                Json::from(-3.0),
                Json::from(-2.0),
                Json::from(-1.0),
                Json::from(0.0),
            ])),
            db.sub("ia", "y")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
            ])),
            db.sub("ia", "ia")
        );

        assert_eq!(bad_type(), db.sub("s", "s"));
        assert_eq!(bad_type(), db.sub("sa", "s"));
        assert_eq!(bad_type(), db.sub("s", "sa"));
        assert_eq!(bad_type(), db.sub("i", "s"));
        assert_eq!(bad_type(), db.sub("s", "i"));
    }

    #[test]
    fn json_mul() {
        let db = test_db();
        assert_eq!(Ok(Json::from(20.0)), db.mul("x", "y"));
        assert_eq!(Ok(Json::from(16.0)), db.mul("x", "x"));
        let arr = vec![
            Json::from(5.0),
            Json::from(10.0),
            Json::from(15.0),
            Json::from(20.0),
            Json::from(25.0),
        ];
        assert_eq!(Ok(Json::from(arr.clone())), db.mul("ia", "y"));
        assert_eq!(Ok(Json::from(arr)), db.mul("y", "ia"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(4.0),
                Json::from(9.0),
                Json::from(16.0),
                Json::from(25.0),
            ])),
            db.mul("ia", "ia")
        );
        assert_eq!(bad_type(), db.mul("s", "s"));
        assert_eq!(bad_type(), db.mul("sa", "s"));
        assert_eq!(bad_type(), db.mul("s", "sa"));
        assert_eq!(bad_type(), db.mul("i", "s"));
        assert_eq!(bad_type(), db.mul("s", "i"));
    }

    #[test]
    fn json_div() {
        let db = test_db();
        assert_eq!(Ok(Json::from(1.0)), db.div("x", "x"));
        assert_eq!(Ok(Json::from(1.0)), db.div("y", "y"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
            ])),
            db.div("ia", "ia")
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.5),
                Json::from(1.0),
                Json::from(1.5),
                Json::from(2.0),
                Json::from(2.5),
            ])),
            db.div("ia", "i")
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.6666666666666666),
                Json::from(0.5),
                Json::from(0.4),
            ])),
            db.div("i", "ia")
        );

        assert_eq!(bad_type(), db.div("s", "s"));
        assert_eq!(bad_type(), db.div("sa", "s"));
        assert_eq!(bad_type(), db.div("s", "sa"));
        assert_eq!(bad_type(), db.div("i", "s"));
        assert_eq!(bad_type(), db.div("s", "i"));
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

    #[test]
    fn eval_append_obj() {
        let mut db = test_db();
        let key = "anna".to_string();
        db.set(key.clone(), json!({"name":"anna", "age": 28}));
        db.append(&key, json!({"email": "anna@gmail.com"}));
        assert_eq!(
            Ok(&json!({"name":"anna", "age": 28, "email": "anna@gmail.com"})),
            db.get("anna")
        );
    }
}
