use crate::cmd::{Cmd, QueryCmd, Reply, Res};
use crate::err::Error;
use crate::json::*;
use serde_json::Value as Json;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct InMemDb {
    cache: BTreeMap<String, Json>,
    ids: BTreeMap<String, usize>,
}

impl InMemDb {
    pub fn new() -> Self {
        Self::default()
    }

    fn eval_append(&mut self, key: String, arg: Json) -> Res<'_> {
        let val = self.get_mut(key)?;
        append(val, arg);
        Ok(Reply::Update)
    }

    fn eval_insert(&mut self, key: String, arg: Json) -> Res<'_> {
        let n = self.insert(key, arg)?;
        Ok(Reply::Insert(n))
    }

    fn gen_insert_entry(&mut self, key: String) -> (&mut Json, &mut usize) {
        let val = self
            .cache
            .entry(key.clone())
            .or_insert_with(|| Json::Array(vec![]));
        let id = self.ids.entry(key).or_insert(0);
        (val, id)
    }

    fn insert(&mut self, key: String, arg: Json) -> Result<usize, Error> {
        let (val, id) = self.gen_insert_entry(key);
        insert(val, arg, id)
    }

    pub fn eval_read_cmd(&self, cmd: Cmd) -> Res<'_> {
        match cmd {
            Cmd::Get(key) => self.eval_get(key),
            Cmd::Count(key) => self.eval_len(key),
            Cmd::Max(key) => self.eval_max(key),
            Cmd::Min(key) => self.eval_min(key),
            Cmd::Avg(key) => self.eval_avg(key),
            Cmd::Dev(key) => self.eval_dev(key),
            Cmd::Add(lhs, rhs) => self.eval_add(lhs, rhs),
            Cmd::Sub(lhs, rhs) => self.eval_sub(lhs, rhs),
            Cmd::Mul(lhs, rhs) => self.eval_mul(lhs, rhs),
            Cmd::Div(lhs, rhs) => self.eval_div(lhs, rhs),
            Cmd::Sum(key) => self.eval_sum(key).map(Reply::Val),
            Cmd::First(key) => self.eval_first(key),
            Cmd::Last(key) => self.eval_last(key),
            Cmd::Var(key) => self.eval_var(key),
            Cmd::Query(qry) => self.eval_query(qry),
            Cmd::Set(_, _) | Cmd::Append(_, _) | Cmd::Pop(_) | Cmd::Insert(_, _) => {
                Err(Error::BadState)
            }
        }
    }

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

    fn eval_get(&self, key: String) -> Res<'_> {
        match self.cache.get(&key) {
            Some(val) => Ok(Reply::Ref(val)),
            None => Err(Error::UnknownKey(key)),
        }
    }

    fn eval_len(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        Ok(Reply::Val(len(val)))
    }

    fn eval_sum(&self, key: String) -> Result<Json, Error> {
        let val = self.get(key)?;
        sum(val)
    }

    fn eval_query(&self, cmd: QueryCmd) -> Res<'_> {
        cmd.eval(self).map(Reply::Val)
    }

    fn eval_min(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        match json_min(val) {
            Ok(Some(val)) => Ok(Reply::Val(val)),
            Ok(None) => Err(Error::EmptySequence),
            Err(err) => Err(err),
        }
    }

    fn eval_max(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        match json_max(val) {
            Ok(Some(val)) => Ok(Reply::Val(val)),
            Ok(None) => Err(Error::EmptySequence),
            Err(err) => Err(err),
        }
    }

    fn eval_avg(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        avg(val).map(Reply::Val)
    }

    fn eval_dev(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        dev(val).map(Reply::Val)
    }

    fn eval_first(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        first(val)
    }

    fn eval_last(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        last(val)
    }

    fn eval_pop(&mut self, key: String) -> Res<'_> {
        let val = self.get_mut(key)?;
        pop(val)
    }

    fn eval_set(&mut self, key: String, val: Json) -> Res<'_> {
        let reply = if let Some(_) = self.cache.insert(key, val) {
            Reply::Update
        } else {
            Reply::Insert(1) // TODO remove hardcoded value
        };
        Ok(reply)
    }

    fn eval_var(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        var(val).map(Reply::Val)
    }

    fn eval_bin_fn(
        &self,
        lhs: String,
        rhs: String,
        f: &dyn Fn(&Json, &Json) -> Result<Json, Error>,
    ) -> Res<'_> {
        let lhs = match self.cache.get(&lhs) {
            Some(val) => val,
            None => return Err(Error::UnknownKey(lhs)),
        };
        let rhs = match self.cache.get(&rhs) {
            Some(val) => val,
            None => return Err(Error::UnknownKey(rhs)),
        };
        f(lhs, rhs).map(Reply::Val)
    }
    fn eval_add(&self, lhs: String, rhs: String) -> Res<'_> {
        self.eval_bin_fn(lhs, rhs, &add)
    }

    fn eval_sub(&self, lhs: String, rhs: String) -> Res<'_> {
        self.eval_bin_fn(lhs, rhs, &sub)
    }
    fn eval_mul(&self, lhs: String, rhs: String) -> Res<'_> {
        self.eval_bin_fn(lhs, rhs, &mul)
    }

    fn eval_div(&self, lhs: String, rhs: String) -> Res<'_> {
        self.eval_bin_fn(lhs, rhs, &div)
    }

    pub fn get(&self, key: String) -> Result<&Json, Error> {
        self.cache.get(&key).ok_or(Error::UnknownKey(key))
    }

    pub fn get_mut(&mut self, key: String) -> Result<&mut Json, Error> {
        self.cache.get_mut(&key).ok_or(Error::UnknownKey(key))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use assert_approx_eq::assert_approx_eq;
    use serde_json::json;

    pub fn append<K: Into<String>, V: Into<Json>>(db: &mut InMemDb, key: K, val: V) {
        let key = key.into();
        let val = val.into();
        db.eval_write_cmd(Cmd::Append(key, val)).unwrap();
    }

    fn insert_data(db: &mut InMemDb) {
        set(db, "a", json!([1, 2, 3, 4, 5]));
        set(db, "b", json!(true));
        set(db, "i", json!(2));
        set(db, "f", json!(3.3));
        set(db, "ia", json!([1, 2, 3, 4, 5]));
        set(db, "fa", json!([1.1, 2.2, 3.3, 4.4, 5.5]));
        set(db, "x", json!(4));
        set(db, "y", json!(5));
        set(db, "s", json!("hello"));
        set(db, "sa", json!(["a", "b", "c", "d"]));
        set(
            db,
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

    fn json_val(r: Reply<'_>) -> Json {
        match r {
            Reply::Val(val) => val.clone(),
            Reply::Ref(val) => val.clone(),
            _ => Json::from("bad state"),
        }
    }

    fn eval_read(db: &InMemDb, cmd: Cmd) -> Res {
        db.eval_read_cmd(cmd).map(json_val)
    }

    fn eval_write(db: &mut InMemDb, cmd: Cmd) -> Res {
        db.eval_write_cmd(cmd).map(json_val)
    }

    fn get<S: Into<String>>(db: &InMemDb, key: S) -> Res {
        eval_read(db, Cmd::get(key.into()))
    }

    fn first<S: Into<String>>(db: &InMemDb, key: S) -> Res {
        eval_read(db, Cmd::first(key.into()))
    }

    fn last<S: Into<String>>(db: &InMemDb, key: S) -> Res {
        eval_read(db, Cmd::last(key.into()))
    }

    fn max<S: Into<String>>(db: &InMemDb, key: S) -> Res {
        eval_read(db, Cmd::max(key.into()))
    }

    fn min<S: Into<String>>(db: &InMemDb, key: S) -> Res {
        eval_read(db, Cmd::min(key.into()))
    }

    fn var<S: Into<String>>(db: &InMemDb, key: S) -> Res {
        eval_read(db, Cmd::var(key.into()))
    }

    fn dev<S: Into<String>>(db: &InMemDb, key: S) -> Res {
        eval_read(db, Cmd::dev(key.into()))
    }

    fn avg<S: Into<String>>(db: &InMemDb, key: S) -> Res {
        eval_read(db, Cmd::avg(key.into()))
    }

    fn div<S: Into<String>>(db: &InMemDb, lhs: S, rhs: S) -> Res {
        eval_read(db, Cmd::Div(lhs.into(), rhs.into()))
    }

    fn mul<S: Into<String>>(db: &InMemDb, lhs: S, rhs: S) -> Res {
        eval_read(db, Cmd::Mul(lhs.into(), rhs.into()))
    }

    fn add<S: Into<String>>(db: &InMemDb, lhs: S, rhs: S) -> Res {
        eval_read(db, Cmd::Add(lhs.into(), rhs.into()))
    }

    fn sub<S: Into<String>>(db: &InMemDb, lhs: S, rhs: S) -> Res {
        eval_read(db, Cmd::Sub(lhs.into(), rhs.into()))
    }

    fn len(db: &InMemDb) -> usize {
        db.cache.len()
    }

    fn set<S: Into<String>>(db: &mut InMemDb, key: S, val: Json) {
        eval_write(db, Cmd::Set(key.into(), val)).unwrap();
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
        assert!(len(&db) >= 10);
        assert_eq!(get(&db, "b"), Ok(Json::Bool(true)));
        assert_eq!(
            get(&db, "ia"),
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5),
            ]))
        );
        assert_eq!(get(&db, "i"), Ok(Json::from(2)));
        assert_eq!(get(&db, "f"), Ok(Json::from(3.3)));
        assert_eq!(
            get(&db, "fa"),
            Ok(Json::Array(vec![
                Json::from(1.1),
                Json::from(2.2),
                Json::from(3.3),
                Json::from(4.4),
                Json::from(5.5),
            ]))
        );
        assert_eq!(get(&db, "f"), Ok(Json::from(3.3)));
        assert_eq!(get(&db, "s"), Ok(Json::from("hello")));
        assert_eq!(
            get(&db, "sa"),
            Ok(Json::Array(vec![
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
        assert_eq!(Ok(Json::Bool(true)), get(&db, "b"));
        assert_eq!(Ok(Json::Bool(true)), get(&db, "b"));
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5)
            ])),
            get(&db, "ia")
        );
        assert_eq!(Ok(Json::from(2)), get(&db, "i"));
    }

    #[test]
    fn test_first() {
        let db = test_db();
        assert_eq!(Ok(Json::Bool(true)), first(&db, "b"));
        assert_eq!(Ok(Json::Bool(true)), first(&db, "b"));
        assert_eq!(Ok(Json::from(3.3)), first(&db, "f"));
        assert_eq!(Ok(Json::from(2)), first(&db, "i"));
        assert_eq!(Ok(Json::from(1.1)), first(&db, "fa"));
        assert_eq!(Ok(Json::from(1)), first(&db, "ia"));
    }

    #[test]
    fn test_last() {
        let db = test_db();
        assert_eq!(Ok(Json::from(true)), last(&db, "b"));
        assert_eq!(Ok(Json::from(3.3)), last(&db, "f"));
        assert_eq!(Ok(Json::from(2)), last(&db, "i"));
        assert_eq!(Ok(Json::from(5.5)), last(&db, "fa"));
        assert_eq!(Ok(Json::from(5)), last(&db, "ia"));
    }

    #[test]
    fn test_max() {
        let db = test_db();
        assert_eq!(Ok(Json::Bool(true)), max(&db, "b"));
        assert_eq!(Ok(Json::from(2)), max(&db, "i"));
        assert_eq!(Ok(Json::from(3.3)), max(&db, "f"));
        assert_eq!(Ok(Json::from(5)), max(&db, "ia"));
        assert_eq!(Err(Error::FloatCmp), max(&db, "fa"));
    }

    #[test]
    fn test_min() {
        let db = test_db();
        assert_eq!(Ok(Json::Bool(true)), min(&db, "b"));
        assert_eq!(Ok(Json::from(2)), min(&db, "i"));
        assert_eq!(Ok(Json::from(3.3)), min(&db, "f"));
        assert_eq!(Err(Error::FloatCmp), min(&db, "fa"));
        assert_eq!(Ok(Json::from(1)), min(&db, "ia"));
    }

    #[test]
    fn test_avg() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), avg(&db, "f"));
        assert_eq!(Ok(Json::from(2)), avg(&db, "i"));
        assert_eq!(Ok(Json::from(3.3)), avg(&db, "fa"));
        assert_eq!(Ok(Json::from(3.0)), avg(&db, "ia"));
    }

    #[test]
    fn test_var() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), var(&db, "f"));
        assert_eq!(Ok(Json::from(2)), var(&db, "i"));
        let val = var(&db, "fa").unwrap();
        assert_approx_eq!(3.10, json_f64(&val), 0.0249f64);
        let val = var(&db, "ia").unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
    }

    #[test]
    fn test_dev() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), dev(&db, "f"));
        assert_eq!(Ok(Json::from(2)), dev(&db, "i"));
        let val = dev(&db, "fa").unwrap();
        assert_approx_eq!(1.55, json_f64(&val), 0.03f64);
        let val = dev(&db, "ia").unwrap();
        assert_approx_eq!(1.414, json_f64(&val), 0.03f64);
    }

    #[test]
    fn test_add() {
        let db = test_db();
        assert_eq!(Ok(Json::from(9)), add(&db, "x", "y"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
                Json::from(8.0),
                Json::from(9.0),
            ])),
            add(&db, "x", "ia")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(4.0),
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
            ])),
            add(&db, "ia", "i")
        );

        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("ahello"),
                Json::from("bhello"),
                Json::from("chello"),
                Json::from("dhello"),
            ])),
            add(&db, "sa", "s")
        );
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("helloa"),
                Json::from("hellob"),
                Json::from("helloc"),
                Json::from("hellod"),
            ])),
            add(&db, "s", "sa")
        );

        assert_eq!(Ok(Json::from("hellohello")), add(&db, "s", "s"));
        assert_eq!(bad_type(), add(&db, "s", "f"));
        assert_eq!(bad_type(), add(&db, "f", "s"));
        assert_eq!(bad_type(), add(&db, "i", "s"));
        assert_eq!(bad_type(), add(&db, "s", "i"));
    }

    #[test]
    fn test_sub() {
        let db = test_db();
        assert_eq!(Ok(Json::from(-1.0)), sub(&db, "x", "y"));
        assert_eq!(Ok(Json::from(1.0)), sub(&db, "y", "x"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.0),
                Json::from(-1.0),
            ])),
            sub(&db, "x", "ia")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(-4.0),
                Json::from(-3.0),
                Json::from(-2.0),
                Json::from(-1.0),
                Json::from(0.0),
            ])),
            sub(&db, "ia", "y")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
            ])),
            sub(&db, "ia", "ia")
        );

        assert_eq!(bad_type(), sub(&db, "s", "s"));
        assert_eq!(bad_type(), sub(&db, "sa", "s"));
        assert_eq!(bad_type(), sub(&db, "s", "sa"));
        assert_eq!(bad_type(), sub(&db, "i", "s"));
        assert_eq!(bad_type(), sub(&db, "s", "i"));
    }

    #[test]
    fn json_mul() {
        let db = test_db();
        assert_eq!(Ok(Json::from(20.0)), mul(&db, "x", "y"));
        assert_eq!(Ok(Json::from(16.0)), mul(&db, "x", "x"));
        let arr = vec![
            Json::from(5.0),
            Json::from(10.0),
            Json::from(15.0),
            Json::from(20.0),
            Json::from(25.0),
        ];
        assert_eq!(Ok(Json::from(arr.clone())), mul(&db, "ia", "y"));
        assert_eq!(Ok(Json::from(arr)), mul(&db, "y", "ia"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(4.0),
                Json::from(9.0),
                Json::from(16.0),
                Json::from(25.0),
            ])),
            mul(&db, "ia", "ia")
        );
        assert_eq!(bad_type(), mul(&db, "s", "s"));
        assert_eq!(bad_type(), mul(&db, "sa", "s"));
        assert_eq!(bad_type(), mul(&db, "s", "sa"));
        assert_eq!(bad_type(), mul(&db, "i", "s"));
        assert_eq!(bad_type(), mul(&db, "s", "i"));
    }

    #[test]
    fn json_div() {
        let db = test_db();
        assert_eq!(Ok(Json::from(1.0)), div(&db, "x", "x"));
        assert_eq!(Ok(Json::from(1.0)), div(&db, "y", "y"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
            ])),
            div(&db, "ia", "ia")
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.5),
                Json::from(1.0),
                Json::from(1.5),
                Json::from(2.0),
                Json::from(2.5),
            ])),
            div(&db, "ia", "i")
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.6666666666666666),
                Json::from(0.5),
                Json::from(0.4),
            ])),
            div(&db, "i", "ia")
        );

        assert_eq!(bad_type(), div(&db, "s", "s"));
        assert_eq!(bad_type(), div(&db, "sa", "s"));
        assert_eq!(bad_type(), div(&db, "s", "sa"));
        assert_eq!(bad_type(), div(&db, "i", "s"));
        assert_eq!(bad_type(), div(&db, "s", "i"));
    }

    #[test]
    fn eval_get_ok() {
        let mut db = test_db();
        let val = Json::from(vec![
            Json::from(1),
            Json::from(2),
            Json::from(3),
            Json::from(4),
            Json::from(5),
        ]);
        set(&mut db, "a", val.clone());
        assert_eq!(Ok(val), get(&db, "a"));
    }

    #[test]
    fn eval_get_string_err_not_found() {
        let db = test_db();
        assert_eq!(Err(Error::UnknownKey("ania".to_string())), get(&db, "ania"));
    }

    #[test]
    fn eval_append_obj() {
        let mut db = test_db();
        set(&mut db, "anna", json!({"name":"anna", "age": 28}));
        append(&mut db, "anna", json!({"email": "anna@gmail.com"}));
        assert_eq!(
            Ok(json!({"name":"anna", "age": 28, "email": "anna@gmail.com"})),
            get(&db, "anna")
        );
    }
}
