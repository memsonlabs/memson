use crate::json::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub enum Cmd {
    #[serde(rename = "append")]
    Append(String, Json),
    #[serde(rename = "get")]
    Get(String),
    #[serde(rename = "set")]
    Set(String, Json),
    #[serde(rename = "len")]
    Len(String),
    #[serde(rename = "max")]
    Max(String),
    #[serde(rename = "min")]
    Min(String),
    #[serde(rename = "avg")]
    Avg(String),
    #[serde(rename = "dev")]
    Dev(String),
    #[serde(rename = "sum")]
    Sum(String),
    #[serde(rename = "add")]
    Add(String, String),
    #[serde(rename = "sub")]
    Sub(String, String),
    #[serde(rename = "mul")]
    Mul(String, String),
    #[serde(rename = "div")]
    Div(String, String),
    #[serde(rename = "first")]
    First(String),
    #[serde(rename = "last")]
    Last(String),
    #[serde(rename = "var")]
    Var(String),
    #[serde(rename = "pop")]
    Pop(String),
}

impl Cmd {
    pub fn is_mutation(&self) -> bool {
        match self {
            Cmd::Append(_, _) | Cmd::Set(_, _) | Cmd::Pop(_) => true,
            Cmd::Get(_)
            | Cmd::Len(_)
            | Cmd::Max(_)
            | Cmd::Min(_)
            | Cmd::Dev(_)
            | Cmd::Avg(_)
            | Cmd::Mul(_, _)
            | Cmd::Div(_, _)
            | Cmd::Add(_, _)
            | Cmd::Sub(_, _)
            | Cmd::Sum(_)
            | Cmd::First(_)
            | Cmd::Last(_)
            | Cmd::Var(_) => false,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Error {
    BadType,
    BadState,
    EmptySequence,
    BadNumber,
    UnknownKey(String),
}

impl Error {
    pub fn to_string(&self) -> String {
        let msg = match self {
            Error::BadType => "incorrect type",
            Error::BadState => "incorrect internal state",
            Error::EmptySequence => "empty sequence",
            Error::BadNumber => "bad number",
            Error::UnknownKey(_) => "unknown key",
        };
        "error: ".to_string() + msg
    }
}

#[derive(Debug, PartialEq)]
pub enum Reply<'a> {
    Val(Json),
    Ref(&'a Json),
    Update,
    Insert,
}

impl<'a> Reply<'a> {
    pub fn to_string(&self) -> String {
        match self {
            Reply::Val(val) => val.to_string(),
            Reply::Ref(val) => val.to_string(),
            Reply::Update => "Updated entry ".to_string(),
            Reply::Insert => "Inserted entry ".to_string(),
        }
    }
}

pub type Res<'a> = Result<Reply<'a>, Error>;

#[derive(Debug)]
pub struct Db {
    cache: BTreeMap<String, Json>,
    store: sled::Db,
}

impl Db {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Db, Error> {
        let store = sled::open(path).expect("cannot open db");
        Ok(Self {
            cache: BTreeMap::new(),
            store,
        })
    }

    pub fn insert<K: Into<String>, V: Into<Json>>(&mut self, key: K, val: V) {
        self.cache.insert(key.into(), val.into());
    }

    fn eval_append(&mut self, key: String, arg: Json) -> Res<'_> {
        let val = self.get_mut(key)?;
        append(val, arg);
        Ok(Reply::Update)
    }

    pub fn eval_read_cmd(&self, cmd: Cmd) -> Res<'_> {
        match cmd {
            Cmd::Get(key) => self.eval_get(key),
            Cmd::Len(key) => self.eval_len(key),
            Cmd::Max(key) => self.eval_max(key),
            Cmd::Min(key) => self.eval_min(key),
            Cmd::Avg(key) => self.eval_avg(key),
            Cmd::Dev(key) => self.eval_dev(key),
            Cmd::Add(lhs, rhs) => self.eval_add(lhs, rhs),
            Cmd::Sub(lhs, rhs) => self.eval_sub(lhs, rhs),
            Cmd::Mul(lhs, rhs) => self.eval_mul(lhs, rhs),
            Cmd::Div(lhs, rhs) => self.eval_div(lhs, rhs),
            Cmd::Sum(key) => self.eval_sum(key),
            Cmd::First(key) => self.eval_first(key),
            Cmd::Last(key) => self.eval_last(key),
            Cmd::Var(key) => self.eval_var(key),
            Cmd::Set(_, _) | Cmd::Append(_, _) | Cmd::Pop(_) => Err(Error::BadState),
        }
    }

    pub fn eval_write_cmd(&mut self, cmd: Cmd) -> Res<'_> {
        match cmd {
            Cmd::Set(key, val) => self.eval_set(key, val),
            Cmd::Append(key, val) => self.eval_append(key, val),
            Cmd::Pop(key) => self.eval_pop(key),
            Cmd::Get(_)
            | Cmd::Len(_)
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
            | Cmd::Var(_) => Err(Error::BadState),
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

    fn eval_sum(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        sum(val).map(Reply::Val)
    }

    fn eval_min(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        min(val).map(Reply::Val)
    }

    fn eval_max(&self, key: String) -> Res<'_> {
        let val = self.get(key)?;
        max(val).map(Reply::Val)
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
            Reply::Insert
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

    fn get(&self, key: String) -> Result<&Json, Error> {
        self.cache.get(&key).ok_or(Error::UnknownKey(key))
    }

    fn get_mut(&mut self, key: String) -> Result<&mut Json, Error> {
        self.cache.get_mut(&key).ok_or(Error::UnknownKey(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_approx_eq::assert_approx_eq;

    type Res = Result<Json, Error>;

    fn add<S: Into<String>>(x: S, y: S) -> String {
        "{\"+\":[".to_string() + &x.into() + "," + &y.into() + "]}"
    }

    fn sub<S: Into<String>>(x: S, y: S) -> String {
        "{\"-\":[".to_string() + &x.into() + "," + &y.into() + "]}"
    }

    fn mul<S: Into<String>>(x: S, y: S) -> String {
        "{\"*\":[".to_string() + &x.into() + "," + &y.into() + "]}"
    }

    fn div<S: Into<String>>(x: S, y: S) -> String {
        "{\"/\":[".to_string() + &x.into() + "," + &y.into() + "]}"
    }

    fn get<S: Into<String>>(arg: S) -> String {
        "{\"get\":".to_string() + "\"" + &arg.into() + "\"" + "}"
    }

    fn first<S: Into<String>>(arg: S) -> String {
        json_fn("first", arg)
    }

    fn last<S: Into<String>>(arg: S) -> String {
        json_fn("last", arg)
    }

    fn max<S: Into<String>>(arg: S) -> String {
        json_fn("max", arg)
    }

    fn min<S: Into<String>>(arg: S) -> String {
        json_fn("min", arg)
    }

    fn avg<S: Into<String>>(arg: S) -> String {
        json_fn("avg", arg)
    }

    fn var<S: Into<String>>(arg: S) -> String {
        json_fn("var", arg)
    }

    fn dev<S: Into<String>>(arg: S) -> String {
        json_fn("dev", arg)
    }

    fn json_fn<S: Into<String>>(f: &str, arg: S) -> String {
        "{\"".to_string() + f + "\":" + &arg.into() + "}"
    }

    fn test_db() -> Db {
        Db::open("test.memson").unwrap()
    }

    fn json_f64_reply<'a>(v: &Reply<'_>) -> f64 {
        match v {
            Reply::Val(ref val) => json_f64(val),
            Reply::Ref(ref val) => json_f64(val),
            _ => panic!("bad state"), 
        }
    }

    fn json_f64(v: &Json) -> f64 {
        v.as_f64().unwrap()
    }

    fn json_val(r: Reply<'_>) -> Json {
        match r {
            Reply::Val(val) => val.clone(),
            Reply::Ref(val) => val.clone(),
            _ => panic!("bad state"),
        }
    }

    fn eval_read(db: &Db, cmd: Cmd) -> Res {
        db.eval_read_cmd(cmd).map(json_val)
    }

    fn eval_write(db: &mut Db, cmd: Cmd) -> Res {
        db.eval_write_cmd(cmd).map(json_val)
    }

    fn db_get<S: Into<String>>(db: &Db, key: S) -> Res {
        eval_read(db, Cmd::Get(key.into()))
    }

    fn db_first<S: Into<String>>(db: &Db, key: S) -> Res {
        eval_read(db, Cmd::First(key.into()))
    }

    fn db_last<S: Into<String>>(db: &Db, key: S) -> Res {
        eval_read(db, Cmd::Last(key.into()))
    }

    fn db_max<S: Into<String>>(db: &Db, key: S) -> Res {
        eval_read(db, Cmd::Max(key.into()))
    }

    fn db_min<S: Into<String>>(db: &Db, key: S) -> Res {
        eval_read(db, Cmd::Min(key.into()))
    }

    fn db_var<S: Into<String>>(db: &Db, key: S) -> Res {
        eval_read(db, Cmd::Var(key.into()))
    }

    fn db_dev<S: Into<String>>(db: &Db, key: S) -> Res {
        eval_read(db, Cmd::Dev(key.into()))
    }

    fn db_avg<S: Into<String>>(db: &Db, key: S) -> Res {
        eval_read(db, Cmd::Avg(key.into()))
    }

    fn db_div<S: Into<String>>(db: &Db, lhs: S, rhs: S) -> Res {
        eval_read(db, Cmd::Div(lhs.into(), rhs.into()))
    }

    fn db_mul<S: Into<String>>(db: &Db, lhs: S, rhs: S) -> Res {
        eval_read(db, Cmd::Mul(lhs.into(), rhs.into()))
    }  
    
    fn db_add<S: Into<String>>(db: &Db, lhs: S, rhs: S) -> Res {
        eval_read(db, Cmd::Add(lhs.into(), rhs.into()))
    } 
    
    fn db_sub<S: Into<String>>(db: &Db, lhs: S, rhs: S) -> Res {
        eval_read(db, Cmd::Sub(lhs.into(), rhs.into()))
    }     

    fn bad_type<'a>() -> Res {
        Err(Error::BadType)
    }

    #[test]
    fn open_db() {
        let mut db = test_db();
        assert_eq!(10, db.cache.len());
        assert_eq!(db_get(&mut db, "b"), Ok(Json::Bool(true)));
        assert_eq!(
            db_get(&mut db, "ia"),
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5),
            ]))
        );
        assert_eq!(db_get(&mut db, "i"), Ok(Json::from(3)));
        assert_eq!(db_get(&mut db, "f"), Ok(Json::from(3.3)));
        assert_eq!(
            db_get(&mut db, "fa"),
            Ok(Json::Array(vec![
                Json::from(1.0),
                Json::from(2.0),
                Json::from(3.0),
                Json::from(4.0),
                Json::from(5.0),
            ]))
        );
        assert_eq!(db_get(&mut db, "f"), Ok(Json::from(3.3)));
        assert_eq!(db_get(&mut db, "s"), Ok(Json::from("hello")));
        assert_eq!(
            db_get(&mut db, "sa"),
            Ok(Json::Array(vec![
                Json::from("a"),
                Json::from("b"),
                Json::from("c"),
                Json::from("d"),
            ]))
        );
        assert_eq!(db_get(&mut db, "z"), Ok(Json::from(2.0)));
    }

    #[test]
    fn test_get() {
        let mut db = test_db();
        let val = db_get(&db, "b");
        assert_eq!(Ok(Json::Bool(true)), val);

        let val = db_get(&db, "b");
        assert_eq!(Ok(Json::Bool(true)), val);

        let val = db_get(&db, "ia");
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5)
            ])),
            val
        );

        let val = db_get(&db, "i");
        assert_eq!(Ok(Json::from(3)), val);
    }

    #[test]
    fn test_first() {
        let db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db_first(&db, "b"));
        assert_eq!(Ok(Json::Bool(true)), db_first(&db, "b"));
        assert_eq!(Ok(Json::from(3.3)), db_first(&db, "f"));
        assert_eq!(Ok(Json::from(3)), db_first(&db, "i"));
        assert_eq!(Ok(Json::from(1.0)), db_first(&db, "fa"));
        assert_eq!(Ok(Json::from(1)), db_first(&db, "ia"));
    }

    #[test]
    fn test_last() {
        let db = test_db();
        assert_eq!(Ok(Json::from(true)), db_last(&db, "b"));
        assert_eq!(Ok(Json::from(3.3)), db_last(&db, "f"));
        assert_eq!(Ok(Json::from(3)), db_last(&db, "i"));
        assert_eq!(Ok(Json::from(5.0)), db_last(&db, "fa"));
        assert_eq!(Ok(Json::from(5)), db_last(&db, "ia"));
    }

    #[test]
    fn test_max() {
        let db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db_max(&db, "b"));
        assert_eq!(Ok(Json::from(3)), db_max(&db, "i"));
        assert_eq!(Ok(Json::from(3.3)), db_max(&db, "f"));
        assert_eq!(Ok(Json::from(5)), db_max(&db, "ia"));
        assert_eq!(Ok(Json::from(5.0)), db_max(&db, "fa"));
    }

    #[test]
    fn test_min() {
        let mut db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db_min(&db, "b"));
        assert_eq!(Ok(Json::from(3)), db_min(&db, "i"));
        assert_eq!(Ok(Json::from(3.3)), db_min(&db, "f"));
        assert_eq!(Ok(Json::from(1.0)), db_min(&db, "fa"));
        assert_eq!(Ok(Json::from(1.0)), db_min(&db, "ia"));
    }

    #[test]
    fn test_avg() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db_avg(&db, "f"));
        assert_eq!(Ok(Json::from(3)), db_avg(&db, "i"));
        assert_eq!(Ok(Json::from(3.0)), db_avg(&db, "fa"));
        assert_eq!(Ok(Json::from(3.0)), db_avg(&db, "ia"));
    }

    #[test]
    fn test_var() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db_var(&db, "f"));
        assert_eq!(Ok(Json::from(3)), db_var(&db, "i"));
        let val = db_var(&db, "fa").unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
        let val = db_var(&db, "ia").unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
    }

    #[test]
    fn test_dev() {
        let db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db_dev(&db, "f"));
        assert_eq!(Ok(Json::from(3)), db_dev(&db, "i"));
        let val = db_dev(&db, "fa").unwrap();
        assert_approx_eq!(1.4, json_f64(&val), 0.0249f64);
        let val = db_dev(&db,  "ia").unwrap();
        assert_approx_eq!(1.4, json_f64(&val), 0.0249f64);
    }

    #[test]
    fn test_add() {
        let db = test_db();
        assert_eq!(Ok(Json::from(9.0)), db_add(&db, "x", "y"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
                Json::from(8.0),
                Json::from(9.0),
            ])),
            db_add(&db, "x", "ia")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(6.0),
                Json::from(7.0),
                Json::from(8.0),
                Json::from(9.0),
                Json::from(10.0),
            ])),
            db_add(&db, "ia", "y")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(4.0),
                Json::from(6.0),
                Json::from(8.0),
                Json::from(10.0),
            ])),
            db_add(&db, "ia", "y")
        );

        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("ahello"),
                Json::from("bhello"),
                Json::from("chello"),
                Json::from("dhello"),
            ])),
            db_add(&db, "sa", "s")
        );
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("helloa"),
                Json::from("hellob"),
                Json::from("helloc"),
                Json::from("hellod"),
            ])),
            db_add(&db, "s", "sa")
        );

        assert_eq!(Ok(Json::from("hellohello")), db_add(&db, "s", "s"));
        assert_eq!(bad_type(), db_add(&db, "s", "f"));
        assert_eq!(bad_type(), db_add(&db, "f", "s"));
        assert_eq!(bad_type(), db_add(&db, "i", "s"));
        assert_eq!(bad_type(), db_add(&db, "s", "i"));
    }

    #[test]
    fn test_sub() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(-1.0)), db_sub(&db, "x", "y"));
        assert_eq!(Ok(Json::from(1.0)), db_sub(&db, "y", "x") );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.0),
                Json::from(-1.0),
            ])),
            db_sub(&db, "x", "ia")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(-4.0),
                Json::from(-3.0),
                Json::from(-2.0),
                Json::from(-1.0),
                Json::from(0.0),
            ])),
            db_sub(&db, "ia", "y")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
            ])),
            db_sub(&db, "ia", "ia")
        );

        assert_eq!(bad_type(), db_sub(&db, "s", "s"));
        assert_eq!(bad_type(), db_sub(&db, "sa", "s"));
        assert_eq!(bad_type(), db_sub(&db, "s", "sa"));
        assert_eq!(bad_type(), db_sub(&db, "i", "s"));
        assert_eq!(bad_type(), db_sub(&db, "s", "i"));
    }

    #[test]
    fn json_mul() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(20.0)), db_mul(&db, "x", "y"));
        assert_eq!(Ok(Json::from(16.0)), db_mul(&db, "x", "x"));
        let arr = vec![
            Json::from(5.0),
            Json::from(10.0),
            Json::from(15.0),
            Json::from(20.0),
            Json::from(25.0),
        ];
        assert_eq!(
            Ok(Json::from(arr.clone())),
            db_mul(&db, "ia", "y")
        );
        assert_eq!(Ok(Json::from(arr)), db_mul(&db, "y", "ia"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(4.0),
                Json::from(9.0),
                Json::from(16.0),
                Json::from(25.0),
            ])),
            db_mul(&db, "ia", "ia")
        );
        assert_eq!(bad_type(), db_mul(&db, "s", "s"));
        assert_eq!(bad_type(), db_mul(&db, "sa", "s"));
        assert_eq!(bad_type(), db_mul(&db, "s", "sa"));
        assert_eq!(bad_type(), db_mul(&db, "i", "s"));
        assert_eq!(bad_type(), db_mul(&db, "s", "i"));
    }

    #[test]
    fn json_div() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(1.0)), db_div(&db, "x", "x"));
        assert_eq!(Ok(Json::from(1.0)), db_div(&db, "y", "y"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
            ])),
            db_div(&db, "ia", "ia")
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.5),
                Json::from(1.0),
                Json::from(1.5),
                Json::from(2.0),
                Json::from(2.5),
            ])),
            db_div(&db, "ia", "z")
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.6666666666666666),
                Json::from(0.5),
                Json::from(0.4),
            ])),
            db_div(&db, "z", "ia")
        );

        assert_eq!(bad_type(), db_div(&db, "s", "s"));
        assert_eq!(bad_type(), db_div(&db, "sa", "s"));
        assert_eq!(bad_type(), db_div(&db, "s", "sa"));
        assert_eq!(bad_type(), db_div(&db, "i", "s"));
        assert_eq!(bad_type(), db_div(&db, "s", "i"));
    }

    #[test]
    fn eval_get_ok() {
        let mut db = Db::open("test.memson").unwrap();
        let vec = vec![1, 2, 3, 4, 5];
        db.insert("a", vec.clone());
        let val = Json::from(vec);
        let exp = Reply::Ref(&val);
        assert_eq!(Ok(exp), db.eval_read_cmd(Cmd::Get("a".to_string())));
    }

    #[test]
    fn eval_get_string_err_not_found() {
        let mut db = Db::open("test.memson").unwrap();
        db.insert("a".to_string(), Json::from(1));
        assert_eq!(
            Err(Error::UnknownKey("b".to_string())),
            db.eval_read_cmd(Cmd::Get("b".to_string()))
        );
    }

    #[test]
    fn eval_append_ok() {
        let mut db = Db::open("test.memson").unwrap();
        db.insert("a", vec![1, 2, 3, 4, 5]);
        assert_eq!(
            Ok(Reply::Update),
            db.eval_write_cmd(Cmd::Append("a".to_string(), Json::from(6)))
        );
        let val = Json::from(vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(
            Ok(Reply::Ref(&val)),
            db.eval_read_cmd(Cmd::Get("a".to_string()))
        );
    }
}
