use crate::json::*;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::collections::BTreeMap;

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

#[derive(Debug, Default, PartialEq)]
pub struct Db {
    data: BTreeMap<String, Json>,
}

impl Db {
    pub fn new() -> Db {
        Self::default()
    }

    pub fn insert<K: Into<String>, V: Into<Json>>(&mut self, key: K, val: V) {
        self.data.insert(key.into(), val.into());
    }

    fn eval_append(&mut self, key: String, arg: Json) -> Res<'_> {
        let val = self.get_mut(key)?;
        append(val, arg)
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
        match self.data.get(&key) {
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
        let reply = if let Some(_) = self.data.insert(key, val) {
            Reply::Update
        } else {
            Reply::Insert
        };
        Ok(reply)
    }

    fn eval_var(&self, key: String) -> Res<'_> {
        match self.data.get(&key) {
            Some(val) => var(val).map(Reply::Val),
            None => Err(Error::UnknownKey(key)),
        }
    }

    fn eval_bin_fn(
        &self,
        lhs: String,
        rhs: String,
        f: &dyn Fn(&Json, &Json) -> Result<Json, Error>,
    ) -> Res<'_> {
        let lhs = match self.data.get(&lhs) {
            Some(val) => val,
            None => return Err(Error::UnknownKey(lhs)),
        };
        let rhs = match self.data.get(&rhs) {
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
        self.data.get(&key).ok_or(Error::UnknownKey(key))
    }

    /*
    fn eval_obj(&mut self, obj: Map<String, Json>) -> Res<'_> {
        for (key, val) in obj.into_iter().take(1) {
            return self.eval_cmd_obj(key, val)
        }
        Err(Error::BadCmd)
    }

    fn eval_cmd_obj(&mut self, key: String, val: Json) -> Res<'_> {
        match key.as_ref() {
            "append" => self.eval_append(key, val),
            "get" => self.eval_get(val),
            "set" => self.eval_set(val),
            "sum" => self.eval_sum(val),
            "first" => self.eval_first(val),
            "last" => self.eval_last(val),
            "pop" => self.eval_pop(val),
            _ => Err(Error::BadCmd),
        }
    }

    fn eval_pop(&mut self, arg: Json) -> Res<'_> {
        match arg {
            Json::String(key) => {
                match self.data.get_mut(&key) {
                    Some(val) => pop(val),
                    None => Err(Error::EmptySequence),
                }
            }
            _ => Err(Error::BadArg),
        }
    }

    fn eval_sum(&mut self, arg: Json) -> Res<'_> {
        match arg {
            Json::String(key) => {
                match self.get(key)? {
                    Reply::Val(ref val) => sum(val).map(Reply::Val),
                    Reply::Ref(val) => sum(val).map(Reply::Val),
                    _ => Err(Error::BadArg)
                }
            }
            Json::Object(obj) => {
                let reply = self.eval_obj(obj)?;
                match reply {
                    Reply::Val(ref val) => sum(val).map(Reply::Val),
                    Reply::Ref(val) => sum(val).map(Reply::Val),
                    _ => Err(Error::BadCmd),
                }
            }
            _ => unimplemented!(),
        }
    }

    fn eval_first(&mut self, arg: Json) -> Res<'_> {
        match arg {
            Json::String(key) => {
                let r = self.data.get(&key);
                match r {
                    Some(val) => first(val),
                    None => Err(Error::UnknownKey(key)),
                }
            }
            _ => unimplemented!(),
        }
    }

    fn eval_last(&mut self, arg: Json) -> Res<'_> {
        match arg {
            Json::String(key) => {
                let r = self.data.get(&key);
                match r {
                    Some(val) => last(val),
                    None => Err(Error::UnknownKey(key)),
                }
            }
            _ => unimplemented!(),
        }
    }

    fn eval_set(&mut self, arg: Json) -> Res<'_> {
        match arg {
            Json::Array(mut arr) => {
                if arr.len() != 2 {
                    return Err(Error::BadArg);
                }
                let val = arr.remove(1);
                let key = arr.remove(0);
                self.put(key, val)
            }
            _ => unimplemented!(),
        }
    }*/

    fn get_mut(&mut self, key: String) -> Result<&mut Json, Error> {
        self.data.get_mut(&key).ok_or(Error::UnknownKey(key))
    }
}

#[test]
fn eval_get_ok() {
    let mut db = Db::default();
    let vec = vec![1, 2, 3, 4, 5];
    db.insert("a", vec.clone());
    let val = Json::from(vec);
    let exp = Reply::Ref(&val);
    assert_eq!(Ok(exp), db.eval_read_cmd(Cmd::Get("a".to_string())));
}

#[test]
fn eval_get_string_err_not_found() {
    let mut db = Db::default();
    db.insert("a".to_string(), Json::from(1));
    assert_eq!(
        Err(Error::UnknownKey("b".to_string())),
        db.eval_read_cmd(Cmd::Get("b".to_string()))
    );
}

#[test]
fn eval_append_ok() {
    let mut db = Db::default();
    db.insert("a", vec![1, 2, 3, 4, 5]);
    assert_eq!(
        Ok(Reply::Update),
        db.eval_write_cmd(Cmd::Append("a".to_string(), Json::from(6)))
    );
    let val = Json::from(vec![1,2,3,4,5,6]);
    assert_eq!(
        Ok(Reply::Ref(&val)),
        db.eval_read_cmd(Cmd::Get("a".to_string()))
    );
}

/*
use crate::json::*;
use crate::replay::*;
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::io::{self};
use std::path::Path;

// Type wrapper
pub type Cache = BTreeMap<String, Json>;

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
pub struct Database {
    cache: Cache,
    log: ReplayLog,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Res<Database> {
        let mut log = ReplayLog::open(path).map_err(|err| {
            eprintln!("{:?}", err);
            "bad io"
        })?;
        let cache = log.replay()?;
        Ok(Database { cache, log })
    }

    pub fn set(&mut self, key: String, val: Json) -> io::Result<Option<Json>> {
        self.log.write(&key, &val)?;
        Ok(self.cache.insert(key, val))
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        self.cache.get(key)
    }

    pub fn del(&mut self, key: &str) -> io::Result<Option<Json>> {
        self.log.remove(&key)?;
        Ok(self.cache.remove(key))
    }

    pub fn eval<S: Into<String>>(&mut self, line: S) -> Res<Json> {
        let line = line.into();
        let json_val: Cmd = parse_json_str(line)?;
        eval_json_cmd(json_val, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_approx_eq::assert_approx_eq;

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

    fn test_db() -> Database {
        Database::open("test.memson").unwrap()
    }

    fn json_f64(v: &Json) -> f64 {
        v.as_f64().unwrap()
    }

    fn eval<'a, S: Into<String>>(db: &'a mut Database, line: S) -> Res<Json> {
        db.eval(line)
    }

    fn db_get(db: &mut Database, key: &str) -> Res<Json> {
        db.eval(get(key))
    }

    fn bad_type() -> Res<Json> {
        Err("bad type")
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
        let val = eval(&mut db, get("b"));
        assert_eq!(Ok(Json::Bool(true)), val);

        let val = eval(&mut db, get("b"));
        assert_eq!(Ok(Json::Bool(true)), val);

        let val = db.eval(get("ia"));
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

        let val = db.eval(get("i"));
        assert_eq!(Ok(Json::from(3)), val);
    }

    #[test]
    fn test_first() {
        let mut db = test_db();
        assert_eq!(Ok(Json::Bool(true)), eval(&mut db, first(get("b"))));
        let val = db.eval(first(get("b")));
        assert_eq!(Ok(Json::Bool(true)), val);
        let val = db.eval(first(get("f")));
        assert_eq!(Ok(Json::from(3.3)), val);
        let val = db.eval(first(get("i")));
        assert_eq!(Ok(Json::from(3)), val);
        let val = db.eval(first(get("fa")));
        assert_eq!(Ok(Json::from(1.0)), val);
        let val = db.eval(first(get("ia")));
        assert_eq!(Ok(Json::from(1)), val);
    }

    #[test]
    fn test_last() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(true)), eval(&mut db, last(get("b"))));
        let val = db.eval(last(get("b")));
        assert_eq!(Ok(Json::from(true)), val);
        let val = db.eval(last(get("f")));
        assert_eq!(Ok(Json::from(3.3)), val);
        let val = db.eval(last(get("i")));
        assert_eq!(Ok(Json::from(3)), val);
        let val = db.eval(last(get("fa")));
        assert_eq!(Ok(Json::from(5.0)), val);
        let val = db.eval(last(get("ia")));
        assert_eq!(Ok(Json::from(5)), val);
    }

    #[test]
    fn test_max() {
        let mut db = test_db();
        let val = eval(&mut db, max(get("b")));
        assert_eq!(Ok(Json::Bool(true)), val);
        let val = eval(&mut db, max(get("b")));
        assert_eq!(Ok(Json::Bool(true)), val);
        let val = eval(&mut db, max(get("i")));
        assert_eq!(Ok(Json::from(3)), val);
        let val = eval(&mut db, max(get("f")));
        assert_eq!(Ok(Json::from(3.3)), val);
        let val = eval(&mut db, max(get("ia")));
        assert_eq!(Ok(Json::from(5.0)), val);
        let val = eval(&mut db, max(get("fa")));
        assert_eq!(Ok(Json::from(5.0)), val);
    }

    #[test]
    fn test_min() {
        let mut db = test_db();
        let val = eval(&mut db, min(get("b")));
        assert_eq!(Ok(Json::Bool(true)), val);
        let val = eval(&mut db, min(get("i")));
        assert_eq!(Ok(Json::from(3)), val);
        let val = eval(&mut db, min(get("f")));
        assert_eq!(Ok(Json::from(3.3)), val);
        let val = eval(&mut db, min(get("fa")));
        assert_eq!(Ok(Json::from(1.0)), val);
        let val = eval(&mut db, min(get("ia")));
        assert_eq!(Ok(Json::from(1.0)), val);
    }

    #[test]
    fn test_avg() {
        let mut db = test_db();
        let val = eval(&mut db, avg(get("f")));
        assert_eq!(Ok(Json::from(3.3)), val);
        let val = db.eval(&json_fn("avg", get("i")));
        assert_eq!(Ok(Json::from(3)), val);
        let val = db.eval(&json_fn("avg", get("fa")));
        assert_eq!(Ok(Json::from(3.0)), val);
        let val = db.eval(&json_fn("avg", get("ia")));
        assert_eq!(Ok(Json::from(3.0)), val);
    }

    #[test]
    fn test_var() {
        let mut db = test_db();
        let val = eval(&mut db, var(get("f")));
        assert_eq!(Ok(Json::from(3.3)), val);
        let val = db.eval(var(get("i")));
        assert_eq!(Ok(Json::from(3)), val);
        let val = db.eval(var(get("fa"))).unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
        let val = db.eval(var(get("ia"))).unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
    }
    #[test]
    fn test_dev() {
        let mut db = test_db();
        let val = eval(&mut db, dev(get("f")));
        assert_eq!(Ok(Json::from(3.3)), val);
        let val = eval(&mut db, dev(get("i")));
        assert_eq!(Ok(Json::from(3)), val);
        let val = eval(&mut db, dev(get("fa"))).unwrap();
        assert_approx_eq!(1.4, json_f64(&val), 0.0249f64);
        let val = eval(&mut db, dev(get("ia"))).unwrap();
        assert_approx_eq!(1.4, json_f64(&val), 0.0249f64);
    }

    #[test]
    fn test_add() {
        let mut db = test_db();
        assert_eq!(
            Ok(Json::from(9.0)),
            eval(&mut db, add(get("x"), get("y")))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
                Json::from(8.0),
                Json::from(9.0),
            ])),
            eval(&mut db, add(get("x"), get("ia")))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(6.0),
                Json::from(7.0),
                Json::from(8.0),
                Json::from(9.0),
                Json::from(10.0),
            ])),
            eval(&mut db, add(get("ia"), get("y")))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(4.0),
                Json::from(6.0),
                Json::from(8.0),
                Json::from(10.0),
            ])),
            eval(&mut db, add(get("ia"), get("ia")))
        );
        assert_eq!(Ok(Json::Array(vec![
           Json::from("ahello"),
           Json::from("bhello"),
           Json::from("chello"),
           Json::from("dhello"),
        ])), db.eval(add(get("sa"), get("s"))));
        assert_eq!(Ok(Json::Array(vec![
           Json::from("helloa"),
           Json::from("hellob"),
           Json::from("helloc"),
           Json::from("hellod"),
        ])), db.eval(add(get("s"), get("sa"))));
        assert_eq!(Ok(Json::from("hellohello")), db.eval(add(get("s"), get("s"))));
        assert_eq!(bad_type(), db.eval(add(get("s"), get("f"))));
        assert_eq!(bad_type(), db.eval(add(get("f"), get("s"))));
        assert_eq!(bad_type(), db.eval(add(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(add(get("s"), get("i"))));

    }

    #[test]
    fn test_sub() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(-1.0)), db.eval(sub(get("x"), get("y"))));
        assert_eq!(Ok(Json::from(1.0)), db.eval(sub(get("y"), get("x"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.0),
                Json::from(-1.0),
            ])),
            db.eval(sub(get("x"), get("ia")))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(-4.0),
                Json::from(-3.0),
                Json::from(-2.0),
                Json::from(-1.0),
                Json::from(0.0),
            ])),
            db.eval(sub(get("ia"), get("y")))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
            ])),
            db.eval(sub(get("ia"), get("ia")))
        );

        assert_eq!(bad_type(), db.eval(sub(get("s"), get("s"))));
        assert_eq!(bad_type(), db.eval(sub(get("sa"), get("s"))));
        assert_eq!(bad_type(), db.eval(sub(get("s"), get("sa"))));
        assert_eq!(bad_type(), db.eval(sub(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(sub(get("s"), get("i"))));
    }

    #[test]
    fn json_mul() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(20.0)), db.eval(mul(get("x"), get("y"))));
        assert_eq!(Ok(Json::from(16.0)), db.eval(mul(get("x"), get("x"))));
        let arr = vec![
            Json::from(5.0),
            Json::from(10.0),
            Json::from(15.0),
            Json::from(20.0),
            Json::from(25.0),
        ];
        assert_eq!(
            Ok(Json::from(arr.clone())),
            db.eval(mul(get("ia"), get("y")))
        );
        assert_eq!(Ok(Json::from(arr)), db.eval(mul(get("y"), get("ia"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(4.0),
                Json::from(9.0),
                Json::from(16.0),
                Json::from(25.0),
            ])),
            db.eval(mul(get("ia"), get("ia")))
        );
        assert_eq!(bad_type(), db.eval(mul(get("s"), get("s"))));
        assert_eq!(bad_type(), db.eval(mul(get("sa"), get("s"))));
        assert_eq!(bad_type(), db.eval(mul(get("s"), get("sa"))));
        assert_eq!(bad_type(), db.eval(mul(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(mul(get("s"), get("i"))));
    }

    #[test]
    fn json_div() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(1.0)), db.eval(div(get("x"), get("x"))));
        assert_eq!(Ok(Json::from(1.0)), db.eval(div(get("y"), get("y"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
            ])),
            db.eval(div(get("ia"), get("ia")))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.5),
                Json::from(1.0),
                Json::from(1.5),
                Json::from(2.0),
                Json::from(2.5),
            ])),
            db.eval(div(get("ia"), get("z")))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.6666666666666666),
                Json::from(0.5),
                Json::from(0.4),
            ])),
            db.eval(div(get("z"), get("ia")))
        );

        assert_eq!(bad_type(), db.eval(div(get("s"), get("s"))));
        assert_eq!(bad_type(), db.eval(div(get("sa"), get("s"))));
        assert_eq!(bad_type(), db.eval(div(get("s"), get("sa"))));
        assert_eq!(bad_type(), db.eval(div(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(div(get("s"), get("i"))));
    }
}
*/
