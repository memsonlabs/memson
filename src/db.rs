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
    BadIO,
    EmptySequence,
    BadNumber,
    UnknownKey(String),
}

impl Error {
    pub fn to_string(&self) -> String {
        let msg = match self {
            Error::BadType => "incorrect type",
            Error::BadState => "incorrect internal state",
            Error::BadIO => "bad i/o",
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
        let mut cache = BTreeMap::new();
        println!("len = {:?}", store.len());
        for r in store.iter() {
            match r {
                Ok((key, val)) => {
                    let key: String = unsafe { String::from_utf8_unchecked(key.to_vec()) };
                    println!("key = {:?}", key);
                    let val: Json = match serde_json::from_slice(&val) {
                        Ok(val) => val,
                        Err(_) => return Err(Error::BadIO),
                    };
                    println!("val = {:?}", val);
                    cache.insert(key, val);
                }
                Err(_) => return Err(Error::BadIO),
            }
        }
        Ok(Self { cache, store })
    }

    pub fn insert<K: Into<String>, V: Into<Json>>(&mut self, key: K, val: V) -> Result<(), Error> {
        let key = key.into();
        let val = val.into();
        if let Err(_) = self.store.insert(&key, val.to_string().as_bytes()) {
            return Err(Error::BadIO);
        }
        self.cache.insert(key, val);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.cache.len()
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
        let vec = match serde_json::to_vec(&val) {
            Ok(vec) => vec,
            Err(_) => return Err(Error::BadIO),
        };
        if let Err(_) = self.store.insert(&key, vec) {
            return Err(Error::BadIO);
        }
        match self.store.flush() {
            Ok(n) => println!("written {} bytes to disk", n),
            Err(_) => return Err(Error::BadIO),
        };
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
    use lazy_static::lazy_static;
    use serde_json::json;
    use std::sync::RwLock;

    lazy_static! {
        static ref DB: RwLock<Db> = {
            let mut db = Db::open("test").unwrap();
            db.insert(
                "a",
                Json::from(vec![
                    Json::from(1),
                    Json::from(2),
                    Json::from(3),
                    Json::from(4),
                    Json::from(5),
                ]),
            )
            .unwrap();
            db.insert("b", true).unwrap();
            db.insert("i", 2).unwrap();
            db.insert("f", 3.3).unwrap();
            db.insert(
                "ia",
                Json::from(
                    vec![1, 2, 3, 4, 5]
                        .into_iter()
                        .map(Json::from)
                        .collect::<Vec<Json>>(),
                ),
            )
            .unwrap();
            db.insert("fa", Json::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]))
                .unwrap();
            db.insert("x", 4).unwrap();
            db.insert("y", 5).unwrap();
            db.insert("s", "hello").unwrap();
            db.insert(
                "sa",
                Json::from(vec![
                    Json::from("a"),
                    Json::from("b"),
                    Json::from("c"),
                    Json::from("d"),
                ]),
            )
            .unwrap();
            RwLock::new(db)
        };
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

    fn eval_read(cmd: Cmd) -> Res {
        let db = DB.read().unwrap();
        db.eval_read_cmd(cmd).map(json_val)
    }

    fn eval_write(cmd: Cmd) -> Res {
        let mut db = DB.write().unwrap();
        db.eval_write_cmd(cmd).map(json_val)
    }

    fn get<S: Into<String>>(key: S) -> Res {
        eval_read(Cmd::Get(key.into()))
    }

    fn first<S: Into<String>>(key: S) -> Res {
        eval_read(Cmd::First(key.into()))
    }

    fn last<S: Into<String>>(key: S) -> Res {
        eval_read(Cmd::Last(key.into()))
    }

    fn max<S: Into<String>>(key: S) -> Res {
        eval_read(Cmd::Max(key.into()))
    }

    fn min<S: Into<String>>(key: S) -> Res {
        eval_read(Cmd::Min(key.into()))
    }

    fn var<S: Into<String>>(key: S) -> Res {
        eval_read(Cmd::Var(key.into()))
    }

    fn dev<S: Into<String>>(key: S) -> Res {
        eval_read(Cmd::Dev(key.into()))
    }

    fn avg<S: Into<String>>(key: S) -> Res {
        eval_read(Cmd::Avg(key.into()))
    }

    fn div<S: Into<String>>(lhs: S, rhs: S) -> Res {
        eval_read(Cmd::Div(lhs.into(), rhs.into()))
    }

    fn mul<S: Into<String>>(lhs: S, rhs: S) -> Res {
        eval_read(Cmd::Mul(lhs.into(), rhs.into()))
    }
    fn add<S: Into<String>>(lhs: S, rhs: S) -> Res {
        eval_read(Cmd::Add(lhs.into(), rhs.into()))
    }
    fn sub<S: Into<String>>(lhs: S, rhs: S) -> Res {
        eval_read(Cmd::Sub(lhs.into(), rhs.into()))
    }
    fn len() -> usize {
        let db = DB.read().unwrap();
        db.len()
    }

    fn append<S: Into<String>>(key: S, val: Json) -> Res {
        let mut db = DB.write().unwrap();
        db.eval_write_cmd(Cmd::Append(key.into(), val))
            .map(json_val)
    }

    fn set<S: Into<String>, J: Into<Json>>(key: S, val: J) -> Res {
        eval_write(Cmd::Set(key.into(), val.into()))
    }

    fn bad_type<'a>() -> Res {
        Err(Error::BadType)
    }

    #[test]
    fn open_db() {
        assert_eq!(11, len());
        assert_eq!(get("b"), Ok(Json::Bool(true)));
        assert_eq!(
            get("ia"),
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5),
            ]))
        );
        assert_eq!(get("i"), Ok(Json::from(2)));
        assert_eq!(get("f"), Ok(Json::from(3.3)));
        assert_eq!(
            get("fa"),
            Ok(Json::Array(vec![
                Json::from(1.1),
                Json::from(2.2),
                Json::from(3.3),
                Json::from(4.4),
                Json::from(5.5),
            ]))
        );
        assert_eq!(get("f"), Ok(Json::from(3.3)));
        assert_eq!(get("s"), Ok(Json::from("hello")));
        assert_eq!(
            get("sa"),
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
        assert_eq!(Ok(Json::Bool(true)), get("b"));
        assert_eq!(Ok(Json::Bool(true)), get("b"));
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5)
            ])),
            get("ia")
        );
        assert_eq!(Ok(Json::from(2)), get("i"));
    }

    #[test]
    fn test_first() {
        assert_eq!(Ok(Json::Bool(true)), first("b"));
        assert_eq!(Ok(Json::Bool(true)), first("b"));
        assert_eq!(Ok(Json::from(3.3)), first("f"));
        assert_eq!(Ok(Json::from(2)), first("i"));
        assert_eq!(Ok(Json::from(1.1)), first("fa"));
        assert_eq!(Ok(Json::from(1)), first("ia"));
    }

    #[test]
    fn test_last() {
        assert_eq!(Ok(Json::from(true)), last("b"));
        assert_eq!(Ok(Json::from(3.3)), last("f"));
        assert_eq!(Ok(Json::from(2)), last("i"));
        assert_eq!(Ok(Json::from(5.5)), last("fa"));
        assert_eq!(Ok(Json::from(5)), last("ia"));
    }

    #[test]
    fn test_max() {
        assert_eq!(Ok(Json::Bool(true)), max("b"));
        assert_eq!(Ok(Json::from(2)), max("i"));
        assert_eq!(Ok(Json::from(3.3)), max("f"));
        assert_eq!(Ok(Json::from(5.0)), max("ia"));
        assert_eq!(Ok(Json::from(5.5)), max("fa"));
    }

    #[test]
    fn test_min() {
        assert_eq!(Ok(Json::Bool(true)), min("b"));
        assert_eq!(Ok(Json::from(2)), min("i"));
        assert_eq!(Ok(Json::from(3.3)), min("f"));
        assert_eq!(Ok(Json::from(1.1)), min("fa"));
        assert_eq!(Ok(Json::from(1.0)), min("ia"));
    }

    #[test]
    fn test_avg() {
        assert_eq!(Ok(Json::from(3.3)), avg("f"));
        assert_eq!(Ok(Json::from(2)), avg("i"));
        assert_eq!(Ok(Json::from(3.3)), avg("fa"));
        assert_eq!(Ok(Json::from(3.0)), avg("ia"));
    }

    #[test]
    fn test_var() {
        assert_eq!(Ok(Json::from(3.3)), var("f"));
        assert_eq!(Ok(Json::from(2)), var("i"));
        let val = var("fa").unwrap();
        assert_approx_eq!(3.10, json_f64(&val), 0.0249f64);
        let val = var("ia").unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
    }

    #[test]
    fn test_dev() {
        assert_eq!(Ok(Json::from(3.3)), dev("f"));
        assert_eq!(Ok(Json::from(2)), dev("i"));
        let val = dev("fa").unwrap();
        assert_approx_eq!(1.55, json_f64(&val), 0.03f64);
        let val = dev("ia").unwrap();
        assert_approx_eq!(1.414, json_f64(&val), 0.03f64);
    }

    #[test]
    fn test_add() {
        assert_eq!(Ok(Json::from(9.0)), add("x", "y"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
                Json::from(8.0),
                Json::from(9.0),
            ])),
            add("x", "ia")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(4.0),
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
            ])),
            add("ia", "i")
        );

        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("ahello"),
                Json::from("bhello"),
                Json::from("chello"),
                Json::from("dhello"),
            ])),
            add("sa", "s")
        );
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("helloa"),
                Json::from("hellob"),
                Json::from("helloc"),
                Json::from("hellod"),
            ])),
            add("s", "sa")
        );

        assert_eq!(Ok(Json::from("hellohello")), add("s", "s"));
        assert_eq!(bad_type(), add("s", "f"));
        assert_eq!(bad_type(), add("f", "s"));
        assert_eq!(bad_type(), add("i", "s"));
        assert_eq!(bad_type(), add("s", "i"));
    }

    #[test]
    fn test_sub() {
        assert_eq!(Ok(Json::from(-1.0)), sub("x", "y"));
        assert_eq!(Ok(Json::from(1.0)), sub("y", "x"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.0),
                Json::from(-1.0),
            ])),
            sub("x", "ia")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(-4.0),
                Json::from(-3.0),
                Json::from(-2.0),
                Json::from(-1.0),
                Json::from(0.0),
            ])),
            sub("ia", "y")
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
            ])),
            sub("ia", "ia")
        );

        assert_eq!(bad_type(), sub("s", "s"));
        assert_eq!(bad_type(), sub("sa", "s"));
        assert_eq!(bad_type(), sub("s", "sa"));
        assert_eq!(bad_type(), sub("i", "s"));
        assert_eq!(bad_type(), sub("s", "i"));
    }

    #[test]
    fn json_mul() {
        assert_eq!(Ok(Json::from(20.0)), mul("x", "y"));
        assert_eq!(Ok(Json::from(16.0)), mul("x", "x"));
        let arr = vec![
            Json::from(5.0),
            Json::from(10.0),
            Json::from(15.0),
            Json::from(20.0),
            Json::from(25.0),
        ];
        assert_eq!(Ok(Json::from(arr.clone())), mul("ia", "y"));
        assert_eq!(Ok(Json::from(arr)), mul("y", "ia"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(4.0),
                Json::from(9.0),
                Json::from(16.0),
                Json::from(25.0),
            ])),
            mul("ia", "ia")
        );
        assert_eq!(bad_type(), mul("s", "s"));
        assert_eq!(bad_type(), mul("sa", "s"));
        assert_eq!(bad_type(), mul("s", "sa"));
        assert_eq!(bad_type(), mul("i", "s"));
        assert_eq!(bad_type(), mul("s", "i"));
    }

    #[test]
    fn json_div() {
        assert_eq!(Ok(Json::from(1.0)), div("x", "x"));
        assert_eq!(Ok(Json::from(1.0)), div("y", "y"));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
            ])),
            div("ia", "ia")
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.5),
                Json::from(1.0),
                Json::from(1.5),
                Json::from(2.0),
                Json::from(2.5),
            ])),
            div("ia", "i")
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.6666666666666666),
                Json::from(0.5),
                Json::from(0.4),
            ])),
            div("i", "ia")
        );

        assert_eq!(bad_type(), div("s", "s"));
        assert_eq!(bad_type(), div("sa", "s"));
        assert_eq!(bad_type(), div("s", "sa"));
        assert_eq!(bad_type(), div("i", "s"));
        assert_eq!(bad_type(), div("s", "i"));
    }

    #[test]
    fn eval_get_ok() {
        let val = Json::from(vec![
            Json::from(1),
            Json::from(2),
            Json::from(3),
            Json::from(4),
            Json::from(5),
        ]);
        set("a", val.clone()).unwrap();
        assert_eq!(Ok(val), get("a"));
    }

    #[test]
    fn eval_get_string_err_not_found() {
        assert_eq!(Err(Error::UnknownKey("ania".to_string())), get("ania"));
    }

    #[test]
    fn eval_append_obj() {
        set("anna", json!({"name":"anna", "age": 28})).unwrap();
        append("anna", json!({"email": "anna@gmail.com"})).unwrap();
        assert_eq!(
            Ok(json!({"name":"anna", "age": 28, "email": "anna@gmail.com"})),
            get("anna")
        );
    }
}
