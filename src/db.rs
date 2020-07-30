use serde_json::{Map};
use std::collections::BTreeMap;
use crate::json::{append, first, last, pop, pop_in, sum, JsonVal};

#[derive(Debug, PartialEq)]
pub enum Error {
    NotFound(String),
    BadArg,
    BadCmd,
    BadType,
    EmptySequence,
    BadNumber,
    BadKey,
    UnknownCmd(String),
    ParseCmd,
}

impl Error {
    pub fn to_string(&self) -> String {
        "error!".to_string()
    }
}

#[derive(Debug, PartialEq)]
pub enum Reply<'a> {
    Val(JsonVal),
    Ref(&'a JsonVal),
    Update(JsonVal),
    Insert(usize),
}

impl <'a> Reply<'a> {
    pub fn to_string(&self) -> String {
        match self {
            Reply::Val(val) => val.to_string(),
            Reply::Ref(val) => val.to_string(),
            Reply::Update(_) => "Updated entry".to_string(),
            Reply::Insert(_) => "Inserted entry".to_string()
        }
    }    
}

pub type Res<'a> = Result<Reply<'a>, Error>;

#[derive(Debug, Default, PartialEq)]
pub struct Db {
    data: BTreeMap<String, JsonVal>
}

impl Db {
    pub fn new() -> Db {
        Self::default()
    }

    pub fn insert<K: Into<String>, V: Into<JsonVal>>(&mut self, key: K, val: V) {
        self.data.insert(key.into(),val.into());
    }

    pub fn eval(&mut self, cmd: &str) -> Res {
        let cmd = match serde_json::from_str(cmd) {
            Ok(cmd) => cmd,
            Err(_) => return Err(Error::ParseCmd),
        };
        match cmd {
            JsonVal::String(key) => {
                match self.data.get(&key) {
                    Some(val) => Ok(Reply::Ref(val)),
                    None => Err(Error::NotFound(key)),
                }
            }
            JsonVal::Object(obj) => self.eval_obj(obj),            
            _ => Err(Error::BadCmd),
        }
    }

    fn eval_obj(&mut self, obj: Map<String, JsonVal>) -> Res {
        for (key, val) in obj.into_iter().take(1) {
            return self.eval_cmd(key, val)
        }
        Err(Error::BadCmd)
    }

    fn eval_cmd(&mut self, key: String, val: JsonVal) -> Res {
        match key.as_ref() {
            "append" => self.eval_append(val),
            "get" => self.eval_get(val),
            "set" => self.eval_set(val),
            "sum" => self.eval_sum(val),
            "first" => self.eval_first(val),
            "last" => self.eval_last(val),
            "pop" => self.eval_pop(val),
            _ => Err(Error::UnknownCmd(key)),
        }
    }

    fn eval_pop(&mut self, arg: JsonVal) -> Res {
        match arg {
            JsonVal::String(key) => {
                match self.data.get_mut(&key) {
                    Some(val) => pop(val),
                    None => Err(Error::EmptySequence),       
                }                
            }
            _ => Err(Error::BadArg), 
        }
    }    

    fn eval_sum(&mut self, arg: JsonVal) -> Res {
        match arg {
            JsonVal::String(key) => {
                match self.get(key)? {
                    Reply::Val(ref val) => sum(val).map(Reply::Val),
                    Reply::Ref(val) => sum(val).map(Reply::Val),
                    _ => Err(Error::BadArg)                  
                }                
            }
            JsonVal::Object(obj) => {
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

    fn eval_first<'a>(&'a mut self, arg: JsonVal) -> Res<'a> {
        match arg {
            JsonVal::String(key) => {
                let r = self.data.get(&key);
                match r {
                    Some(val) => first(val),
                    None => Err(Error::BadKey),                 
                }                
            }
            _ => unimplemented!(), 
        }
    }

    fn eval_last(&mut self, arg: JsonVal) -> Res {
        match arg {
            JsonVal::String(key) => {
                let r = self.data.get(&key);
                match r {
                    Some(val) => last(val),
                    None => Err(Error::BadKey),                 
                }                
            }
            _ => unimplemented!(), 
        }
    }

    fn eval_set(&mut self, arg: JsonVal) -> Res {
        match arg {
            JsonVal::Array(mut arr) => {
                if arr.len() != 2 {
                    return Err(Error::BadArg);
                }
                let val = arr.remove(1);
                let key = arr.remove(0);                
                self.put(key, val)
            }
            _ => unimplemented!(), 
        }
    }

    fn put(&mut self, key: JsonVal, val: JsonVal) -> Res {        
        match key {
            JsonVal::String(key) => {
                match self.data.insert(key, val) {
                    Some(val) => Ok(Reply::Update(val)),
                    None => Ok(Reply::Insert(1)),
                }
            }
            _ =>  Err(Error::BadKey)
        }
    }

    fn eval_append(&mut self, mut val: JsonVal) -> Res {
        match val {
            JsonVal::Array(ref mut arr) => {
                if arr.len() != 2 {
                    return Err(Error::BadCmd);
                }
                let elem = arr.remove(1);
                let key = arr.remove(0);
                let val = self.eval_get_mut(key)?;
                append(val, elem)
            }
            ref mut val => {
                *val = JsonVal::from(vec![val.clone()]); //TODO(optimize: remove clone)
                Ok(Reply::Update(JsonVal::Null))
            } 
        }
    }

    fn eval_get(&mut self, val: JsonVal) -> Res {
        match val {
            JsonVal::String(key) => self.get(key),
            _ => unimplemented!()
        }
    }

    fn get<'a>(&'a self, key: String) -> Res<'a> {
        match self.data.get(&key) {
            Some(val) => Ok(Reply::Ref(val)),
            None => Err(Error::NotFound(key.clone()))
        }
    }

    fn eval_get_mut(&mut self, val: JsonVal) -> Result<&mut JsonVal, Error> {
        match val {
            JsonVal::String(key) => self.data.get_mut(&key).ok_or(Error::NotFound(key)),
            _ => Err(Error::BadKey),
        }
    }
}

fn wrap<'a>(res: Option<&'a JsonVal>) -> Res<'a> {
    match res {
        Some(val) => Ok(Reply::Ref(val)),
        None => Err(Error::EmptySequence),
    }
}


#[test]
fn eval_get_string_ok<'a>() {
    let mut db = Db::default();
    let vec = vec![1,2,3,4,5];
    db.insert("a", vec.clone());
    let val = JsonVal::from(vec);
    let exp = Reply::Ref(&val);
    assert_eq!(Ok(exp), db.eval("a"));
} 

#[test]
fn eval_get_string_err_not_found<'a>() {
    let mut db = Db::default();
    db.insert("a".to_string(), JsonVal::from(1));
    assert_eq!(Err(Error::NotFound("b".to_string())), db.eval("b"));
} 

#[test]
fn eval_append_ok() {
    let mut db = Db::default();
    let vec =vec![1,2,3,4,5];
    db.insert("a", vec.clone());
    let val = JsonVal::from(vec);
    let exp = Reply::Ref(&val);
    assert_eq!(Ok(exp), db.eval("a"));
} 

/*
use crate::json::*;
use crate::replay::*;
use serde_json::Value as JsonVal;
use std::collections::BTreeMap;
use std::io::{self};
use std::path::Path;

// Type wrapper
pub type Cache = BTreeMap<String, JsonVal>;

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

    pub fn set(&mut self, key: String, val: JsonVal) -> io::Result<Option<JsonVal>> {
        self.log.write(&key, &val)?;
        Ok(self.cache.insert(key, val))
    }

    pub fn get(&self, key: &str) -> Option<&JsonVal> {
        self.cache.get(key)
    }

    pub fn del(&mut self, key: &str) -> io::Result<Option<JsonVal>> {
        self.log.remove(&key)?;
        Ok(self.cache.remove(key))
    }

    pub fn eval<S: Into<String>>(&mut self, line: S) -> Res<JsonVal> {
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

    fn json_f64(v: &JsonVal) -> f64 {
        v.as_f64().unwrap()
    }

    fn eval<'a, S: Into<String>>(db: &'a mut Database, line: S) -> Res<JsonVal> {
        db.eval(line)
    }

    fn db_get(db: &mut Database, key: &str) -> Res<JsonVal> {
        db.eval(get(key))
    }

    fn bad_type() -> Res<JsonVal> {
        Err("bad type")
    }

    #[test]
    fn open_db() {
        let mut db = test_db();
        assert_eq!(10, db.cache.len());
        assert_eq!(db_get(&mut db, "b"), Ok(JsonVal::Bool(true)));
        assert_eq!(
            db_get(&mut db, "ia"),
            Ok(JsonVal::Array(vec![
                JsonVal::from(1),
                JsonVal::from(2),
                JsonVal::from(3),
                JsonVal::from(4),
                JsonVal::from(5),
            ]))
        );
        assert_eq!(db_get(&mut db, "i"), Ok(JsonVal::from(3)));
        assert_eq!(db_get(&mut db, "f"), Ok(JsonVal::from(3.3)));
        assert_eq!(
            db_get(&mut db, "fa"),
            Ok(JsonVal::Array(vec![
                JsonVal::from(1.0),
                JsonVal::from(2.0),
                JsonVal::from(3.0),
                JsonVal::from(4.0),
                JsonVal::from(5.0),
            ]))
        );
        assert_eq!(db_get(&mut db, "f"), Ok(JsonVal::from(3.3)));
        assert_eq!(db_get(&mut db, "s"), Ok(JsonVal::from("hello")));
        assert_eq!(
            db_get(&mut db, "sa"),
            Ok(JsonVal::Array(vec![
                JsonVal::from("a"),
                JsonVal::from("b"),
                JsonVal::from("c"),
                JsonVal::from("d"),
            ]))
        );
        assert_eq!(db_get(&mut db, "z"), Ok(JsonVal::from(2.0)));
    }

    #[test]
    fn test_get() {
        let mut db = test_db();
        let val = eval(&mut db, get("b"));
        assert_eq!(Ok(JsonVal::Bool(true)), val);

        let val = eval(&mut db, get("b"));
        assert_eq!(Ok(JsonVal::Bool(true)), val);

        let val = db.eval(get("ia"));
        assert_eq!(
            Ok(JsonVal::Array(vec![
                JsonVal::from(1),
                JsonVal::from(2),
                JsonVal::from(3),
                JsonVal::from(4),
                JsonVal::from(5)
            ])),
            val
        );

        let val = db.eval(get("i"));
        assert_eq!(Ok(JsonVal::from(3)), val);
    }

    #[test]
    fn test_first() {
        let mut db = test_db();
        assert_eq!(Ok(JsonVal::Bool(true)), eval(&mut db, first(get("b"))));
        let val = db.eval(first(get("b")));
        assert_eq!(Ok(JsonVal::Bool(true)), val);
        let val = db.eval(first(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = db.eval(first(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = db.eval(first(get("fa")));
        assert_eq!(Ok(JsonVal::from(1.0)), val);
        let val = db.eval(first(get("ia")));
        assert_eq!(Ok(JsonVal::from(1)), val);
    }

    #[test]
    fn test_last() {
        let mut db = test_db();
        assert_eq!(Ok(JsonVal::from(true)), eval(&mut db, last(get("b"))));
        let val = db.eval(last(get("b")));
        assert_eq!(Ok(JsonVal::from(true)), val);
        let val = db.eval(last(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = db.eval(last(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = db.eval(last(get("fa")));
        assert_eq!(Ok(JsonVal::from(5.0)), val);
        let val = db.eval(last(get("ia")));
        assert_eq!(Ok(JsonVal::from(5)), val);
    }

    #[test]
    fn test_max() {
        let mut db = test_db();
        let val = eval(&mut db, max(get("b")));
        assert_eq!(Ok(JsonVal::Bool(true)), val);
        let val = eval(&mut db, max(get("b")));
        assert_eq!(Ok(JsonVal::Bool(true)), val);
        let val = eval(&mut db, max(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = eval(&mut db, max(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = eval(&mut db, max(get("ia")));
        assert_eq!(Ok(JsonVal::from(5.0)), val);
        let val = eval(&mut db, max(get("fa")));
        assert_eq!(Ok(JsonVal::from(5.0)), val);
    }

    #[test]
    fn test_min() {
        let mut db = test_db();
        let val = eval(&mut db, min(get("b")));
        assert_eq!(Ok(JsonVal::Bool(true)), val);
        let val = eval(&mut db, min(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = eval(&mut db, min(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = eval(&mut db, min(get("fa")));
        assert_eq!(Ok(JsonVal::from(1.0)), val);
        let val = eval(&mut db, min(get("ia")));
        assert_eq!(Ok(JsonVal::from(1.0)), val);
    }

    #[test]
    fn test_avg() {
        let mut db = test_db();
        let val = eval(&mut db, avg(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("avg", get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("avg", get("fa")));
        assert_eq!(Ok(JsonVal::from(3.0)), val);
        let val = db.eval(&json_fn("avg", get("ia")));
        assert_eq!(Ok(JsonVal::from(3.0)), val);
    }

    #[test]
    fn test_var() {
        let mut db = test_db();
        let val = eval(&mut db, var(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = db.eval(var(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = db.eval(var(get("fa"))).unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
        let val = db.eval(var(get("ia"))).unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
    }
    #[test]
    fn test_dev() {
        let mut db = test_db();
        let val = eval(&mut db, dev(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = eval(&mut db, dev(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = eval(&mut db, dev(get("fa"))).unwrap();
        assert_approx_eq!(1.4, json_f64(&val), 0.0249f64);
        let val = eval(&mut db, dev(get("ia"))).unwrap();
        assert_approx_eq!(1.4, json_f64(&val), 0.0249f64);
    }

    #[test]
    fn test_add() {
        let mut db = test_db();
        assert_eq!(
            Ok(JsonVal::from(9.0)),
            eval(&mut db, add(get("x"), get("y")))
        );
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(5.0),
                JsonVal::from(6.0),
                JsonVal::from(7.0),
                JsonVal::from(8.0),
                JsonVal::from(9.0),
            ])),
            eval(&mut db, add(get("x"), get("ia")))
        );

        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(6.0),
                JsonVal::from(7.0),
                JsonVal::from(8.0),
                JsonVal::from(9.0),
                JsonVal::from(10.0),
            ])),
            eval(&mut db, add(get("ia"), get("y")))
        );

        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(2.0),
                JsonVal::from(4.0),
                JsonVal::from(6.0),
                JsonVal::from(8.0),
                JsonVal::from(10.0),
            ])),
            eval(&mut db, add(get("ia"), get("ia")))
        );
        assert_eq!(Ok(JsonVal::Array(vec![
           JsonVal::from("ahello"),
           JsonVal::from("bhello"),
           JsonVal::from("chello"),
           JsonVal::from("dhello"), 
        ])), db.eval(add(get("sa"), get("s"))));
        assert_eq!(Ok(JsonVal::Array(vec![
           JsonVal::from("helloa"),
           JsonVal::from("hellob"),
           JsonVal::from("helloc"),
           JsonVal::from("hellod"),
        ])), db.eval(add(get("s"), get("sa")))); 
        assert_eq!(Ok(JsonVal::from("hellohello")), db.eval(add(get("s"), get("s"))));        
        
        assert_eq!(bad_type(), db.eval(add(get("s"), get("f"))));
        assert_eq!(bad_type(), db.eval(add(get("f"), get("s"))));
        assert_eq!(bad_type(), db.eval(add(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(add(get("s"), get("i"))));

    }

    #[test]
    fn test_sub() {
        let mut db = test_db();
        assert_eq!(Ok(JsonVal::from(-1.0)), db.eval(sub(get("x"), get("y"))));
        assert_eq!(Ok(JsonVal::from(1.0)), db.eval(sub(get("y"), get("x"))));
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(3.0),
                JsonVal::from(2.0),
                JsonVal::from(1.0),
                JsonVal::from(0.0),
                JsonVal::from(-1.0),
            ])),
            db.eval(sub(get("x"), get("ia")))
        );

        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(-4.0),
                JsonVal::from(-3.0),
                JsonVal::from(-2.0),
                JsonVal::from(-1.0),
                JsonVal::from(0.0),
            ])),
            db.eval(sub(get("ia"), get("y")))
        );

        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(0.0),
                JsonVal::from(0.0),
                JsonVal::from(0.0),
                JsonVal::from(0.0),
                JsonVal::from(0.0),
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
        assert_eq!(Ok(JsonVal::from(20.0)), db.eval(mul(get("x"), get("y"))));
        assert_eq!(Ok(JsonVal::from(16.0)), db.eval(mul(get("x"), get("x"))));
        let arr = vec![
            JsonVal::from(5.0),
            JsonVal::from(10.0),
            JsonVal::from(15.0),
            JsonVal::from(20.0),
            JsonVal::from(25.0),
        ];
        assert_eq!(
            Ok(JsonVal::from(arr.clone())),
            db.eval(mul(get("ia"), get("y")))
        );
        assert_eq!(Ok(JsonVal::from(arr)), db.eval(mul(get("y"), get("ia"))));
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(1.0),
                JsonVal::from(4.0),
                JsonVal::from(9.0),
                JsonVal::from(16.0),
                JsonVal::from(25.0),
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
        assert_eq!(Ok(JsonVal::from(1.0)), db.eval(div(get("x"), get("x"))));
        assert_eq!(Ok(JsonVal::from(1.0)), db.eval(div(get("y"), get("y"))));
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(1.0),
                JsonVal::from(1.0),
                JsonVal::from(1.0),
                JsonVal::from(1.0),
                JsonVal::from(1.0),
            ])),
            db.eval(div(get("ia"), get("ia")))
        );
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(0.5),
                JsonVal::from(1.0),
                JsonVal::from(1.5),
                JsonVal::from(2.0),
                JsonVal::from(2.5),
            ])),
            db.eval(div(get("ia"), get("z")))
        );
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(2.0),
                JsonVal::from(1.0),
                JsonVal::from(0.6666666666666666),
                JsonVal::from(0.5),
                JsonVal::from(0.4),
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


