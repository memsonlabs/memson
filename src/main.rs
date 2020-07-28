use serde_json::{Map};
use std::collections::BTreeMap;
use std::io;
use json::JsonVal;

mod json;

#[derive(Debug, PartialEq)]
pub enum Error {
    NotFound(String),
    BadCmd,
    BadType,
    EmptySequence,
    BadNumber,
}

#[derive(Debug, PartialEq)]
pub enum Reply<'a> {
    Val(JsonVal),
    Ref(&'a JsonVal),
}

pub type Res<'a> = Result<Reply<'a>, Error>;

#[derive(Debug, Default, PartialEq)]
struct Db {
    data: BTreeMap<String, JsonVal>
}

impl Db {
    fn new() -> Db {
        Self::default()
    }

    fn insert<K: Into<String>, V: Into<JsonVal>>(&mut self, key: K, val: V) {
        self.data.insert(key.into(),val.into());
    }

    fn eval<V: Into<JsonVal>>(&mut self, cmd: V) -> Res {
        match cmd.into() {
            JsonVal::String(key) => {
                match self.data.get(&key) {
                    Some(val) => Ok(Reply::Ref(val)),
                    None => Err(Error::NotFound(key)),
                }
            }
            JsonVal::Null => {
                unimplemented!()
            }
            JsonVal::Bool(_) => {
                unimplemented!()
            }
            JsonVal::Number(_) => {
                unimplemented!()
            }
            JsonVal::Array(_) => {
                unimplemented!()
            }
            JsonVal::Object(obj) => self.eval_obj(obj),            
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
            "sum" => self.eval_sum(val),
            _ => unimplemented!()
        }
    }

    fn eval_sum(&mut self, mut arg: JsonVal) -> Res {
        match arg {
            JsonVal::String(key) => {
                match self.get(key)? {
                    Reply::Val(ref val) => json::sum(val).map(Reply::Val),
                    Reply::Ref(val) => json::sum(val).map(Reply::Val),                    
                }                
            }
            _ => unimplemented!(), 
        }
    }

    fn eval_append(&mut self, mut val: JsonVal) -> Res {
        match val {
            JsonVal::Array(ref mut arr) => {
                if arr.len() != 2 {
                    return Err(Error::BadCmd);
                }
                let elem = arr.remove(1);
                let key = arr.remove(0).to_string();
                let val = self.get_mut(key)?;
                append(val, elem).map(|_| Reply::Val(JsonVal::Null))
            }
            _ => unimplemented!(), 
        }
    }

    fn eval_get(&mut self, val: JsonVal) -> Res {
        match val {
            JsonVal::String(key) => self.get(key),
            _ => unimplemented!()
        }
    }

    fn get(&self, key: String) -> Res {
        match self.data.get(&key) {
            Some(val) => Ok(Reply::Ref(val)),
            None => Err(Error::NotFound(key.clone()))
        }
    }

    fn get_mut(&mut self, key: String) -> Result<&mut JsonVal, Error> {
        self.data.get_mut(&key).ok_or(Error::NotFound(key))
    }
}

fn append(val: &mut JsonVal, elem: JsonVal) -> Result<(), Error> {
    match val {
        JsonVal::Array(ref mut arr) => arr.push(elem),
        _ => unimplemented!(),
    };
    Ok(())
}

fn main() {
    let mut db = Db::new();
    loop {
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {                
                println!("cmd = {}", input);
            }
            Err(error) => {
                println!("error: {}", error);
                continue
            }
        }
        let val = db.eval(input);
        println!("val = {:?}", val);
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

