use crate::db::{Db,Reply};
use std::collections::BTreeMap;
use serde_json::Value as Json;

#[derive(Debug,Clone,PartialEq)]
pub enum Error {
    NotObj,
    MissingFrom,
    BadFrom,
}

pub fn eval_query(db: &Db, query: Json) -> Result<Reply<'_>, Error> {
    let obj = match query {
        Json::Object(obj) => obj,
        _ => return Err(Error::NotObj),
    };
    let key = match obj.get("from") {
        Some(Json::String(key)) => key,
        Some(_) => return Err(Error::BadFrom),
        None => return Err(Error::MissingFrom),
    };

    println!("{:?}", db.cache);
    let data = match db.get(key.clone()) {
        Ok(val) => val,
        Err(_) => return Err(Error::BadFrom),
    };
    Ok(Reply::Ref(data))
}

#[cfg(test)]
mod tests {
    use super::*;

    use lazy_static::lazy_static;
    use serde_json::json;
    use std::sync::RwLock;

    lazy_static! {
        static ref DB: RwLock<Db> = {
            let mut db = Db::open("query").unwrap();  
            db.insert("t", Json::from(vec![
                json!({"name": "james", "age": 35}),
                json!({"name": "ania", "age": 28, "job": "English Teacher"}),
                json!({"name": "misha", "age": 12}),
            ])).unwrap();          
            RwLock::new(db)
        };
    }

    #[test]
    fn select_all_query() {
        let db = DB.read().unwrap();
        let data = eval_query(&db, json!({"from": "t"}));
        let val = Json::from(vec![
            json!({"name": "james", "age": 35}),
            json!({"name": "ania", "age": 28, "job": "English Teacher"}),
            json!({"name": "misha", "age": 12}),
        ]);
        assert_eq!(Ok(Reply::Ref(&val)), data);
    }
}