use crate::cmd::{Cmd, QueryCmd};
use crate::err::Error;
use crate::inmemdb::InMemDb;
use crate::json::*;
use crate::ondiskdb::OnDiskDb;
use crate::query::Query;
use serde::Serialize;
use std::fs;
use std::path::Path;

fn count(val: &Json) -> Result<Json, Error> {
    Ok(json_count(val))
}

fn unique(val: &Json) -> Result<Json, Error> {
    Ok(json_unique(val))
}

#[derive(Debug)]
struct Cache {
    rdb: InMemDb,
    hdb: OnDiskDb,
}

#[derive(Debug, Serialize)]
pub struct Summary<'a> {
    no_entries: usize,
    keys: Vec<&'a String>,
    size: usize,
}

#[derive(Debug)]
pub struct Memson {
    cache: InMemDb,
    db: OnDiskDb,
}

fn opt_val(r: Result<Option<Json>, Error>) -> Result<Json, Error> {
    match r {
        Ok(Some(val)) => Ok(val),
        Ok(None) => Ok(Json::Null),
        Err(err) => Err(err),
    }
}

impl Memson {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        fs::create_dir_all(&path).map_err(|err| Error::BadIO(err))?;
        let db = OnDiskDb::open(path)?;
        let cache = db.populate()?;
        Ok(Self { cache, db })
    }

    pub(crate) fn query(&self, cmd: QueryCmd) -> Result<Json, Error> {
        let qry = Query::from(&self.cache, cmd);
        qry.exec()
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn summary(&self) -> Summary {
        let no_entries = self.cache.len();
        let size = self.cache.size();
        let keys = self.cache.keys();
        Summary {
            no_entries,
            size,
            keys,
        }
    }

    pub fn get(&self, key: &str) -> Result<&Json, Error> {
        self.cache.get(key)
    }

    pub fn set<K: Into<String>>(&mut self, key: K, val: Json) -> Result<Option<Json>, Error> {
        let key = key.into();
        self.db.set(&key, &val)?;
        Ok(self.cache.set(key, val))
    }

    pub fn append(&mut self, key: String, val: Json) -> Result<Json, Error> {
        let entry = self.cache.entry(key.clone());
        json_append(entry, val);
        self.db.set(key, entry)?;
        Ok(Json::Null)
    }

    pub fn eval(&mut self, cmd: Cmd) -> Result<Json, Error> {
        match cmd {
            Cmd::Add(lhs, rhs) => self.eval_bin_fn(lhs, rhs, &json_add),
            Cmd::Append(key, arg) => {
                let val = self.eval(*arg)?;
                self.append(key, val)
            },
            Cmd::Avg(arg) => self.eval_unr_fn(arg, &json_avg),
            Cmd::Count(arg) => self.eval_unr_fn(arg, &count),
            Cmd::Delete(key) => opt_val(self.delete(&key)),
            Cmd::Div(lhs, rhs) => self.eval_bin_fn(lhs, rhs, &json_div),
            Cmd::First(arg) => self.eval_unr_fn_ref(arg, &json_first),
            Cmd::Get(key) => self.get(&key).map(|x| x.clone()),
            Cmd::Insert(key, arg) => self.insert(key, arg).map(Json::from),
            Cmd::Json(val) => Ok(val),
            Cmd::Keys(_page) => Ok(self.keys()),
            Cmd::Last(arg) => self.eval_unr_fn_ref(arg, &json_last),
            Cmd::Len => Ok(Json::from(self.len())),
            Cmd::Max(arg) => self.eval_unr_fn_ref(arg, &json_max),
            Cmd::Min(arg) => self.eval_unr_fn_ref(arg, &json_min),
            Cmd::Mul(lhs, rhs) => self.eval_bin_fn(lhs, rhs, &json_mul),
            Cmd::Pop(key) => self.pop(&key),
            Cmd::Query(cmd) => self.query(cmd),
            Cmd::Set(key, arg) => {
                let val = self.eval(*arg)?;
                opt_val(self.set(key, val))
            }
            Cmd::StdDev(arg) => self.eval_unr_fn(arg, &json_dev),
            Cmd::Sub(lhs, rhs) => self.eval_bin_fn(lhs, rhs, &json_sub),
            Cmd::Sum(arg) => self.eval_unr_fn(arg, &json_sum),
            Cmd::Unique(arg) => self.eval_unr_fn(arg, &unique),
            Cmd::Var(arg) => self.eval_unr_fn(arg, &json_var),
        }
    } 

    fn keys(&self) -> Json {
        self.cache.json_keys()
    }

    fn eval_bin_fn(&mut self, lhs: Box<Cmd>, rhs: Box<Cmd>, f: &dyn Fn(&Json, &Json) -> Result<Json, Error>) -> Result<Json, Error> {
        let lhs = self.eval(*lhs)?;
        let rhs = self.eval(*rhs)?;
        f(&lhs, &rhs)
    }

    fn eval_unr_fn(&mut self, arg: Box<Cmd>, f: &dyn Fn(&Json) -> Result<Json, Error>) -> Result<Json, Error> {
        let val = self.eval(*arg)?;
        f(&val)
    }

    fn eval_unr_fn_ref(&mut self, arg: Box<Cmd>, f: &dyn Fn(&Json) -> Result<&Json, Error>) -> Result<Json, Error> {
        let val = self.eval(*arg)?;
        f(&val).map(|x| x.clone())
    }


    pub fn insert<K: Into<String>>(&mut self, key: K, val: Json) -> Result<usize, Error> {
        let mut rows = to_rows(val)?;
        let key = key.into();
        self.db.insert(&key, &mut rows)?;
        self.cache.insert(key, rows)
    }

    pub fn delete(&mut self, key: &str) -> Result<Option<Json>, Error> {
        self.db.delete(key)?;
        Ok(self.cache.delete(key))
    }

    pub fn pop(&mut self, key: &str) -> Result<Json, Error> {
        self.cache.pop(key)
    }  
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_approx_eq::assert_approx_eq;

    use serde_json::json;

    use std::fs;

    fn get<K:Into<String>>(key: K) -> Box<Cmd> {
        Box::new(Cmd::Get(key.into()))
    }

    fn avg<K:Into<String>>(key: K) -> Cmd {
        Cmd::Avg(get(key))
    }    

    fn first<K:Into<String>>(key: K) -> Cmd {
        Cmd::First(get(key))
    }

    fn last<K:Into<String>>(key: K) -> Cmd {
        Cmd::Last(get(key))
    }

    fn max<K:Into<String>>(key: K) -> Cmd {
        Cmd::Max(get(key))
    }

    fn min<K:Into<String>>(key: K) -> Cmd {
        Cmd::Min(get(key))
    }

    fn dev<K:Into<String>>(key: K) -> Cmd {
        Cmd::StdDev(get(key))
    }

    fn var<K:Into<String>>(key: K) -> Cmd {
        Cmd::Var(get(key))
    }

    fn mul<K:Into<String>>(x: K, y: K) -> Cmd {
        Cmd::Mul(get(x), get(y))
    }

    fn div<K:Into<String>>(x: K, y: K) -> Cmd {
        Cmd::Div(get(x), get(y))
    }

    fn add<K:Into<String>>(x: K, y: K) -> Cmd {
        Cmd::Add(get(x), get(y))
    }

    fn sub<K:Into<String>>(x: K, y: K) -> Cmd {
        Cmd::Sub(get(x), get(y))
    }

    fn test_db(path: &str) -> Memson {
        let _ = fs::remove_dir_all(path);
        let mut db = Memson::open(path).unwrap();
        insert_data(&mut db);
        db
    }

    fn bad_type() -> Result<Json, Error> {
        Err(Error::BadType)
    }      

    fn insert_data(db: &mut Memson) {
        assert_eq!(Ok(None), db.set("a", json!([1, 2, 3, 4, 5])));
        assert_eq!(Ok(None), db.set("b", json!(true)));
        assert_eq!(Ok(None), db.set("i", json!(2)));
        assert_eq!(Ok(None), db.set("f", json!(3.3)));
        assert_eq!(Ok(None), db.set("ia", json!([1, 2, 3, 4, 5])));
        assert_eq!(Ok(None), db.set("fa", json!([1.1, 2.2, 3.3, 4.4, 5.5])));
        assert_eq!(Ok(None), db.set("x", json!(4)));
        assert_eq!(Ok(None), db.set("y", json!(5)));
        assert_eq!(Ok(None), db.set("s", json!("hello")));
        assert_eq!(Ok(None), db.set("sa", json!(["a", "b", "c", "d"])));
        assert_eq!(Ok(None), db.set(
            "t",
            json!([
                {"name": "james", "age": 35},
                {"name": "ania", "age": 28, "job": "English Teacher"},
                {"name": "misha", "age": 12},
            ])
        ));
    }

    #[test]
    fn test_first() {
        let path = "test_first";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::Bool(true)), db.eval(first("b")));
        assert_eq!(Ok(Json::Bool(true)), db.eval(first("b")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(first("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(first("i")));
        assert_eq!(Ok(Json::from(1.1)), db.eval(first("fa")));
        assert_eq!(Ok(Json::from(1)), db.eval(first("ia")));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_last() {
        let path = "test_last";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::from(true)), db.eval(last("b")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(last("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(last("i")));
        assert_eq!(Ok(Json::from(5.5)), db.eval(last("fa")));
        assert_eq!(Ok(Json::from(5)), db.eval(last("ia")));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_max() {
        let path = "test_max";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::Bool(true)), db.eval(max("b")));
        assert_eq!(Ok(Json::from(2)), db.eval(max("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(max("f")));
        assert_eq!(Ok(Json::from(5)), db.eval(max("ia")));
        assert_eq!(Err(Error::FloatCmp), db.eval(max("fa")));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_min() {
        let path = "test_min";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::Bool(true)), db.eval(min("b")));
        assert_eq!(Ok(Json::from(2)), db.eval(min("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(min("f")));
        assert_eq!(Err(Error::FloatCmp), db.eval(min("fa")));
        assert_eq!(Ok(Json::from(1)), db.eval(min("ia")));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_avg() {
        let path = "test_avvg";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::from(3.3)), db.eval(avg("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(avg("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(avg("fa")));
        assert_eq!(Ok(Json::from(3.0)), db.eval(avg("ia")));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_var() {
        let path = "test_var";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::from(3.3)), db.eval(var("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(var("i")));
        let val = db.eval(var("fa")).unwrap().as_f64().unwrap();
        assert_approx_eq!(3.10, val, 0.0249f64);
        let val = db.eval(var("ia")).unwrap().as_f64().unwrap();
        assert_approx_eq!(2.56, val, 0.0249f64);
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_dev() {
        let path = "test_dev";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::from(3.3)), db.eval(dev("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(dev("i")));
        let val = db.eval(dev("fa")).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.55, val, 0.03f64);
        let val = db.eval(dev("ia")).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.414, val, 0.03f64);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_add() {
        let path = "test_add";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::from(9)), db.eval(add("x", "y")));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
                Json::from(8.0),
                Json::from(9.0),
            ])),
            db.eval(add("x", "ia"))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(4.0),
                Json::from(5.0),
                Json::from(6.0),
                Json::from(7.0),
            ])),
            db.eval(add("ia", "i"))
        );

        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("ahello"),
                Json::from("bhello"),
                Json::from("chello"),
                Json::from("dhello"),
            ])),
            db.eval(add("sa", "s"))
        );
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("helloa"),
                Json::from("hellob"),
                Json::from("helloc"),
                Json::from("hellod"),
            ])),
            db.eval(add("s", "sa"))
        );

        assert_eq!(Ok(Json::from("hellohello")), db.eval(add("s", "s")));
        assert_eq!(bad_type(), db.eval(add("s", "f")));
        assert_eq!(bad_type(), db.eval(add("f", "s")));
        assert_eq!(bad_type(), db.eval(add("i", "s")));
        assert_eq!(bad_type(), db.eval(add("s", "i")));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_sub() {
        let path = "test_sub";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::from(-1.0)), db.eval(sub("x", "y")));
        assert_eq!(Ok(Json::from(1.0)), db.eval(sub("y", "x")));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3.0),
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.0),
                Json::from(-1.0),
            ])),
            db.eval(sub("x", "ia"))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(-4.0),
                Json::from(-3.0),
                Json::from(-2.0),
                Json::from(-1.0),
                Json::from(0.0),
            ])),
            db.eval(sub("ia", "y"))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
                Json::from(0.0),
            ])),
            db.eval(sub("ia", "ia"))
        );

        assert_eq!(bad_type(), db.eval(sub("s", "s")));
        assert_eq!(bad_type(), db.eval(sub("sa", "s")));
        assert_eq!(bad_type(), db.eval(sub("s", "sa")));
        assert_eq!(bad_type(), db.eval(sub("i", "s")));
        assert_eq!(bad_type(), db.eval(sub("s", "i")));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn json_mul() {
        let path = "json_mul";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::from(20.0)), db.eval(mul("x", "y")));
        assert_eq!(Ok(Json::from(16.0)), db.eval(mul("x", "x")));
        let arr = vec![
            Json::from(5.0),
            Json::from(10.0),
            Json::from(15.0),
            Json::from(20.0),
            Json::from(25.0),
        ];
        assert_eq!(Ok(Json::from(arr.clone())), db.eval(mul("ia", "y")));
        assert_eq!(Ok(Json::from(arr)), db.eval(mul("y", "ia")));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(4.0),
                Json::from(9.0),
                Json::from(16.0),
                Json::from(25.0),
            ])),
            db.eval(mul("ia", "ia"))
        );
        assert_eq!(bad_type(), db.eval(mul("s", "s")));
        assert_eq!(bad_type(), db.eval(mul("sa", "s")));
        assert_eq!(bad_type(), db.eval(mul("s", "sa")));
        assert_eq!(bad_type(), db.eval(mul("i", "s")));
        assert_eq!(bad_type(), db.eval(mul("s", "i")));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn json_div() {
        let path = "json_div";
        let mut db = test_db(path);
        assert_eq!(Ok(Json::from(1.0)), db.eval(div("x", "x")));
        assert_eq!(Ok(Json::from(1.0)), db.eval(div("y", "y")));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
            ])),
            db.eval(div("ia", "ia"))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.5),
                Json::from(1.0),
                Json::from(1.5),
                Json::from(2.0),
                Json::from(2.5),
            ])),
            db.eval(div("ia", "i"))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.6666666666666666),
                Json::from(0.5),
                Json::from(0.4),
            ])),
            db.eval(div("i", "ia"))
        );

        assert_eq!(bad_type(), db.eval(div("s", "s")));
        assert_eq!(bad_type(), db.eval(div("sa", "s")));
        assert_eq!(bad_type(), db.eval(div("s", "sa")));
        assert_eq!(bad_type(), db.eval(div("i", "s")));
        assert_eq!(bad_type(), db.eval(div("s", "i")));

        let _ = fs::remove_dir_all(path);
    }


    #[test]
    fn set_persists_ok() {
        let val = json!({"name": "james"});
        let path = "set_ok";

        {
            let mut db = Memson::open(path).unwrap();
            assert_eq!(Ok(None), db.set("k", val.clone()));
        }
        {
            let db = Memson::open(path).unwrap();
            let exp = Ok(&val);
            assert_eq!(exp, db.get("k"));
        }
        fs::remove_dir_all(path).unwrap();
    }
}
