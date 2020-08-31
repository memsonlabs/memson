use crate::aggregate::KeyedGet;
use crate::query::parse_aggregators;
use crate::cmd::Filter;
use crate::aggregate::KeyedAggregate;
use crate::cmd::QueryCmd;
use crate::cmd::Cmd;
use crate::db::Db;
use crate::err::Error;
use crate::json::*;
use serde_json::{Value as Json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct InMemDb {
    cache: BTreeMap<String, Json>,
}

impl Db for InMemDb {
    fn eval(&mut self, cmd: Cmd) -> Result<Json, Error> {
        match cmd {
                Cmd::Add(lhs, rhs) => self.eval_bin_fn(lhs, rhs, &json_add),
                Cmd::Append(key, arg) => {
                    let val = self.eval(*arg)?;
                    self.append(key, val)
                },
                Cmd::Avg(arg) => self.eval_unr_fn(arg, &json_avg),
                Cmd::Count(arg) => self.eval_unr_fn(arg, &count),
                Cmd::Delete(key) => {
                    Ok(if let Some(val) = self.cache.remove(&key) {
                        val
                    } else {
                        Json::Null
                    })
                }
                Cmd::Div(lhs, rhs) => self.eval_bin_fn(lhs, rhs, &json_div),
                Cmd::First(arg) => self.eval_unr_fn_ref(arg, &json_first),
                Cmd::Key(key) | Cmd::Key(key) => self.get(&key).map(|x| x.clone()),
                Cmd::Insert(_key, _arg) => {
                    unimplemented!()
                }
                Cmd::Json(val) => Ok(val),
                Cmd::Keys(_page) =>  Ok(self.keys()),
                Cmd::Last(arg) => self.eval_unr_fn_ref(arg, &json_last),
                Cmd::Len => Ok(Json::from(self.len())),
                Cmd::Max(arg) => self.eval_unr_fn_ref(arg, &json_max),
                Cmd::Min(arg) => self.eval_unr_fn_ref(arg, &json_min),
                Cmd::Mul(lhs, rhs) => self.eval_bin_fn(lhs, rhs, &json_mul),
                Cmd::Pop(key) => self.pop(&key),
                Cmd::Query(cmd) => self.query(cmd),
                Cmd::Set(key, arg) => {
                    let val = self.eval(*arg)?;
                    Ok(if let Some(val) = self.cache.insert(key, val) {
                        val
                    } else {
                        Json::Null
                    })
                }
                Cmd::StdDev(arg) => self.eval_unr_fn(arg, &json_dev),
                Cmd::Sub(lhs, rhs) => self.eval_bin_fn(lhs, rhs, &json_sub),
                Cmd::Sum(arg) => self.eval_unr_fn(arg, &json_sum),
                Cmd::Summary => Ok(self.summary()),
                Cmd::Unique(arg) => self.eval_unr_fn(arg, &unique),
                Cmd::Var(arg) => self.eval_unr_fn(arg, &json_var),
            }        
    }
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

    fn keys(&self) -> Json {
        let arr = self.cache.keys().map(|x| Json::from(x.to_string())).collect();
        Json::Array(arr)
    }

    pub fn summary(&self) -> Json {
        let no_entries = Json::from(self.cache.len());
        let keys: Vec<Json> = self.cache.keys().map(|x| Json::String(x.to_string())).collect();
        let size = Json::from(bincode::serialized_size(&self.cache).unwrap());
        json!({"no_entries": no_entries, "keys": keys, "size": size})
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn pop(&mut self, key: &str) -> Result<Json, Error> {
        let val = self.get_mut(key)?;
        json_pop(val)
    }

    pub fn query(&self, cmd: QueryCmd) -> Result<Json, Error> {
        let qry = Query::from(self, cmd);
        qry.exec()
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

    pub fn get(&self, key: &str) -> Result<Json, Error> {
        let mut it = key.split(".");
        let val = self.cache
            .get(it.next().unwrap())
            .ok_or(Error::UnknownKey(key.into()))?;
        let mut nested_val = None;
        for key in it {
            nested_val = json_get(key, if let Some(ref v) = nested_val { v } else {val});
        }   
        if let Some(val) = nested_val {
            Ok(val)
        } else {
            Ok(val.clone())
        }
    }

    pub fn get_ref(&self, key: &str) -> Result<&Json, Error> {
        self.cache.get(key).ok_or(Error::BadKey)
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

    pub fn delete(&mut self, key: &str) -> Option<Json> {
        self.cache.remove(key)
    }

    fn append(&mut self, key: String, val: Json) -> Result<Json, Error> {
        let entry = self.entry(key);
        json_append(entry, val);
        Ok(Json::Null)
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
}

fn json_arr(val: &mut Json) -> &mut Vec<Json> {
    match val {
        Json::Array(ref mut arr) => arr,
        _ => panic!(),
    }
}

pub struct Query<'a> {
    db: &'a InMemDb,
    cmd: QueryCmd,
}

fn find_bin_fn(lhs: &Cmd, rhs: &Cmd) -> Option<Vec<String>> {
    let x = find_keys(&lhs);
    let y = find_keys(&rhs);
    match (x, y) {
        (Some(mut x), Some(y)) => {
            x.extend(y);
            Some(x)
        }
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None
    }
}

fn find_keys(cmd: &Cmd) -> Option<Vec<String>> {
    match cmd {
        Cmd::Add(lhs, rhs) => find_bin_fn(lhs, rhs),
        Cmd::Append(_, _) => None,
        Cmd::Avg(arg) => find_keys(arg),
        Cmd::Count(arg) => find_keys(arg),
        Cmd::Delete(_) => None,
        Cmd::Div(lhs, rhs) => find_bin_fn(lhs, rhs),
        Cmd::First(arg) => find_keys(arg),
        Cmd::Key(key) | Cmd::Key(key) => Some(vec![key.clone()]),
        Cmd::Insert(_, _) => None,
        Cmd::Json(_) => None,
        Cmd::Keys(_) => None,
        Cmd::Last(arg) => find_keys(arg),
        Cmd::Len => None,
        Cmd::Max(arg) => find_keys(arg),
        Cmd::Min(arg) => find_keys(arg),
        Cmd::Mul(lhs, rhs) => find_bin_fn(lhs, rhs),
        Cmd::Pop(_) => None,
        Cmd::Query(_) => None,
        Cmd::Set(_, _) => None,
        Cmd::StdDev(arg) => find_keys(arg),
        Cmd::Sub(lhs, rhs) => find_bin_fn(lhs, rhs),
        Cmd::Sum(arg) => find_keys(arg),
        Cmd::Summary => None,
        Cmd::Unique(arg) => find_keys(arg),
        Cmd::Var(arg) => find_keys(arg),
    }
}

fn cmd_keys(cmds: &[(String, Cmd)]) -> Vec<String> {
    let mut keys = Vec::new();
    for (_,cmd) in cmds {
        if let Some(k) = find_keys(cmd) {
            keys.extend(k);
        }
    }
    keys
}

fn has_aggregation(cmds: &[(String, Cmd)]) -> bool {
    for (_, cmd) in cmds {
        if !cmd.is_aggregate() {
            return false;
        }
    }
    true
}

impl<'a> Query<'a> {
    pub fn from(db: &'a InMemDb, cmd: QueryCmd) -> Self {
        Self { db, cmd }
    }

    pub fn exec(&self) -> Result<Json, Error> {
        let rows = self.eval_from()?;
        if let Some(ref filter) = self.cmd.filter {
            let filtered_rows = self.eval_where(rows, filter.clone())?;
            self.eval_select(&filtered_rows)
        } else {
            self.eval_select(rows)
        }
    }

    fn eval_where(&self, rows: &[Json], filter: Filter) -> Result<Vec<Json>, Error> {
        let mut filtered_rows = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            let obj = json_obj(row)?;
            if filter.apply(obj)? {
                let obj_row = add_row_id(row, i)?;
                filtered_rows.push(obj_row);
            }
        }
        Ok(filtered_rows)
    }

    fn eval_from(&self) -> Result<&'a [Json], Error> {
        //TODO remove cloning
        match self.db.get_ref(&self.cmd.from) {
            Ok(Json::Array(rows)) => Ok(rows.as_ref()),
            _ => Err(Error::BadFrom),
        }
    }

    // TODO remove cloning
    fn eval_select(&self, rows: &[Json]) -> Result<Json, Error> {
        if let Some(by) = &self.cmd.by {
            self.eval_keyed_select(by, rows)
        } else {
            match &self.cmd.selects {
                Some(Json::Array(ref selects)) => self.eval_selects(selects, rows),
                Some(Json::String(s)) => self.eval_selects(&[Json::from(s.to_string())], rows),
                Some(Json::Object(obj)) => self.eval_obj_selects(obj.clone(), rows),
                Some(_) => Err(Error::BadSelect),
                None => self.eval_select_all(rows),
            }
        }
    }

    fn parse_ids(&self) -> Result<Option<Vec<String>>, Error> {
        if let Some(selects) = &self.cmd.selects {
            match selects {
                Json::Object(obj) => {
                    let mut ids = Vec::new();
                    for (_, v) in obj {
                        match v {
                            Json::Object(obj) => {
                                for (_, v) in obj {
                                    ids.push(json_to_string(v).ok_or(Error::BadSelect)?);
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }
                    Ok(Some(ids))
                }
                Json::Array(arr) => {
                    let mut ids = Vec::new();
                    for val in arr {
                        match val {
                            Json::String(s) => ids.push(s.to_string()),
                            Json::Object(obj) => {
                                for (_, v) in obj {
                                    match v {
                                        Json::Object(obj) => {
                                            for (_, v) in obj {
                                                ids.push(json_to_string(v).ok_or(Error::BadSelect)?);
                                            }
                                        }
                                        _ => unimplemented!(),
                                    }
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }
                    Ok(Some(ids))
                }
                Json::String(s) => {
                    Ok(Some(vec![s.to_string()]))
                }
                _ => unimplemented!(),
            }
        } else {
            Ok(None)
        }
    }

    fn eval_keyed_select(&self, by: &Json, rows: &[Json]) -> Result<Json, Error> {
        println!("eval_keyed_select({:?}, {:?})", by, rows);
        let by_key: &str = match by {
            Json::String(s) => s.as_ref(),            
            _ => todo!(),
        };
        println!("by_key={:?}", by_key);
        let ids = self.parse_ids()?; // TODO remove unwrap
        println!("ids={:?}", ids);
        let mut out = JsonObj::new();
        if let Some(keys) = ids {            
            for row in rows {
                
                let by_key = if let Some(key) = row.get(by_key) {
                    key
                } else {
                    continue
                };
                println!("by_key={:?}", by_key);
                match row {
                    Json::Object(obj) => {
                        let mut keyed_obj = JsonObj::new();
                        for key in &keys {
                            if let Some(val) = obj.get(key) {
                                keyed_obj.insert(key.to_string(), val.clone());
                            }
                        }
                        let entry = out.entry(by_key.to_string()).or_insert_with(|| Json::Array(vec![]));
                        let arr = json_arr(entry);
                        println!("arr={:?}", arr);
                        arr.push(Json::from(keyed_obj));
                    }
                    _ => continue
                }
                
            }            
        } else {
            for row in rows {
                let key = if let Some(key) = row.get(by_key) {
                    key
                } else {
                    continue
                };                
            }
        };
        Ok(Json::Object(out))
    }

    fn parse_key_aggregations(&self) -> Result<Option<KeyedGet>, Error> {
        if let Some(selects) = &self.cmd.selects {
            let mut keys = Vec::new();
            match selects {
                Json::Object(obj) => {
                    println!("{:?}", obj);
                    for (col_name, cmd) in obj {
                        let cmd = Cmd::parse(cmd.clone())?;
                        if let Some(v) = cmd.key() {
                            keys.extend(v);
                        }
                    }                    
                }
                _ => todo!(),
            };
            Ok(Some(KeyedGet::from(keys)))
        } else {
            Ok(None)
        }
    }

    fn eval_obj_selects(&self, obj: JsonObj, rows: &[Json]) -> Result<Json, Error> {
        if obj.is_empty() {
            return self.eval_select_all(rows);
        }
        let mut cmds = Vec::new();
        for (key, val) in obj {
            let cmd = Cmd::parse_cmd(val).map_err(|_| Error::BadSelect)?;
            cmds.push((key, cmd));
        }
        if has_aggregation(&cmds) {
            println!("aggregation has started");
            let mut out = JsonObj::new();
            let mut aggs = parse_aggregators(&cmds)?;
            println!("got parsed aggregations");
            for row in rows {
                match row {
                    Json::Object(obj) => {
                        for agg in aggs.iter_mut() {
                            agg.push(obj)?;
                        }
                    }
                    _ => continue,
                }
            }
            println!("processed rows");
            println!("aggs len ={:?}", aggs.len());
            for agg in aggs {
                println!("processing agg");
                let col_name = agg.name().to_string();
                println!("col_name={:?}", col_name);
                let val = agg.aggregate();
                println!("val={:?}", val);
                out.insert(col_name, val);
            }

            println!("out={:?}", out);
            Ok(Json::Object(out))
        } else {
            eval_nonaggregate(&cmds, rows)
        }
    }

    //TODO remove clones
    fn eval_selects(&self, selects: &[Json], rows: &[Json]) -> Result<Json, Error> {
        if selects.is_empty() {
            return self.eval_select_all(rows);
        }
        let mut output = Vec::new();
        let cmds = parse_selects_to_cmd(selects)?;
        for (_, row) in rows.iter().enumerate() {
            let mut map = JsonObj::new();
            for cmd in &cmds {
                eval_row_cmd(cmd, row, &mut map)?;
            }
            if !map.is_empty() {
                output.push(Json::from(map));
            }
        }
        Ok(Json::from(output))
    }

    fn eval_select_all(&self, rows: &[Json]) -> Result<Json, Error> {
        let mut output = Vec::new();
        for (i, row) in rows.iter().take(50).enumerate() {
            let row = add_row_id(row, i)?;
            output.push(row);
        }
        Ok(Json::from(output))
    }
}

fn eval_row_cmd(cmd: &Cmd, row: &Json, obj: &mut JsonObj) -> Result<(), Error> {
    match cmd {
        Cmd::Key(key) => {
            if let Some(val) = row.get(&key) {
                obj.insert(key.to_string(), val.clone());
            }
            Ok(())
        }
        _ => unimplemented!(),
    }
}

fn add_row_id(row: &Json, i: usize) -> Result<Json, Error> {
    match row {
        Json::Object(obj) => {
            let mut obj = obj.clone();
            if obj.get("_id").is_none() {
                obj.insert("_id".to_string(), Json::from(i));
            }
            Ok(Json::from(obj))
        }
        _ => Err(Error::BadObject),
    }
}


//TODO pass vec to remove clone
fn parse_selects_to_cmd(selects: &[Json]) -> Result<Vec<Cmd>, Error> {
    let mut cmds = Vec::new();
    for select in selects {
        cmds.push(Cmd::parse(select.clone())?);
    }
    Ok(cmds)
}

fn eval_aggregate(cmd: Cmd, rows: &[Json]) -> Result<Option<Json>, Error> {
    unimplemented!()
}

fn eval_nonaggregate(cmds: &[(String, Cmd)], rows: &[Json]) -> Result<Json, Error> {
    let mut out = Vec::new();
    for row in rows {
        let mut obj = None;
        for cmd in cmds {
            eval_row(&mut obj, cmd, row)?;
        }
        if let Some(obj) = obj {
            out.push(Json::from(obj));
        }
    }
    Ok(Json::Array(out))
}

fn eval_row(out: &mut Option<JsonObj>, cmd: &(String, Cmd), row: &Json) -> Result<(), Error> {
    match (&cmd.1, row) {
        (Cmd::Key(key), Json::Object(obj)) => {
            if let Some(val) = obj.get(key) {
                let key = cmd.0.to_string();
                let val = val.clone();
                if let Some(ref mut obj) = out {
                    obj.insert(key, val);
                } else {
                    let mut o = JsonObj::new();
                    //o.insert("_id".to_string(), obj.get("_id").unwrap().clone());
                    o.insert(key, val);
                    *out = Some(o);
                }
            }
            Ok(())
        }
        _ => {
            eprintln!("({:?}, {:?})", &cmd.1, row);
            unimplemented!()
        }
    }
}

fn eval_var(arg: Box<Cmd>, rows: &[Json]) -> Result<Option<Json>, Error> {
    unimplemented!()
    /*
    let key = match *arg {
        Cmd::Key(key) => key,
        _ => unimplemented!(), 
    };
    let mut avg = Avg::new();
    for row in rows {
        if let Some(val) = row.get(&key) {
            avg.aggregate(val)?;
        }
    }
    if avg.count == 0 {
        return Ok(None);
    }
    let mut var = Var::from(avg.avg(), avg.count);
    for row in rows {
        if let Some(val) = row.get(&key) {
            var.aggregate(val)?;
        }
    }
    Ok(var.apply())
    */
}

fn eval_dev(arg: Box<Cmd>, rows: &[Json]) -> Result<Option<Json>, Error> {
    let r = match eval_var(arg, rows)? {
        Some(val) => val.as_f64().map(|x| Json::from(x.sqrt())),
        None => None,
    };
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_approx_eq::assert_approx_eq;

    use serde_json::json;

    fn get<K:Into<String>>(key: K) -> Box<Cmd> {
        Box::new(Cmd::Key(key.into()))
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

    fn test_db() -> InMemDb {
        let mut db = InMemDb::new();
        insert_data(&mut db);
        db
    }

    fn bad_type() -> Result<Json, Error> {
        Err(Error::BadType)
    }      

    fn insert_data(db: &mut InMemDb) {
        assert_eq!(None, db.set("a", json!([1, 2, 3, 4, 5])));
        assert_eq!(None, db.set("b", json!(true)));
        assert_eq!(None, db.set("i", json!(2)));
        assert_eq!(None, db.set("f", json!(3.3)));
        assert_eq!(None, db.set("ia", json!([1, 2, 3, 4, 5])));
        assert_eq!(None, db.set("fa", json!([1.1, 2.2, 3.3, 4.4, 5.5])));
        assert_eq!(None, db.set("x", json!(4)));
        assert_eq!(None, db.set("y", json!(5)));
        assert_eq!(None, db.set("s", json!("hello")));
        assert_eq!(None, db.set("sa", json!(["a", "b", "c", "d"])));
        assert_eq!(None, db.set(
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
        let mut db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db.eval(first("b")));
        assert_eq!(Ok(Json::Bool(true)), db.eval(first("b")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(first("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(first("i")));
        assert_eq!(Ok(Json::from(1.1)), db.eval(first("fa")));
        assert_eq!(Ok(Json::from(1)), db.eval(first("ia")));
    }

    #[test]
    fn test_last() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(true)), db.eval(last("b")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(last("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(last("i")));
        assert_eq!(Ok(Json::from(5.5)), db.eval(last("fa")));
        assert_eq!(Ok(Json::from(5)), db.eval(last("ia")));
    }

    #[test]
    fn test_max() {
        let mut db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db.eval(max("b")));
        assert_eq!(Ok(Json::from(2)), db.eval(max("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(max("f")));
        assert_eq!(Ok(Json::from(5)), db.eval(max("ia")));
        assert_eq!(Err(Error::FloatCmp), db.eval(max("fa")));
    }

    #[test]
    fn test_min() {
        let mut db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db.eval(min("b")));
        assert_eq!(Ok(Json::from(2)), db.eval(min("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(min("f")));
        assert_eq!(Err(Error::FloatCmp), db.eval(min("fa")));
        assert_eq!(Ok(Json::from(1)), db.eval(min("ia")));
    }

    #[test]
    fn test_avg() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.eval(avg("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(avg("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(avg("fa")));
        assert_eq!(Ok(Json::from(3.0)), db.eval(avg("ia")));
    }

    #[test]
    fn test_var() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.eval(var("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(var("i")));
        let val = db.eval(var("fa")).unwrap().as_f64().unwrap();
        assert_approx_eq!(3.10, val, 0.0249f64);
        let val = db.eval(var("ia")).unwrap().as_f64().unwrap();
        assert_approx_eq!(2.56, val, 0.0249f64);
    }

    #[test]
    fn test_dev() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.eval(dev("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(dev("i")));
        let val = db.eval(dev("fa")).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.55, val, 0.03f64);
        let val = db.eval(dev("ia")).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.414, val, 0.03f64);
    }

    #[test]
    fn test_add() {
        let mut db = test_db();
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
    }

    #[test]
    fn test_sub() {
        let mut db = test_db();
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
    }

    #[test]
    fn json_mul() {
        let mut db = test_db();
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
    }

    #[test]
    fn json_div() {
        let mut db = test_db();
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
    }
    
    #[test]
    fn open_db() {
        let db = test_db();
        assert!(db.len() >= 10);
        assert_eq!(db.get("b"), Ok(Json::Bool(true)));
        assert_eq!(
            db.get("ia"),
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5),
            ]))
        );
        assert_eq!(db.get("i"), Ok(Json::from(2)));
        assert_eq!(db.get("f"), Ok(Json::from(3.3)));
        assert_eq!(
            db.get("fa"),
            Ok(Json::Array(vec![
                Json::from(1.1),
                Json::from(2.2),
                Json::from(3.3),
                Json::from(4.4),
                Json::from(5.5),
            ]))
        );
        assert_eq!(db.get("f"), Ok(Json::from(3.3)));
        assert_eq!(db.get("s"), Ok(Json::from("hello")));
        assert_eq!(
            db.get("sa"),
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
        assert_eq!(Ok(Json::Bool(true)), db.get("b"));
        assert_eq!(Ok(Json::Bool(true)), db.get("b"));
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5)
            ])),
            db.get("ia")
        );
        assert_eq!(Ok(Json::from(2)), db.get("i"));
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
        assert_eq!(Ok(Json::from(val)), db.get("a"));
    }

    #[test]
    fn eval_get_string_err_not_found() {
        let db = test_db();
        assert_eq!(Err(Error::UnknownKey("ania".to_string())), db.get("ania"));
    }
    
    /*
    #[test]
    fn test_first() {
        let mut db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db.eval(first("b")));
        assert_eq!(Ok(Json::Bool(true)), db.eval(first("b")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(first("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(first("i")));
        assert_eq!(Ok(Json::from(1.1)), db.eval(first("fa")));
        assert_eq!(Ok(Json::from(1)), db.eval(first("ia")));
    }

    #[test]
    fn test_last() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(true)), db.eval(last("b")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(last("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(last("i")));
        assert_eq!(Ok(Json::from(5.5)), db.eval(last("fa")));
        assert_eq!(Ok(Json::from(5)), db.eval(last("ia")));
    }

    #[test]
    fn test_max() {
        let mut db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db.eval(max("b")));
        assert_eq!(Ok(Json::from(2)), db.eval(max("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(max("f")));
        assert_eq!(Ok(Json::from(5)), db.eval(max("ia")));
        assert_eq!(Err(Error::FloatCmp), db.eval(max("fa")));
    }

    #[test]
    fn test_min() {
        let mut db = test_db();
        assert_eq!(Ok(Json::Bool(true)), db.eval(min("b")));
        assert_eq!(Ok(Json::from(2)), db.eval(min("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(min("f")));
        assert_eq!(Err(Error::FloatCmp), db.eval(min("fa")));
        assert_eq!(Ok(Json::from(1)), db.eval(min("ia")));
    }

    #[test]
    fn test_avg() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.eval(avg("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(avg("i")));
        assert_eq!(Ok(Json::from(3.3)), db.eval(avg("fa")));
        assert_eq!(Ok(Json::from(3.0)), db.eval(avg("ia")));
    }

    #[test]
    fn test_var() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.eval(var("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(var("i")));
        let val = db.eval(var("fa")).unwrap().as_f64().unwrap();
        assert_approx_eq!(3.10, val, 0.0249f64);
        let val = db.eval(var("ia")).unwrap().as_f64().unwrap();
        assert_approx_eq!(2.56, val, 0.0249f64);
    }

    #[test]
    fn test_dev() {
        let mut db = test_db();
        assert_eq!(Ok(Json::from(3.3)), db.eval(dev("f")));
        assert_eq!(Ok(Json::from(2)), db.eval(dev("i")));
        let val = db.eval(dev("fa")).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.55, val, 0.03f64);
        let val = db.eval(dev("ia")).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.414, val, 0.03f64);
    }

    #[test]
    fn test_add() {
        let mut db = test_db();
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
    }

    #[test]
    fn test_sub() {
        let mut db = InMemDb::new();
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
    }

    #[test]
    fn json_mul() {
        let mut db = test_db();
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
    }

    #[test]
    fn json_div() {
        let mut db = test_db();
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
    }
    */

    #[test]
    fn nested_get() {
        let mut db = test_db();
        let act = db.eval(*get("t.name")).unwrap();
        assert_eq!(json!(["james", "ania", "misha"]), act);
    }

    fn query(json: Json) -> Result<Json, Error> {
        let db = test_db();
        let cmd = serde_json::from_value(json).unwrap();
        let qry = Query::from(&db, cmd);
        qry.exec()
    }


    #[test]
    fn select_all_query() {
        let qry = query(json!({"from": "t"}));
        assert_eq!(Ok(all_data()), qry);
    }

    fn all_data() -> Json {
        json!([
            {"_id": 0, "name": "james", "age": 35},
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
            {"_id": 2, "name": "misha", "age": 10},
            {"_id": 3, "name": "ania", "age": 20},
        ])
    }

    #[test]
    fn select_1_prop_query() {
        let qry = query(json!({"select": "name", "from": "t"}));
        let val = json!([
            {"name": "james"},
            {"name": "ania"},
            {"name": "misha"},
            {"name": "ania"},
        ]);
        assert_eq!(Ok(val), qry);
    }
    #[test]
    fn select_3_prop_query() {
        let qry = query(json!({"select": ["name", "age", "job"], "from": "t"}));
        let val = json!([
            {"name": "james", "age": 35},
            {"name": "ania", "age": 28, "job": "english teacher"},
            {"name": "misha", "age": 10},
            {"name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_eq_query() {
        let qry = query(json!({"from": "t", "where": {"==": ["name", "james"]}}));
        let val = json!([{"_id": 0, "name": "james", "age": 35}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_neq_query_ok() {
        let qry = query(json!({"from": "t", "where": {"!=": ["name", "james"]}}));
        let val = json!([
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
            {"_id": 2, "name": "misha", "age": 10},
            {"_id": 3, "name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_gt_query_ok() {
        let qry = query(json!({"from": "t", "where": {">": ["age", 20]}}));
        let val = json!([
            {"_id": 0, "name": "james", "age": 35},
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_lt_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<": ["age", 20]}}));
        let val = json!([{"_id": 2, "name": "misha", "age": 10}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_lte_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<=": ["age", 28]}}));
        let val = json!([
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
            {"_id": 2, "name": "misha", "age": 10},
            {"_id": 3, "name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_gte_query_ok() {
        let qry = query(json!({"from": "t", "where": {">=": ["age", 28]}}));
        let val = json!([
            {"_id": 0, "name": "james", "age": 35},
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"}
        ]);

        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_and_gate_ok() {
        let qry = query(json!({
            "from": "t",
            "where": {"&&": [
                {">": ["age", 20]},
                {"==": ["name", "ania"]}
            ]}
        }));
        let val = json!([{"_id": 1, "name": "ania", "age": 28, "job": "english teacher"}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_sum_ok() {
        let qry = query(json!({
            "select": {"totalAge": {"sum": {"get": "age"}}},
            "from": "t"
        }));
        let val = json!({"totalAge": 93});
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_max_num_ok() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": {"get": "age"}}
            },
            "from": "t"
        }));
        let val = json!({"maxAge": 35});
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_max_str_ok() {
        let qry = query(json!({
            "select": {
                "maxName": {"max":  {"get": "name"}}
            },
            "from": "t"
        }));
        let val = json!({"maxName": "misha"});
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_min_num_ok() {
        let qry = query(json!({
            "select": {
                "minAge": {"min": {"get": "age"}}
            },
            "from": "t"
        }));
        let val = json!({"minAge": 10});
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_avg_num_ok() {
        let qry = query(json!({
            "select": {
                "avgAge": {"avg": {"get": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"avgAge": 23.25})), qry);
    }

    #[test]
    fn select_first_ok() {
        let qry = query(json!({
            "select": {
                "firstAge": {"first": {"get": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"firstAge": 35})), qry);
    }

    #[test]
    fn select_last_ok() {
        let qry = query(json!({
            "select": {
                "lastAge": {"last": {"get": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"lastAge": 20})), qry);
    }

    #[test]
    fn select_var_num_ok() {
        let qry = query(json!({
            "select": {
                "varAge": {"var": {"get": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"varAge": 115.58333333333333})), qry);
    }

    #[test]
    fn select_dev_num_ok() {
        let qry = query(json!({
            "select": {
                "devAge": {"dev": {"get": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"devAge": 10.750968948580091})), qry);
    }

    #[test]
    fn select_max_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": {"get": "age"}}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!({"maxAge": 35})), qry);
    }

    #[test]
    fn select_get_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"get": "age"}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!([{"_id": 0,"age":35}, {"_id": 1, "age": 28}])), qry);
    }

    #[test]
    fn select_min_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "minAge": {"min":  {"get": "age"}}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!({"minAge": 28})), qry);
    }

    #[test]
    fn select_min_max_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "youngestAge": {"min": {"get": "age"}},
                "oldestAge": {"max": {"get": "age"}},
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!({"youngestAge": 28, "oldestAge": 35})), qry);
    }

    #[test]
    fn select_empty_obj() {
        let qry = query(json!({
            "select": {},
            "from": "t",
        }));
        assert_eq!(
            Ok(json!([
                {"_id": 0, "name": "james", "age": 35},
                {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
                {"_id": 2, "name": "misha", "age": 10},
                {"_id": 3, "name": "ania", "age": 20},
            ])),
            qry
        );
    }

    #[test]
    fn select_empty_obj_where_age_gt_20() {
        let qry = query(json!({
            "select": {},
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!([
                {"_id": 0, "name": "james", "age": 35},
                {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
            ])),
            qry
        );
    }

    #[test]
    fn select_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"get": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": [{"age": 10}],
                "ania": [{"age": 28}, {"age": 20}],
                "james": [{"age": 35}],
            })),
            qry
        );
    }

    #[test]
    fn select_first_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"first": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 10},
                "ania": {"age": 28},
                "james": {"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_last_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"last": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 10},
                "ania": {"age": 20},
                "james": {"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_count_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"count": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 1},
                "ania": {"age": 2},
                "james": {"age": 1},
            })),
            qry
        );
    }

    #[test]
    fn select_min_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"min": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 10},
                "ania": {"age": 20},
                "james": {"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_max_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"max": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 10},
                "ania": {"age": 28},
                "james": {"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_name_by_age() {
        let qry = query(json!({
            "select": {
                "name": {"get": "name"},
            },
            "from": "t",
            "by": "age",
        }));
        assert_eq!(
            Ok(json!({
                "10": [{"name": "misha"}],
                "20": [{"name": "ania"}],
                "28": [{"name": "ania"}],
                "35": [{"name": "james"}],
            })),
            qry
        );
    }

    #[test]
    fn select_age_by_name_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"get": "age"},
            },
            "from": "t",
            "by": "name",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({"ania": [{"age":28}], "james": [{"age": 35}]})),
            qry
        );
    }

    #[test]
    fn select_age_job_by_name_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"get": "age"},
                "job": {"get": "job"},
            },
            "from": "t",
            "by": "name",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({
                "ania": [{"age": 28, "job": "english teacher"}],
                "james": [{"age": 35}],
            })),
            qry
        );
    }

    #[test]
    fn select_all_by_name_where_age_gt_20() {
        let qry = query(json!({
            "from": "t",
            "by": "name",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({
                "ania": [{"_id": 1, "age": 28, "job": "english teacher"}],
                "james": [{"_id": 0, "age": 35}],
            })),
            qry
        );
    }

    #[test]
    fn select_count_max_min_age_by_name() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": "age"},
                "minAge": {"min": "age"},
                "countAge": {"count": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "ania": {"maxAge": 28, "countAge": 2, "minAge": 20},
                "james": {"maxAge": 35, "countAge": 1, "minAge": 35},
                "misha": {"maxAge": 10, "countAge": 1, "minAge": 10}
            })),
            qry
        );
    }
}
