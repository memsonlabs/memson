use crate::db::{Cmd,Db};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use crate::json::{json_eq, json_gt, json_gte, json_lt, json_lte, json_neq, json_add_num, json_max};

type JsonObj = Map<String, Json>;

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryCmd {
    #[serde(rename = "select")]
    selects: Option<Json>,
    from: String,
    #[serde(rename = "where")]
    filter: Option<Filter>
}

impl QueryCmd {
    pub fn eval(self, db: &Db) -> Result<Json, Error> {
        let qry = Query::from(db, self);
        qry.eval()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    BadFrom,
    BadSelect,
    BadObject,
    BadWhere,
    NotArray,
    BadJson,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
enum Filter {
    #[serde(rename = "==")]
    Eq(String, Json),
    #[serde(rename = "!=")]
    NotEq(String, Json),
    #[serde(rename = ">")]
    Gt(String, Json),
    #[serde(rename = "<")]
    Lt(String, Json),
    #[serde(rename = ">=")]
    Gte(String, Json),
    #[serde(rename = "<=")]
    Lte(String, Json),
    #[serde(rename = "&&")]
    And(Box<Filter>, Box<Filter>),
    #[serde(rename = "||")]
    Or(Box<Filter>, Box<Filter>),
}


impl Filter {
    fn apply(&self, row: &Json) -> Result<bool, Error> {
        match self {
            Filter::Eq(key, val) => Filter::eval(row, key, val, &json_eq),
            Filter::NotEq(key, val) => Filter::eval(row, key, val, &json_neq),
            Filter::Lt(key, val) => Filter::eval(row, key, val, &json_lt),
            Filter::Gt(key, val) => Filter::eval(row, key, val, &json_gt),
            Filter::Lte(key, val) => Filter::eval(row, key, val, &json_lte),
            Filter::Gte(key, val) => Filter::eval(row, key, val, &json_gte),
            Filter::And(lhs, rhs) => Filter::eval_gate(row, lhs, rhs, &|x,y| x && y),
            Filter::Or(lhs, rhs) => Filter::eval_gate(row, lhs, rhs, &|x,y| x || y),
        }
    }

    fn eval_gate(row: &Json, lhs: &Filter, rhs: &Filter, p: &dyn Fn(bool, bool) -> bool) -> Result<bool, Error> {
        let lhs = lhs.apply(row)?;
        let rhs = rhs.apply(row)?;
        Ok(p(lhs, rhs))
    }

    fn eval(row: &Json, key: &str, val: &Json, predicate: &dyn Fn(&Json, &Json) -> Option<bool>) ->  Result<bool, Error> {
        match row {
            Json::Object(obj) => match obj.get(key) {
                Some(v) => predicate(v, val).ok_or(Error::BadWhere),
                None => Ok(false),
            }
            v => predicate(v, val).ok_or(Error::BadWhere),
        }
    }
}

//TODO pass vec to remove clone
fn parse_selects_to_cmd(selects: &[Json]) -> Result<Vec<Cmd>, Error> {
    let mut cmds = Vec::new();
    for select in selects {
        cmds.extend(Cmd::parse(select.clone()).map_err(|_| Error::BadSelect)?);
    }
    Ok(cmds)
}

fn eval_aggregate(cmd: Cmd, rows: &[Json]) -> Result<Json, Error> {
    match cmd {
        Cmd::Sum(key) => eval_sum(&key, rows),
        Cmd::Max(key) => eval_max(&key, rows),
        Cmd::Min(key) => eval_min(&key, rows),
        //Cmd::Avg(key) => eval_avg(&key, rows),
        _ => unimplemented!(),
    }
}

fn eval_sum(key: &str, rows: &[Json]) -> Result<Json, Error> {
    let mut total = Json::from(0);
    for row in rows {
        match row.get(key) {
            Some(Json::Number(n))  => {
                if let Some(val) = json_add_num(&total, n) {
                    total = val;
                }
            }
            _ => continue,
        }
    }
    Ok(Json::from(total))
}

fn eval_max(key: &str, rows: &[Json]) -> Result<Json, Error> {
    let mut max_val = None;
    for row in rows {
        if let Some(val) = row.get(key) {
            max_val = match max_val {
                None => Some(val.clone()),
                Some(cur_max) => {
                    let val = json_max(&cur_max, val).ok_or(Error::BadSelect)?;
                    Some(val)
                },
            }
        }
    }
    let val = if let Some(val) = max_val { val } else { Json::Null };
    Ok(val)
}

struct Query<'a> {
    db: &'a Db,
    cmd: QueryCmd,
}

impl<'a> Query<'a> {
    fn from(db: &'a Db, cmd: QueryCmd) -> Self {
        Self { db, cmd }
    }

    fn eval(&self) -> Result<Json, Error> {
        let rows = self.eval_from()?;
        if let Some(ref filter) = self.cmd.filter {
            let filtered_rows = self.eval_where(rows, filter.clone())?;
            println!("filtered_rows = {:?}", filtered_rows);
            self.eval_select(&filtered_rows)
        } else {
            self.eval_select(rows)
        }        
    }

    fn eval_where(&self, rows: &[Json], filter: Filter) -> Result<Vec<Json>, Error> {
        let mut filtered_rows = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            if filter.apply(row)? {
                let obj_row = add_row_id(row, i)?;
                filtered_rows.push(obj_row);
            }
        }
        Ok(filtered_rows)
    }

    fn eval_from(&self) -> Result<&'a [Json], Error> {
        //TODO remove cloning
        match self.db.get(self.cmd.from.to_string()) {
            Ok(Json::Array(rows)) => Ok(rows),
            Ok(_) => return Err(Error::NotArray),
            Err(_) => return Err(Error::BadFrom),
        }
    }

    // TODO remove cloning
    fn eval_select(&self, rows: &[Json]) -> Result<Json, Error> {
        match &self.cmd.selects {
            Some(Json::Array(ref selects)) => self.eval_selects(selects, rows),
            Some(Json::String(s)) => self.eval_selects(&vec![Json::from(s.to_string())], rows),
            Some(Json::Object(obj)) => self.eval_obj_selects(obj.clone(), rows),
            Some(val) => {                
                println!("bad select = {:?}", val);
                Err(Error::BadSelect)
            }
            None => self.eval_select_all(rows),
        }
    }

    fn eval_obj_selects(&self, obj: JsonObj, rows: &[Json]) -> Result<Json, Error> {        
        let mut out = JsonObj::new();        
        for (key, val) in obj {
            let cmd = Cmd::parse_cmd(val).map_err(|_| Error::BadSelect)?;
            println!("cmd={:?}", cmd);
            out.insert(key, eval_aggregate(cmd, rows)?);
            println!("out={:?}", out);
        }
        Ok(Json::Object(out))
    }

    //TODO remove clones
    fn eval_selects(&self, selects: &[Json], rows: &[Json]) -> Result<Json, Error> {
        if selects.is_empty() {
            return self.eval_select_all(rows);
        }
        let mut output = Vec::new();
        let cmds = parse_selects_to_cmd(selects)?;
        for (i, row) in rows.iter().enumerate() {
            let mut map = JsonObj::new();
            for cmd in &cmds {
                eval_row_cmd(cmd, row, &mut map)?;                
            }
            if !map.is_empty() {
                map.insert("_id".to_string(), Json::from(i));
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
        Cmd::Get(key) => {
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
            if let None = obj.get("_id") {
                obj.insert("_id".to_string(), Json::from(i));
            }
            Ok(Json::from(obj))
        }
        _ => Err(Error::BadObject),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use lazy_static::lazy_static;
    use serde_json::json;
    use std::sync::{RwLockReadGuard,RwLock};
    use crate::db::tests::insert;

    lazy_static! {
        static ref DB: RwLock<Db> = {
            let mut db = Db::open("query").unwrap();
            insert(&mut db, 
                "t",
                Json::from(vec![
                    json!({"name": "james", "age": 35}),
                    json!({"name": "ania", "age": 28, "job": "English Teacher"}),
                    json!({"name": "misha", "age": 10}),
                ]),
            );
            RwLock::new(db)
        };
        static ref DB_REF: RwLockReadGuard<'static, Db> = DB.try_read().unwrap();
    }

    fn query(json: Json) -> Query<'static> {
        let cmd = serde_json::from_value(json).unwrap();
        Query::from(&DB_REF, cmd)
    }    

    #[test]
    fn select_all_query() {
        let qry = query(json!({"from": "t"}));
        let val = Json::from(vec![
            json!({"_id": 0, "name": "james", "age": 35}),
            json!({"_id": 1, "name": "ania", "age": 28, "job": "English Teacher"}),
            json!({"_id": 2, "name": "misha", "age": 10}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }

    #[test]
    fn select_1_prop_query() {
        let qry = query(json!({"select": "name", "from": "t"}));
        let val = Json::from(vec![
            json!({"_id": 0, "name": "james"}),
            json!({"_id": 1, "name": "ania"}),
            json!({"_id": 2, "name": "misha"}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }
    #[test]
    fn select_3_prop_query() {
        let qry = query(json!({"select": ["name", "age", "job"], "from": "t"}));
        let val = Json::from(vec![
            json!({"_id": 0, "name": "james", "age": 35}),
            json!({"_id": 1, "name": "ania", "age": 28, "job": "English Teacher"}),
            json!({"_id": 2, "name": "misha", "age": 10}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }

    #[test]
    fn select_all_where_eq_query() {
        let qry = query(json!({"from": "t", "where": {"==": ["name", "james"]}}));
        let val = Json::from(vec![
            json!({"_id": 0, "name": "james", "age": 35}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }

    #[test]
    fn select_all_where_neq_query_ok() {
        let qry = query(json!({"from": "t", "where": {"!=": ["name", "james"]}}));
        let val = Json::from(vec![
            json!({"_id": 1, "name": "ania", "age": 28, "job": "English Teacher"}),
            json!({"_id": 2, "name": "misha", "age": 10}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }

    #[test]
    fn select_all_where_gt_query_ok() {
        let qry = query(json!({"from": "t", "where": {">": ["age", 20]}}));
        let val = Json::from(vec![
            json!({"_id": 0, "name": "james", "age": 35}),
            json!({"_id": 1, "name": "ania", "age": 28, "job": "English Teacher"}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }
    
    #[test]
    fn select_all_where_lt_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<": ["age", 20]}}));
        let val = Json::from(vec![
            json!({"_id": 2, "name": "misha", "age": 10}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }  
    
    #[test]
    fn select_all_where_lte_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<=": ["age", 28]}}));
        let val = Json::from(vec![
            json!({"_id": 1, "name": "ania", "age": 28, "job": "English Teacher"}),
            json!({"_id": 2, "name": "misha", "age": 10}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }  
    
    #[test]
    fn select_all_where_gte_query_ok() {
        let qry = query(json!({"from": "t", "where": {">=": ["age", 28]}}));
        let val = Json::from(vec![
            json!({"_id": 0, "name": "james", "age": 35}),
            json!({"_id": 1, "name": "ania", "age": 28, "job": "English Teacher"}),
        ]);

        assert_eq!(Ok(val), qry.eval());
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
        let val = Json::from(vec![
            json!({"_id": 1, "name": "ania", "age": 28, "job": "English Teacher"}),
        ]);
        assert_eq!(Ok(val), qry.eval());
    }  
    
    #[test]
    fn select_sum_ok() {
        let qry = query(json!({
            "select": {"totalAge": {"sum": "age"}},
            "from": "t"
        }));
        let val = Json::from(json!({"totalAge": 73}));
        assert_eq!(Ok(val), qry.eval());
    }

    #[test]
    fn select_max_num_ok() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": "age"}
            },
            "from": "t"
        }));
        let val = Json::from(json!({"maxAge": 35}));
        assert_eq!(Ok(val), qry.eval());
    }

    #[test]
    fn select_max_str_ok() {
        let qry = query(json!({
            "select": {
                "maxName": {"max": "name"}
            },
            "from": "t"
        }));
        let val = Json::from(json!({"maxName": "misha"}));
        assert_eq!(Ok(val), qry.eval());
    }

    #[test]
    fn select_min_num_ok() {
        let qry = query(json!({
            "select": {
                "minAge": {"min": "age"}
            },
            "from": "t"
        }));
        let val = Json::from(json!({"minAge": 10}));
        assert_eq!(Ok(val), qry.eval());
    }
}
