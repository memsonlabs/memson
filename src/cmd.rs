use crate::err::Error;
use crate::json::{json_eq, json_gt, json_gte, json_lt, json_lte, json_neq, Json, JsonObj};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]

pub enum Filter {
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
    pub fn apply(&self, row: &JsonObj) -> bool {
        match self {
            Filter::Eq(key, val) => Filter::eval(row, key, val, &json_eq),
            Filter::NotEq(key, val) => Filter::eval(row, key, val, &json_neq),
            Filter::Lt(key, val) => Filter::eval(row, key, val, &json_lt),
            Filter::Gt(key, val) => Filter::eval(row, key, val, &json_gt),
            Filter::Lte(key, val) => Filter::eval(row, key, val, &json_lte),
            Filter::Gte(key, val) => Filter::eval(row, key, val, &json_gte),
            Filter::And(lhs, rhs) => Filter::eval_gate(row, lhs, rhs, &|x, y| x && y),
            Filter::Or(lhs, rhs) => Filter::eval_gate(row, lhs, rhs, &|x, y| x || y),
        }
    }

    fn eval_gate(
        row: &JsonObj,
        lhs: &Filter,
        rhs: &Filter,
        p: &dyn Fn(bool, bool) -> bool,
    ) -> bool {
        let lhs = lhs.apply(row);
        let rhs = rhs.apply(row);
        p(lhs, rhs)
    }

    fn eval(
        row: &JsonObj,
        key: &str,
        val: &Json,
        predicate: &dyn Fn(&Json, &Json) -> bool,
    ) -> bool {
        match row.get(key) {
            Some(v) => predicate(v, val),
            None => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryCmd {
    #[serde(rename = "select")]
    pub selects: Option<HashMap<String, Cmd>>,
    pub from: String,
    pub by: Option<Json>,
    #[serde(rename = "where")]
    pub filter: Option<Filter>,
}

impl QueryCmd {
    fn parse(json: Json) -> Result<Self, Error> {
        serde_json::from_value(json).map_err(|_| Error::Serialize)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Cmd {
    #[serde(rename = "append")]
    Append(String, Box<Cmd>),
    #[serde(rename = "bar")]
    Bar(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "set")]
    Set(String, Box<Cmd>),
    #[serde(rename = "max")]
    Max(Box<Cmd>),
    #[serde(rename = "min")]
    Min(Box<Cmd>),
    #[serde(rename = "avg")]
    Avg(Box<Cmd>),
    #[serde(rename = "del")]
    Delete(String),
    #[serde(rename = "dev")]
    StdDev(Box<Cmd>),
    #[serde(rename = "sum")]
    Sum(Box<Cmd>),
    #[serde(rename = "+")]
    Add(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "-")]
    Sub(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "*")]
    Mul(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "/")]
    Div(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "first")]
    First(Box<Cmd>),
    #[serde(rename = "last")]
    Last(Box<Cmd>),
    #[serde(rename = "var")]
    Var(Box<Cmd>),
    #[serde(rename = "push")]
    Push(String, Box<Cmd>),
    #[serde(rename = "pop")]
    Pop(String),
    #[serde(rename = "query")]
    Query(QueryCmd),
    #[serde(rename = "insert")]
    Insert(String, Vec<JsonObj>),
    #[serde(rename = "keys")]
    Keys(Option<usize>),
    #[serde(rename = "len")]
    Len(Box<Cmd>),
    #[serde(rename = "unique")]
    Unique(Box<Cmd>),
    #[serde(rename = "json")]
    Json(Json),
    #[serde(rename = "summary")]
    Summary,
    #[serde(rename = "get")]
    Get(String, Box<Cmd>),
    #[serde(rename = "key")]
    Key(String),
    #[serde(rename = "str")]
    ToString(Box<Cmd>),
    #[serde(rename = "sort")]
    Sort(Box<Cmd>),
    #[serde(rename = "reverse")]
    Reverse(Box<Cmd>), 
    #[serde(rename = "groupBy")]
    GroupBy(String, Box<Cmd>),  
    #[serde(rename = "median")]
    Median(Box<Cmd>),     
}

impl Cmd {
    pub fn is_read(&self) -> bool {
        match self {
            Cmd::Div(lhs, rhs) | Cmd::Mul(lhs, rhs) | Cmd::Bar(lhs, rhs) | Cmd::Add(lhs, rhs) | Cmd::Sub(lhs, rhs) => {
                let x = lhs.is_read();
                let y = rhs.is_read();
                x && y
            }
            Cmd::Reverse(arg) | Cmd::Median(arg) | Cmd::Sort(arg) | Cmd::GroupBy(_, arg) | Cmd::Last(arg) | Cmd::First(arg) | Cmd::Var(arg) | Cmd::Max(arg) | Cmd::Min(arg) | Cmd::StdDev(arg) | Cmd::Unique(arg) | Cmd::ToString(arg) | Cmd::Avg(arg) | Cmd::Get(_, arg) | Cmd::Sum(arg) => arg.is_read(),
            Cmd::Push(_, _) | Cmd::Pop(_) | Cmd::Delete(_) | Cmd::Append(_, _) | Cmd::Set(_, _) | Cmd::Insert(_, _) => false,
            Cmd::Key(_)
            | Cmd::Json(_)
            | Cmd::Summary
            | Cmd::Keys(_)
            | Cmd::Len(_)
            | Cmd::Query(_) => true,

        }
    }

    pub fn keys(&self) -> Option<Vec<String>> {
        match self {
            Cmd::Key(key) => Some(vec![key.clone()]),
            | Cmd::Last(arg)
            | Cmd::Var(arg)
            | Cmd::Avg(arg)
            | Cmd::Min(arg)
            | Cmd::Max(arg)
            | Cmd::Sum(arg)
            | Cmd::First(arg)
            | Cmd::StdDev(arg)
            | Cmd::Unique(arg)
            | Cmd::ToString(arg)
            | Cmd::Len(arg)
            | Cmd::GroupBy(_, arg)
            | Cmd::Reverse(arg)
            | Cmd::Median(arg)
            | Cmd::Sort(arg)
            | Cmd::Get(_, arg) => arg.keys(),
            Cmd::Bar(lhs, rhs)
            | Cmd::Add(lhs, rhs)
            | Cmd::Div(lhs, rhs)
            | Cmd::Mul(lhs, rhs)
            | Cmd::Sub(lhs, rhs) => match (lhs.keys(), rhs.keys()) {
                (Some(mut x), Some(y)) => {
                    x.extend(y);
                    Some(x)
                }
                (None, Some(v)) => Some(v),
                (Some(v), None) => Some(v),
                (None, None) => None,
            },
            Cmd::Summary
            | Cmd::Keys(_)
            | Cmd::Insert(_, _)
            | Cmd::Query(_)
            | Cmd::Append(_, _)
            | Cmd::Delete(_)
            | Cmd::Push(_, _)
            | Cmd::Pop(_)
            | Cmd::Json(_)
            | Cmd::Set(_, _) => None,
        }
    }
}

fn parse_bin_fn<F>(arg: Json, f: F) -> Result<Cmd, Error>
where
    F: FnOnce(Box<Cmd>, Box<Cmd>) -> Cmd,
{
    match arg {
        Json::Array(mut arr) => {
            if arr.len() != 2 {
                return Err(Error::BadCmd);
            }
            let rhs = Box::new(Cmd::parse(arr.pop().unwrap())?);
            let lhs = Box::new(Cmd::parse(arr.pop().unwrap())?);
            Ok(f(lhs, rhs))
        }
        _ => unimplemented!(),
    }
}

fn parse_unr_fn<F>(arg: Json, f: F) -> Result<Cmd, Error>
where
    F: FnOnce(Box<Cmd>) -> Cmd,
{
    let val = Cmd::parse(arg)?;
    Ok(f(Box::new(val)))
}

fn parse_b_str_fn<F>(val: Json, f: F) -> Result<Cmd, Error>
where
    F: FnOnce(String, Box<Cmd>) -> Cmd,
{
    match val {
        Json::Array(mut arr) => {
            if arr.len() != 2 {
                return Err(Error::BadCmd);
            }
            let arg = Cmd::parse(arr.pop().unwrap())?;
            let key = match arr.pop().unwrap() {
                Json::String(s) => s,
                _ => return Err(Error::BadCmd),
            };
            Ok(f(key, Box::new(arg)))
        }
        _ => Err(Error::BadCmd),
    }
}

fn parse_insert(val: Json) -> Result<Cmd, Error> 
{
    match val {
        Json::Array(mut arr) => {
            if arr.len() != 2 {
                return Err(Error::BadCmd);
            }
            let arg = arr.pop().unwrap();
            let key = match arr.pop().unwrap() {
                Json::String(s) => s,
                _ => return Err(Error::BadCmd),
            };
            let rows = match arg {
                Json::Array(arr) => {
                    let mut rows = Vec::new();
                    for val in arr {
                        match val {
                            Json::Object(obj) => rows.push(obj),
                            _ => return Err(Error::BadCmd),
                        }
                    }
                    rows
                }
                _ => return Err(Error::BadCmd),
            };
            Ok(Cmd::Insert(key, rows))
        }
        _ => Err(Error::BadCmd),
    }
}

fn parse_unr_str_fn<F>(arg: Json, f: F) -> Result<Cmd, Error> where F:FnOnce(String) -> Cmd {
    match arg {
        Json::String(s) => Ok(f(s)),
        _ => Err(Error::BadKey),
    }
}



impl Cmd {
    pub fn parse(json: Json) -> Result<Self, Error> {
        match json {
            Json::Object(obj) => {
                if obj.len() == 1 {
                    let (key, val) = obj.clone().into_iter().next().unwrap();
                    match key.as_ref() {
                        "+" | "add" => parse_bin_fn(val, Cmd::Add),                        
                        "append" => parse_b_str_fn(val, Cmd::Append),
                        "avg" => parse_unr_fn(val, Cmd::Avg),
                        "bar" => parse_bin_fn(val, Cmd::Bar),
                        "del" => parse_unr_str_fn(val, Cmd::Delete),
                        "dev" => parse_unr_fn(val, Cmd::StdDev),
                        "/" | "div" => parse_bin_fn(val, Cmd::Div),
                        "first" => parse_unr_fn(val, Cmd::First),
                        "get" => parse_b_str_fn(val, Cmd::Get),
                        "insert" => parse_insert(val),
                        "key" => parse_unr_str_fn(val, Cmd::Key),
                        "last" => parse_unr_fn(val, Cmd::Last),
                        "len" => parse_unr_fn(val, Cmd::Len),                                            
                        "max" => parse_unr_fn(val, Cmd::Max),
                        "median" => parse_unr_fn(val, Cmd::Median), 
                        "min" => parse_unr_fn(val, Cmd::Min),
                        "*" | "mul" => parse_bin_fn(val, Cmd::Mul),
                        "pop" => parse_unr_str_fn(val, Cmd::Pop),
                        "push" => parse_b_str_fn(val, Cmd::Push),
                        "query" => {
                            let qry_cmd = QueryCmd::parse(val)?;
                            Ok(Cmd::Query(qry_cmd))
                        }
                        "set" => parse_b_str_fn(val, Cmd::Set),
                        "sub" | "-" => parse_bin_fn(val, Cmd::Sub),
                        "sum" => parse_unr_fn(val, Cmd::Sum),
                        "str" => parse_unr_fn(val, Cmd::ToString),  
                        "unique" => parse_unr_fn(val, Cmd::Unique),
                        "var" => parse_unr_fn(val, Cmd::Var),                    
                        _ => Ok(Cmd::Json(Json::Object(obj))),
                    }
                } else {
                    Ok(Cmd::Json(Json::from(obj)))
                }
            }
            Json::String(s) => Ok(if s == "summary" {Cmd::Summary} else { Cmd::Json(Json::from(s)) }),
            val => Ok(Cmd::Json(val)),
        }
    }

    pub fn is_aggregate(&self) -> bool {
        match self {
            Cmd::Avg(_)

            | Cmd::Max(_)
            | Cmd::First(_)
            | Cmd::Min(_)
            | Cmd::StdDev(_)
            | Cmd::Last(_)
            | Cmd::Sum(_)
            | Cmd::Var(_)
            | Cmd::Unique(_)
            | Cmd::Len(_) => true,
            _ => false,
        }
    }
}

#[test]
fn cmd_parse_json_string() {
    use serde_json::json;
    let val = json!("abc");
    let cmd = Cmd::parse(val.clone()).unwrap();
    assert_eq!(Cmd::Json(val), cmd);
}

#[test]
fn cmd_parse_sum() {
    use serde_json::json;
    let val = json!({"sum": {"key": "k"}});
    let cmd = Cmd::parse(val.clone()).unwrap();
    assert_eq!(Cmd::Sum(Box::new(Cmd::Key("k".to_string()))), cmd);
}

#[test]
fn cmd_parse_json_int() {
    use serde_json::json;
    let val = json!(1);
    let cmd = Cmd::parse(val.clone()).unwrap();
    assert_eq!(Cmd::Json(val), cmd);
}

#[test]
fn cmd_parse_key() {
    use serde_json::json;
    let val = json!({"key": "k"});
    let cmd = Cmd::parse(val.clone()).unwrap();
    assert_eq!(Cmd::Key("k".to_string()), cmd);
}

#[test]
fn cmd_parse_add() {
    use serde_json::json;
    let val = json!({"add": [{"key": "k"}, 1]});
    let cmd = Cmd::parse(val.clone()).unwrap();
    let exp = Cmd::Add(
        Box::new(Cmd::Key("k".to_string())),
        Box::new(Cmd::Json(json!(1))),
    );
    assert_eq!(exp, cmd);
}
