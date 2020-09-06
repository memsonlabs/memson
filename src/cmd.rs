use crate::err::Error;
use crate::json::{json_eq, json_gt, json_gte, json_lt, json_lte, json_neq, Json, JsonObj};
use serde::{Deserialize, Serialize};
use serde_json::Map;
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
    #[serde(rename = "set")]
    Set(String, Box<Cmd>),
    #[serde(rename = "count")]
    Count(Box<Cmd>),
    #[serde(rename = "max")]
    Max(Box<Cmd>),
    #[serde(rename = "min")]
    Min(Box<Cmd>),
    #[serde(rename = "avg")]
    Avg(Box<Cmd>),
    #[serde(rename = "delete")]
    Delete(String),
    #[serde(rename = "dev")]
    StdDev(Box<Cmd>),
    #[serde(rename = "sum")]
    Sum(Box<Cmd>),
    #[serde(rename = "add")]
    Add(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "sub")]
    Sub(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "mul")]
    Mul(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "div")]
    Div(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "first")]
    First(Box<Cmd>),
    #[serde(rename = "last")]
    Last(Box<Cmd>),
    #[serde(rename = "var")]
    Var(Box<Cmd>),
    #[serde(rename = "pop")]
    Pop(String),
    #[serde(rename = "query")]
    Query(QueryCmd),
    #[serde(rename = "insert")]
    Insert(String, Json),
    #[serde(rename = "keys")]
    Keys(Option<usize>),
    #[serde(rename = "len")]
    Len,
    #[serde(rename = "unique")]
    Unique(Box<Cmd>),
    #[serde(rename = "json")]
    Json(Json),
    #[serde(rename = "summary")]
    Summary,
    #[serde(rename = "key")]
    Key(String),
}

impl Cmd {

    pub fn keys(&self) -> Option<Vec<String>> {
        match self {
            Cmd::Key(key) => Some(vec![key.clone()]),
            Cmd::Count(arg)
            | Cmd::Last(arg)
            | Cmd::Var(arg)
            | Cmd::Avg(arg)
            | Cmd::Min(arg)
            | Cmd::Max(arg)
            | Cmd::Sum(arg)
            | Cmd::First(arg)
            | Cmd::StdDev(arg)
            | Cmd::Unique(arg) => arg.keys(),
            Cmd::Add(lhs, rhs) | Cmd::Div(lhs, rhs) | Cmd::Mul(lhs, rhs) | Cmd::Sub(lhs, rhs) => {
                match (lhs.keys(), rhs.keys()) {
                    (Some(mut x), Some(y)) => {
                        x.extend(y);
                        Some(x)
                    }
                    (None, Some(v)) => Some(v),
                    (Some(v), None) => Some(v),
                    (None, None) => None,
                }
            }
            Cmd::Summary
            | Cmd::Len
            | Cmd::Keys(_)
            | Cmd::Insert(_, _)
            | Cmd::Query(_)
            | Cmd::Append(_, _)
            | Cmd::Delete(_)
            | Cmd::Pop(_)
            | Cmd::Json(_)
            | Cmd::Set(_, _) => None,
        }
    }

    pub fn is_mutate(&self) -> bool {
        match self {
            Cmd::Key(_) => false,
            Cmd::Add(_, _) => true,
            Cmd::Unique(_) => true,
            _ => false,
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
        _ => return Err(Error::BadCmd),
    }
}

impl Cmd {
    pub fn parse(json: Json) -> Result<Self, Error> {
        match json {
            Json::Object(obj) => {
                if obj.len() == 1 {
                    let (key, val) = obj.clone().into_iter().next().unwrap();
                    match key.as_ref() {
                        "add" => parse_bin_fn(val, Cmd::Add),
                        "append" => parse_b_str_fn(val, Cmd::Append),
                        "sum" => parse_unr_fn(val, Cmd::Sum),
                        "key" => {
                            let key = match val {
                                Json::String(key) => key,
                                _ => unimplemented!(),
                            };
                            Ok(Cmd::Key(key))
                        }
                        "first" => parse_unr_fn(val, Cmd::First),
                        "last" => parse_unr_fn(val, Cmd::Last),
                        "var" => parse_unr_fn(val, Cmd::Var),
                        "dev" => parse_unr_fn(val, Cmd::StdDev),
                        "div" => parse_bin_fn(val, Cmd::Div),
                        "avg" => parse_unr_fn(val, Cmd::Avg),
                        "max" => parse_unr_fn(val, Cmd::Max),
                        "min" => parse_unr_fn(val, Cmd::Min),
                        "mul" => parse_bin_fn(val, Cmd::Mul),
                        "len" => parse_unr_fn(val, Cmd::Count),
                        "query" => {
                            let qry_cmd = QueryCmd::parse(val)?;
                            Ok(Cmd::Query(qry_cmd))
                        }
                        "unique" => parse_unr_fn(val, Cmd::Unique),
                        "set" => parse_b_str_fn(val, Cmd::Set),
                        "sub" => parse_bin_fn(val, Cmd::Sub),
                        _ => Ok(Cmd::Json(Json::Object(obj))),
                    }
                } else {
                    Ok(Cmd::Json(Json::from(obj)))
                }
            }
            val => Ok(Cmd::Json(val)),
        }
    }

    pub fn is_aggregate(&self) -> bool {
        match self {
            Cmd::Avg(_)
            | Cmd::Count(_)
            | Cmd::Max(_)
            | Cmd::First(_)
            | Cmd::Min(_)
            | Cmd::StdDev(_)
            | Cmd::Last(_)
            | Cmd::Sum(_)
            | Cmd::Var(_)
            | Cmd::Last(_) => true,
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
    let val = json!({"sum": {"get": "k"}});
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
fn cmd_parse_get() {
    use serde_json::json;
    let val = json!({"get": "k"});
    let cmd = Cmd::parse(val.clone()).unwrap();
    assert_eq!(Cmd::Key("k".to_string()), cmd);
}

#[test]
fn cmd_parse_add() {
    use serde_json::json;
    let val = json!({"add": [{"get": "k"}, 1]});
    let cmd = Cmd::parse(val.clone()).unwrap();
    let exp = Cmd::Add(
        Box::new(Cmd::Key("k".to_string())),
        Box::new(Cmd::Json(json!(1))),
    );
    assert_eq!(exp, cmd);
}
