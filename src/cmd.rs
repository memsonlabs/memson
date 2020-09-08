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
    pub by: Option<Box<Cmd>>,
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
    Len,
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
    #[serde(rename = "toString")]
    ToString(Box<Cmd>),
}

impl Cmd {
    pub fn is_read(&self) -> bool {
        match self {
            Cmd::Div(lhs, rhs) | Cmd::Mul(lhs, rhs) | Cmd::Bar(lhs, rhs) | Cmd::Add(lhs, rhs) | Cmd::Sub(lhs, rhs) => {
                let x = lhs.is_read();
                let y = rhs.is_read();
                x && y
            }
            Cmd::Last(arg) | Cmd::First(arg) | Cmd::Var(arg) | Cmd::Max(arg) | Cmd::Min(arg) | Cmd::Count(arg) | Cmd::StdDev(arg) | Cmd::Unique(arg) | Cmd::ToString(arg) | Cmd::Avg(arg) | Cmd::Get(_, arg) | Cmd::Sum(arg) => arg.is_read(),
            Cmd::Json(_) => true,
            Cmd::Key(_) => true,
            Cmd::Summary => true,
            Cmd::Keys(_) => true,
            Cmd::Push(_, _) | Cmd::Pop(_) | Cmd::Delete(_) | Cmd::Append(_, _) | Cmd::Set(_, _) | Cmd::Insert(_, _) => false,
            Cmd::Len => true,
            Cmd::Query(_) => true,
        }
    }

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
            | Cmd::Unique(arg)
            | Cmd::ToString(arg)
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
            | Cmd::Len
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
                        "+" | "add" => parse_bin_fn(val, Cmd::Add),
                        "append" => parse_b_str_fn(val, Cmd::Append),
                        "sum" => parse_unr_fn(val, Cmd::Sum),
                        "get" => parse_b_str_fn(val, Cmd::Get),
                        "first" => parse_unr_fn(val, Cmd::First),
                        "last" => parse_unr_fn(val, Cmd::Last),
                        "var" => parse_unr_fn(val, Cmd::Var),
                        "dev" => parse_unr_fn(val, Cmd::StdDev),
                        "/" | "div" => parse_bin_fn(val, Cmd::Div),
                        "avg" => parse_unr_fn(val, Cmd::Avg),
                        "max" => parse_unr_fn(val, Cmd::Max),
                        "min" => parse_unr_fn(val, Cmd::Min),
                        "*" | "mul" => parse_bin_fn(val, Cmd::Mul),
                        "query" => {
                            let qry_cmd = QueryCmd::parse(val)?;
                            Ok(Cmd::Query(qry_cmd))
                        }
                        "unique" => parse_unr_fn(val, Cmd::Unique),
                        "set" => parse_b_str_fn(val, Cmd::Set),
                        "sub" | "-" => parse_bin_fn(val, Cmd::Sub),
                        "key" => match val {
                            Json::String(s) => Ok(Cmd::Key(s)),
                            _ => Err(Error::BadKey),
                        },
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
            | Cmd::Unique(_)
            | Cmd::Len => true,
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
