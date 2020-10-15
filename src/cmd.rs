use crate::err::Error;
use crate::json::{Json, JsonObj};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub type Cache = BTreeMap<String, Json>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryCmd {
    #[serde(rename = "select")]
    pub selects: Option<HashMap<String, Cmd>>,
    pub from: String,
    pub by: Option<Box<Cmd>>,
    #[serde(rename = "where")]
    pub filter: Option<Json>,
    #[serde(rename = "sort")]
    pub sort: Option<String>,
    pub descend: Option<bool>,
}

impl QueryCmd {
    fn parse(json: Json) -> crate::Result<Self> {
        serde_json::from_value(json).map_err(|_| Error::Serialize)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Range {
    pub start: Option<usize>,
    pub size: Option<usize>,
}

impl Range {
    pub fn has_indices(&self) -> bool {
        self.start.is_some() || self.size.is_some()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Cmd {
    #[serde(rename = "append")]
    Append(String, Box<Cmd>),
    #[serde(rename = "apply")]
    Apply(Box<Cmd>, Box<Cmd>),
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
    Keys(Option<Range>),
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
    Sort(Box<Cmd>, Option<bool>),
    #[serde(rename = "reverse")]
    Reverse(Box<Cmd>),
    #[serde(rename = "sortBy")]
    SortBy(Box<Cmd>, String),
    #[serde(rename = "median")]
    Median(Box<Cmd>),
    #[serde(rename = "eval")]
    Eval(Vec<Cmd>),
    #[serde(rename = "==")]
    Eq(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "!=")]
    NotEq(Box<Cmd>, Box<Cmd>),
    #[serde(rename = ">")]
    Gt(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "<")]
    Lt(Box<Cmd>, Box<Cmd>),
    #[serde(rename = ">=")]
    Gte(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "<=")]
    Lte(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "&&")]
    And(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "||")]
    Or(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "map")]
    Map(Box<Cmd>, String),
    #[serde(rename = "in")]
    In(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "flat")]
    Flat(Box<Cmd>),
    #[serde(rename = "numSort")]
    NumSort(Box<Cmd>, bool),
    #[serde(rename = "has")]
    Has(String),
    #[serde(rename = "slice")]
    Slice(Box<Cmd>, Range),
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
        _ => Err(Error::BadCmd),
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

fn parse_insert(val: Json) -> Result<Cmd, Error> {
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

fn parse_unr_str_fn<F>(arg: Json, f: F) -> Result<Cmd, Error>
where
    F: FnOnce(String) -> Cmd,
{
    match arg {
        Json::String(s) => Ok(f(s)),
        val => Err(Error::BadArg(val)),
    }
}

impl Cmd {
    pub fn parse_line(line: &str) -> Result<Self, Error> {
        let val = serde_json::from_str(line).map_err(|_| Error::BadIO)?;
        Self::parse(val)
    }

    pub fn parse(json: Json) -> Result<Self, Error> {
        match json {
            Json::Object(obj) => {
                if obj.len() == 1 {
                    let (key, mut val): (String, Json) = obj.clone().into_iter().next().unwrap();
                    match key.as_ref() {
                        "&&" => parse_bin_fn(val, Cmd::And),
                        "==" => parse_bin_fn(val, Cmd::Eq),
                        "!=" => parse_bin_fn(val, Cmd::NotEq),
                        ">" => parse_bin_fn(val, Cmd::Gt),
                        "<" => parse_bin_fn(val, Cmd::Lt),
                        ">=" => parse_bin_fn(val, Cmd::Gte),
                        "<=" => parse_bin_fn(val, Cmd::Lte),
                        "||" => parse_bin_fn(val, Cmd::Or),
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
                        "has" => parse_unr_str_fn(val, Cmd::Has),
                        "last" => parse_unr_fn(val, Cmd::Last),
                        "len" => parse_unr_fn(val, Cmd::Len),
                        "flat" => parse_unr_fn(val, Cmd::Flat),
                        "map" => {
                            let arr = val.as_array_mut().ok_or(Error::ExpectedArr)?;
                            let val = arr.remove(1);
                            let val_str = val.as_str().map(|x| x.to_string());
                            let f = val_str.ok_or(Error::BadArg(val))?;
                            let arg = Cmd::parse(arr.remove(0))?;
                            Ok(Cmd::Map(Box::new(arg), f))
                        }
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
                        "sort" => match val {
                            Json::Array(mut arr) => {
                                if arr.len() != 2 {
                                    Err(Error::BadCmd)
                                } else {
                                    let s = arr.remove(1);
                                    let a = arr.remove(0);
                                    let arg = Cmd::parse(a)?;
                                    let interval = s.as_bool().ok_or(Error::BadType)?;
                                    Ok(Cmd::Sort(
                                        Box::new(arg),
                                        if interval { Some(interval) } else { None },
                                    ))
                                }
                            }
                            val => {
                                let arg = Cmd::parse(val)?;
                                Ok(Cmd::Sort(Box::new(arg), None))
                            }
                        },
                        _ => Ok(Cmd::Json(Json::Object(obj))),
                    }
                } else {
                    Ok(Cmd::Json(Json::from(obj)))
                }
            }
            Json::String(s) => Ok(if s == "summary" {
                Cmd::Summary
            } else {
                Cmd::Json(Json::from(s))
            }),
            val => Ok(Cmd::Json(val)),
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

#[test]
fn cmd_parse_sort_descend() {
    use serde_json::json;
    let val = json!({"sort": [{"key": "k"}, true]});
    let cmd = Cmd::parse(val.clone()).unwrap();
    let exp = Cmd::Sort(Box::new(Cmd::Key("k".to_string())), Some(true));
    assert_eq!(exp, cmd);
}

#[test]
fn cmd_parse_sort_ascend_arr() {
    use serde_json::json;
    let val = json!({"sort": [{"key": "k"}, false]});
    let cmd = Cmd::parse(val.clone()).unwrap();
    let exp = Cmd::Sort(Box::new(Cmd::Key("k".to_string())), None);
    assert_eq!(exp, cmd);
}

#[test]
fn cmd_parse_sort_ascend() {
    use serde_json::json;
    let val = json!({"sort": {"key": "k"}});
    let cmd = Cmd::parse(val.clone()).unwrap();
    let exp = Cmd::Sort(Box::new(Cmd::Key("k".to_string())), None);
    assert_eq!(exp, cmd);
}

#[test]
fn cmd_parse_map() {
    use serde_json::json;
    let val = json!({"map": [{"key": "k"}, "len"]});
    let cmd = Cmd::parse(val.clone()).unwrap();
    let exp = Cmd::Map(Box::new(Cmd::Key("k".to_string())), "len".to_string());
    assert_eq!(exp, cmd);
}
