use crate::err::ParseError;
use crate::json::JsonObj;
use crate::query::Filter;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryCmd {
    #[serde(rename = "select")]
    pub selects: Option<Json>,
    pub from: String,
    pub by: Option<Json>,
    #[serde(rename = "where")]
    pub filter: Option<Filter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ReadCmd {
    #[serde(rename = "get")]
    Get(String),
    #[serde(rename = "query")]
    Query(QueryCmd),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WriteCmd {
    #[serde(rename = "set")]
    Set(String, Json),
    #[serde(rename = "insert")]
    Insert(String, Vec<JsonObj>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Cmd {
    #[serde(rename = "append")]
    Append(String, Json),
    #[serde(rename = "get")]
    Get(String),
    #[serde(rename = "set")]
    Set(String, Json),
    #[serde(rename = "len")]
    Count(String),
    #[serde(rename = "max")]
    Max(String),
    #[serde(rename = "min")]
    Min(String),
    #[serde(rename = "avg")]
    Avg(String),
    #[serde(rename = "dev")]
    Dev(String),
    #[serde(rename = "sum")]
    Sum(String),
    #[serde(rename = "add")]
    Add(String, String),
    #[serde(rename = "sub")]
    Sub(String, String),
    #[serde(rename = "mul")]
    Mul(String, String),
    #[serde(rename = "div")]
    Div(String, String),
    #[serde(rename = "first")]
    First(String),
    #[serde(rename = "last")]
    Last(String),
    #[serde(rename = "var")]
    Var(String),
    #[serde(rename = "pop")]
    Pop(String),
    #[serde(rename = "query")]
    Query(QueryCmd),
    #[serde(rename = "insert")]
    Insert(String, Json),
}

impl Cmd {
    pub fn sum(key: String) -> Cmd {
        Cmd::Sum(key)
    }

    pub fn get(key: String) -> Cmd {
        Cmd::Get(key)
    }

    pub fn count(key: String) -> Cmd {
        Cmd::Count(key)
    }

    pub fn max(key: String) -> Cmd {
        Cmd::Max(key)
    }

    pub fn min(key: String) -> Cmd {
        Cmd::Min(key)
    }

    pub fn avg(key: String) -> Cmd {
        Cmd::Avg(key)
    }

    pub fn dev(key: String) -> Cmd {
        Cmd::Dev(key)
    }

    pub fn first(key: String) -> Cmd {
        Cmd::First(key)
    }

    pub fn last(key: String) -> Cmd {
        Cmd::Last(key)
    }

    pub fn var(key: String) -> Cmd {
        Cmd::Var(key)
    }

    pub fn pop(key: String) -> Cmd {
        Cmd::Pop(key)
    }

    pub fn parse_cmd(json: Json) -> Result<Cmd, ParseError> {
        serde_json::from_value(json).map_err(|_| ParseError::BadCmd)
    }

    pub fn parse(json: Json) -> Result<Vec<Cmd>, ParseError> {
        match json {
            Json::String(s) => Ok(vec![Cmd::Get(s)]),
            Json::Array(arr) => {
                let mut cmds = Vec::new();
                for val in arr {
                    cmds.extend(Cmd::parse(val)?);
                }
                Ok(cmds)
            }
            Json::Object(obj) => {
                let mut cmds = Vec::new();
                for (key, val) in obj {
                    cmds.push(Self::parse_obj(key, val)?);
                }
                Ok(cmds)
            }
            _ => return Err(ParseError::BadCmd),
        }
    }

    pub fn parse_obj(key: String, arg: Json) -> Result<Cmd, ParseError> {
        match key.as_ref() {
            "sum" => Self::parse_arg(arg, &Cmd::sum),
            "first" => Self::parse_arg(arg, &Cmd::first),
            "last" => Self::parse_arg(arg, &Cmd::last),
            "pop" => Self::parse_arg(arg, &Cmd::pop),
            "var" => Self::parse_arg(arg, &Cmd::var),
            "dev" => Self::parse_arg(arg, &Cmd::dev),
            "avg" => Self::parse_arg(arg, &Cmd::avg),
            "max" => Self::parse_arg(arg, &Cmd::max),
            "min" => Self::parse_arg(arg, &Cmd::min),
            "len" => Self::parse_arg(arg, &Cmd::count),
            "get" => Self::parse_arg(arg, &Cmd::get),
            _ => unimplemented!(),
        }
    }

    fn parse_arg(arg: Json, f: &dyn Fn(String) -> Cmd) -> Result<Cmd, ParseError> {
        match arg {
            Json::String(key) => Ok(f(key)),
            val => return Err(ParseError::BadArgument(val)),
        }
    }

    pub fn is_mutation(&self) -> bool {
        match self {
            Cmd::Append(_, _) | Cmd::Set(_, _) | Cmd::Pop(_) | Cmd::Insert(_, _) => true,
            Cmd::Get(_)
            | Cmd::Count(_)
            | Cmd::Max(_)
            | Cmd::Min(_)
            | Cmd::Dev(_)
            | Cmd::Avg(_)
            | Cmd::Mul(_, _)
            | Cmd::Div(_, _)
            | Cmd::Add(_, _)
            | Cmd::Sub(_, _)
            | Cmd::Sum(_)
            | Cmd::First(_)
            | Cmd::Last(_)
            | Cmd::Var(_)
            | Cmd::Query(_) => false,
        }
    }

    pub fn is_aggregate(&self) -> bool {
        match self {
            Cmd::Avg(_)
            | Cmd::Count(_)
            | Cmd::Max(_)
            | Cmd::First(_)
            | Cmd::Min(_)
            | Cmd::Dev(_)
            | Cmd::Last(_)
            | Cmd::Sum(_)
            | Cmd::Var(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Response<'a> {
    Val(Json),
    Ref(&'a Json),
    Update,
    Insert(usize),
}

impl<'a> Response<'a> {
    pub fn to_string(&self) -> String {
        match self {
            Response::Val(val) => val.to_string(),
            Response::Ref(val) => val.to_string(),
            Response::Update => "Updated entry ".to_string(),
            Response::Insert(_) => "Inserted entry ".to_string(),
        }
    }
}
