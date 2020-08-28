use crate::err::ParseError;
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
pub enum Cmd {
    #[serde(rename = "append")]
    Append(String, Box<Cmd>),
    #[serde(rename = "get")]
    Get(String),
    #[serde(rename = "set")]
    Set(String, Box<Cmd>),
    #[serde(rename = "len")]
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
}

impl Cmd {    
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
            _ => Err(ParseError::BadCmd),
        }
    }

    pub fn parse_obj(key: String, arg: Json) -> Result<Cmd, ParseError> {
        match key.as_ref() {
            "sum" => Self::parse_arg(arg, &Cmd::Sum),
            "first" => Self::parse_arg(arg, &Cmd::First),
            "last" => Self::parse_arg(arg, &Cmd::Last),
            "pop" => Self::parse_arg_str(arg, &Cmd::Pop),
            "var" => Self::parse_arg(arg, &Cmd::Var),
            "dev" => Self::parse_arg(arg, &Cmd::StdDev),
            "avg" => Self::parse_arg(arg, &Cmd::Avg),
            "max" => Self::parse_arg(arg, &Cmd::Max),
            "min" => Self::parse_arg(arg, &Cmd::Min),
            "len" => Self::parse_arg(arg, &Cmd::Count),
            "get" => Self::parse_arg_str(arg, &Cmd::Get),
            _ => unimplemented!(),
        }
    }

    fn parse_arg_str(arg: Json, f: &dyn Fn(String) -> Cmd) -> Result<Cmd, ParseError> {
        match arg {
            Json::String(key) => Ok(f(key)),
            val => Err(ParseError::BadArgument(val)),
        }
    }

    fn parse_arg(arg: Json, f: &dyn Fn(Box<Cmd>) -> Cmd) -> Result<Cmd, ParseError> {
        match arg {
            Json::Object(obj) => {
                for (_, val) in &obj {
                    match val {
                        Json::String(s) => return Ok(f(Box::new(Cmd::Get(s.to_string())))),
                        val => return Err(ParseError::BadArgument(val.clone())),
                    }
                }
                Err(ParseError::BadArgument(Json::from(obj)))
            },
            val => Err(ParseError::BadArgument(val)),
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
            | Cmd::Var(_) => true,
            _ => false,
        }
    }
}
