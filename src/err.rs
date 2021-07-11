use crate::json::Json;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    BadType,
    BadCmd,
    BadKey(String),
    ExpectedArr,
    BadFrom,
    Serialize,
    BadGroupBy,
    BadIO,
    BadArg(Json),
    IndexOutOfBounds,
    FloatCmp,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::BadCmd => write!(f, "bad cmd"),
            Error::BadType => write!(f, "incorrect type"),
            Error::BadKey(key) => write!(f, "bad key: {}", &key),
            Error::ExpectedArr => write!(f, "expected json array"),
            Error::BadFrom => write!(f, "bad from"),
            Error::Serialize => write!(f, "bad serialization"),
            Error::BadGroupBy => write!(f, "bad group by"),
            Error::BadIO => write!(f, "bad io"),
            Error::BadArg(msg) => write!(f, "{} is a bad argument", msg),
            Error::IndexOutOfBounds => write!(f, "index out of bounds"),
            Error::FloatCmp => write!(f, "float comparison"),
        }
    }
}
