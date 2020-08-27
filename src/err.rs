use serde_json::value::Value;
use std::{fmt, io};

pub enum ParseError {
    BadArgument(Value),
    BadCmd,
}

#[derive(Debug)]
pub enum Error {
    BadIO(io::Error),
    BadType,
    BadInsert,
    EmptySequence,
    BadNumber,
    UnknownKey(String),
    ExpectedObj,
    ExpectedArr,
    FloatCmp,
    BadKey,
    BadFrom,
    BadSelect,
    BadObject,
    NotAggregate,
    Sled(sled::Error),

    Serialize,
}

impl Error {
    // TODO provide more details in errors
    /*
    pub fn to_string(&self) -> String {
        let msg = match self {
            Error::BadIO => "bad io",
            Error::BadType => "incorrect type",
            Error::BadState => "incorrect internal state",
            Error::EmptySequence => "empty sequence",
            Error::BadNumber => "bad number",
            Error::UnknownKey(_) => "unknown key",
            Error::ExpectedObj => "expected object",
            Error::FloatCmp => "float comparison",

            _ => unimplemented!(),
        };
        "error: ".to_string() + msg
    }
    */
}

impl fmt::Display for Error {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            Error::BadIO(_) => "bad io",
            Error::BadType => "incorrect type",
            Error::EmptySequence => "empty sequence",
            Error::BadNumber => "bad number",
            Error::UnknownKey(_) => "unknown key",
            Error::ExpectedObj => "expected object",
            Error::FloatCmp => "float comparison",
            Error::ExpectedArr => "expected json array",
            Error::BadKey => "bad key",
            Error::NotAggregate => "not aggregate",
            Error::BadInsert => "bad insert",
            Error::BadObject => "bad object",
            Error::BadFrom => "bad from",
            Error::BadSelect => "bad select",
            Error::Sled(_) => "bad persistence",
            Error::Serialize => "bad serialization",
        };
        write!(f, "{}", "error: ".to_string() + msg)
    }
}
