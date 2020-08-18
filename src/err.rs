use serde_json::value::Value;

pub enum ParseError {
    BadArgument(Value),
    BadCmd,
}

#[derive(Debug, PartialEq)]
pub enum Error {
    BadIO,
    BadType,
    BadState,
    EmptySequence,
    BadNumber,
    UnknownKey(String),
    ExpectedObj,
    FloatCmp,
    BadKey,
    BadFrom,
    BadSelect,
    BadObject,
    BadWhere,
    NotArray,
    NotAggregate,
}

impl Error {
    // TODO provide more details in errors
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
}
