use std::{cmp, fmt};

pub enum ParseError {
    BadCmd,
}

#[derive(Debug)]
pub enum Error {
    BadType,
    BadInsert,
    BadCmd,
    BadKey,
    BadNumber,
    UnknownKey(String),
    ExpectedObj,
    ExpectedArr,
    BadFrom,
    BadSelect,
    BadObject,
    NotAggregate,
    Sled(sled::Error),
    Serialize,
}

impl fmt::Display for Error {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            Error::BadCmd => "bad command",
            Error::BadType => "incorrect type",
            Error::BadKey => "bad key",
            Error::BadNumber => "bad number",
            Error::UnknownKey(_) => "unknown key",
            Error::ExpectedObj => "expected object",
            Error::ExpectedArr => "expected json array",
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

impl cmp::PartialEq for Error {
    fn eq(&self, rhs: &Error) -> bool {
        match (self, rhs) {
            (Error::BadFrom, Error::BadFrom) => true,
            (Error::BadInsert, Error::BadInsert) => true,
            (Error::BadNumber, Error::BadNumber) => true,
            (Error::BadObject, Error::BadObject) => true,
            (Error::BadSelect, Error::BadSelect) => true,
            (Error::BadType, Error::BadType) => true,
            (Error::ExpectedArr, Error::ExpectedArr) => true,
            (Error::ExpectedObj, Error::ExpectedObj) => true,
            (Error::NotAggregate, Error::NotAggregate) => true,
            (Error::Serialize, Error::Serialize) => true,
            (Error::Sled(_), Error::Sled(_)) => true,
            (Error::UnknownKey(x), Error::UnknownKey(y)) => x == y,
            _ => false,
        }
    }
}
