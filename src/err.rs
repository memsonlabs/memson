use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    BadType,
    BadCmd,
    BadKey,
    ExpectedArr,
    BadFrom,
    Serialize,
    BadGroupBy,
    BadIO,
    BadArg,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Error::BadCmd => "bad cmd",
            Error::BadType => "incorrect type",
            Error::BadKey => "bad key",
            Error::ExpectedArr => "expected json array",
            Error::BadFrom => "bad from",
            Error::Serialize => "bad serialization",
            Error::BadGroupBy => "bad group by",
            Error::BadIO => "bad io",
            Error::BadArg => "bad argument",
        };
        write!(f, "{}", "error: ".to_string() + msg)
    }
}
