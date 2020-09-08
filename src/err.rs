use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    BadType,
    BadCmd,
    BadKey,
    BadBy,
    ExpectedObj,
    ExpectedArr,
    BadFrom,
    Serialize,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            Error::BadBy => "bad group by",
            Error::BadCmd => "bad command",
            Error::BadType => "incorrect type",
            Error::BadKey => "bad key",
            Error::ExpectedObj => "expected object",
            Error::ExpectedArr => "expected json array",
            Error::BadFrom => "bad from",
            Error::Serialize => "bad serialization",
        };
        write!(f, "{}", "error: ".to_string() + msg)
    }
}
