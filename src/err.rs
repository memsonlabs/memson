use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Error {
    BadType,
    BadCmd,
    BadKey,
    ExpectedObj,
    ExpectedArr,
    BadFrom,
    BadSelect,
    BadObject,
    Serialize,
}

impl fmt::Display for Error {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            Error::BadCmd => "bad command",
            Error::BadType => "incorrect type",
            Error::BadKey => "bad key",
            Error::ExpectedObj => "expected object",
            Error::ExpectedArr => "expected json array",
            Error::BadObject => "bad object",
            Error::BadFrom => "bad from",
            Error::BadSelect => "bad select",
            Error::Serialize => "bad serialization",
        };
        write!(f, "{}", "error: ".to_string() + msg)
    }
}
