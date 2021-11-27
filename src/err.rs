use crate::json::Json;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use actix_web::ResponseError;
use serde::Serialize;
use std::fmt;

#[derive(Serialize)]
pub struct ErrorResponse {
    code: u16,
    error: String,
    message: String,
}

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

impl ResponseError for Error {
    fn status_code(&self) -> StatusCode {
        match *self {
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let status_code = self.status_code();
        let error_response = ErrorResponse {
            code: status_code.as_u16(),
            message: self.to_string(),
            error: "system".to_string(),
        };
        HttpResponse::build(status_code).json(error_response)
    }
}
