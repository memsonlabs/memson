use crate::cmd::{Cmd, QueryCmd};
use crate::db::Memson;
use crate::err::Error;
use crate::json::{Json, JsonVal};
use serde::Serialize;
use std::env;
use std::fmt::Debug;

pub mod cmd;
pub mod db;
pub mod err;
pub mod eval;
pub mod inmem;
pub mod json;
pub mod ondisk;
pub const DEFAULT_PORT: &str = "8888";

#[macro_use] extern crate rocket;
#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![index])
}