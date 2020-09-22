/*use crate::cmd::Cmd;
use crate::json::Json;
use crate::err::Error;
use rayon::prelude::*;

fn eval_cmd(cmd: &Cmd, rows: &[Json]) -> Result<Json, Error> {
    match cmd {
        Cmd::Key(key) => {
            let vec = rows.par_iter().map(|r| r.get(key).cloned().unwrap_or(Json::Null)).collect();
            Ok(Json::Array(vec))
        },
        Cmd::Sum(arg) => {

        }
        _ => unimplemented!()
    }
}

pub fn eval_cmds(cmd: &[Cmd], rows: &[Json]) -> Result<Vec<Json>, Error> {

}*/