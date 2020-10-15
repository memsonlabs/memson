use crate::apply::apply;
use crate::cmd::{Cmd, QueryCmd};
use crate::db::{InMemDb, Query};
use crate::json::*;
use crate::Error;
use crate::Res;
use core::option::Option::Some;

// evaluate the sort command
fn eval_sort_cmd(db: &mut InMemDb, arg: Cmd) -> Res {
    let mut val = eval_cmd(db, arg)?;
    json_sort_ascend(&mut val);
    Ok(val)
}

// evaluate a command
pub fn eval_cmd(db: &mut InMemDb, cmd: Cmd) -> Res {
    match cmd {
        Cmd::Apply(lhs, rhs) => {
            let val = eval_cmd(db, *rhs)?;
            apply(*lhs, &val)
        }
        Cmd::Add(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_add),
        Cmd::Append(_key, _arg) => {
            //let val = self.eval(*arg)?;
            //self.append(key, val.into_val())
            unimplemented!()
        }
        Cmd::Avg(arg) => eval_unr_fn(db, *arg, json_avg),
        Cmd::Bar(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_bar),
        Cmd::Len(arg) => eval_unr_fn(db, *arg, &count),
        Cmd::Delete(key) => Ok(db.delete(&key).unwrap_or(Json::Null)),
        Cmd::Div(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_div),
        Cmd::First(arg) => eval_unr_fn(db, *arg, |x| Ok(json_first(x))),
        Cmd::Get(key, arg) => {
            let val = eval_cmd(db, *arg)?;
            Ok(json_get(&key, &val).unwrap_or(Json::Null))
        }
        Cmd::Insert(key, arg) => {
            let val = db.get_mut(&key)?;
            let n = arg.len();
            json_insert(val, arg);
            Ok(Json::from(n))
        }
        Cmd::Json(val) => Ok(val),
        Cmd::Keys(_page) => unimplemented!(),
        Cmd::Last(arg) => eval_unr_fn(db, *arg, |x| Ok(json_last(x))),
        Cmd::Max(arg) => Ok(json_max(&eval_cmd(db, *arg)?)
            .cloned()
            .unwrap_or(Json::Null)),
        Cmd::In(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, |x, y| Ok(json_in(x, y))),
        Cmd::Min(arg) => Ok(json_min(&eval_cmd(db, *arg)?)
            .cloned()
            .unwrap_or(Json::Null)),
        Cmd::Mul(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_mul),
        Cmd::Push(key, arg) => {
            let val = eval_cmd(db, *arg)?;
            let kv = db.get_mut(&key)?;
            json_push(kv, val);
            Ok(Json::Null)
        }
        Cmd::Pop(key) => Ok(pop(db, key)?.unwrap_or(Json::Null)),
        Cmd::Query(cmd) => eval_query(db, cmd),
        Cmd::Set(key, arg) => {
            let val = eval_cmd(db, *arg)?;
            Ok(db.set(key, val).unwrap_or(Json::Null))
        }
        Cmd::Slice(arg, range) => json_slice(eval_cmd(db, *arg)?, range),
        Cmd::Sort(arg, _) => eval_sort_cmd(db, *arg),
        Cmd::StdDev(arg) => eval_unr_fn(db, *arg, json_dev),
        Cmd::Sub(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_sub),
        Cmd::Sum(arg) => eval_unr_fn(db, *arg, |x| Ok(json_sum(x))),
        Cmd::Summary => unimplemented!(),
        Cmd::Unique(arg) => eval_unr_fn(db, *arg, unique),
        Cmd::Var(arg) => eval_unr_fn(db, *arg, json_var),
        Cmd::ToString(arg) => Ok(eval_cmd(db, *arg)?),
        Cmd::Key(key) => {
            let mut it = key.split('.');
            let key = it.next().ok_or_else(|| Error::BadKey(key.clone()))?;
            let mut ref_val = db.get(key)?;
            let mut fat_val = None;
            for key in it {
                match ref_val {
                    Json::Array(ref arr) => {
                        let mut out = Vec::new();
                        for val in arr {
                            if let Some(val) = val.get(key) {
                                out.push(val.clone());
                            }
                        }
                        fat_val = Some(out);
                    }
                    val => {
                        ref_val = val.get(&key).ok_or_else(|| Error::BadKey(key.to_string()))?;
                    }
                }
            }
            Ok(if let Some(val) = fat_val {
                Json::Array(val)
            } else {
                ref_val.clone()
            })
        }
        Cmd::Reverse(arg) => {
            let mut val = eval_cmd(db, *arg)?;
            json_reverse(&mut val);
            Ok(val)
        }
        Cmd::Median(arg) => {
            let mut val = eval_cmd(db, *arg)?;
            json_median(&mut val)
        }
        Cmd::SortBy(_, _) => unimplemented!(),
        Cmd::Eval(_) => unimplemented!(),
        Cmd::Eq(_, _) => unimplemented!(),
        Cmd::NotEq(_, _) => unimplemented!(),
        Cmd::Gt(_, _) => unimplemented!(),
        Cmd::Lt(_, _) => unimplemented!(),
        Cmd::Gte(_, _) => unimplemented!(),
        Cmd::Lte(_, _) => unimplemented!(),
        Cmd::And(_, _) => unimplemented!(),
        Cmd::Or(_, _) => unimplemented!(),
        Cmd::Map(arg, f) => {
            let val = eval_cmd(db, *arg)?;
            json_map(&val, f)
        }
        Cmd::Flat(arg) => {
            let val = eval_cmd(db, *arg)?;
            Ok(json_flat(val))
        }
        Cmd::NumSort(arg, descend) => {
            let val = eval_cmd(db, *arg)?;
            Ok(json_numsort(val, descend))
        }
        Cmd::Has(key) => Ok(Json::Bool(db.has(&key))),
    }
}

// evaluate the query command
fn eval_query(db: &InMemDb, cmd: QueryCmd) -> Res {
    let qry = Query::from(db, cmd);
    qry.exec()
}

// evaluation of the pop command
pub fn pop(db: &mut InMemDb, key: String) -> Result<Option<Json>, Error> {
    let val = db.get_mut(&key)?;
    json_pop(val)
}

// evaluate binary function (a fn with 2 args)
fn eval_bin_fn<F>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd, f: F) -> Res
where
    F: Fn(&Json, &Json) -> Result<Json, Error>,
{
    let x = eval_cmd(db, lhs)?;
    let y = eval_cmd(db, rhs)?;
    f(&x, &y)
}

/// evaluate unary function (a fn with 1 arg)
fn eval_unr_fn<F>(db: &mut InMemDb, arg: Cmd, f: F) -> Res
where
    F: Fn(&Json) -> Result<Json, Error>,
{
    let val = eval_cmd(db, arg)?;
    f(&val)
}

/// evaluate filter to filter out data
pub fn eval_filter(cmd: Cmd, val: &Json) -> Option<bool> {
    let r = apply(cmd, val).ok();
    if let Some(g) = r {
        g.as_bool()
    } else {
        None
    }
}

//TODO refactor to take Json val instead of rows to make more generic
pub fn eval_rows_cmd(cmd: Cmd, rows: &Json) -> Option<Json> {
    apply(cmd, rows).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn eval_in() {
        let mut db = InMemDb::new();
        db.set("nums".to_string(), json!([1, 2, 3, 4, 5]));
        let cmd = Cmd::In(
            Box::new(Cmd::Key("nums".to_string())),
            Box::new(Cmd::Json(json!(3))),
        );
        assert_eq!(
            Ok(json!([false, false, true, false, false])),
            eval_cmd(&mut db, cmd)
        );
    }
}
