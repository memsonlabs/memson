use crate::apply::apply;
use crate::cmd::{Cmd, QueryCmd};
use crate::db::{InMemDb, Query};
use crate::json::*;
use crate::Error;
use crate::Res;
use core::option::Option::Some;

/// evaluate the key command
fn eval_key(db: &InMemDb, key: String) -> Res {
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
                ref_val = val
                    .get(&key)
                    .ok_or_else(|| Error::BadKey(key.to_string()))?;
            }
        }
    }
    Ok(if let Some(val) = fat_val {
        Json::Array(val)
    } else {
        ref_val.clone()
    })
}

fn eval_append(db: &mut InMemDb, key: &str, arg: Cmd) -> Res {
    let elem = eval_cmd(db, arg)?;
    let val = db.get_mut(&key)?;
    json_append(val, elem);
    Ok(Json::Null)
}

/// evaluate the sort command
fn eval_sort_cmd(db: &mut InMemDb, arg: Cmd) -> Res {
    let mut val = eval_cmd(db, arg)?;
    json_sort(&mut val, false);
    Ok(val)
}

/// evaluate the insert command
fn eval_insert(db: &mut InMemDb, key: &str, arg: Vec<JsonObj>) -> Res {
    let val = db.get_mut(&key)?;
    let n = arg.len();
    json_insert(val, arg);
    Ok(Json::from(n))
}

fn eval_push(db: &mut InMemDb, key: &str, arg: Cmd) -> Res {
    let val = eval_cmd(db, arg)?;
    let kv = db.get_mut(&key)?;
    json_push(kv, val);
    Ok(Json::Null)
}

fn eval_reverse(db: &mut InMemDb, arg: Cmd) -> Res {
    let mut val = eval_cmd(db, arg)?;
    json_reverse(&mut val);
    Ok(val)
}

fn eval_evals(db: &mut InMemDb, cmds: Vec<Cmd>) -> Res {
    let vals: Result<Vec<_>, _> = cmds.into_iter().map(|cmd| eval_cmd(db, cmd)).collect();
    Ok(Json::Array(vals?))
}

fn eval_map(db: &mut InMemDb, arg: Cmd, key: String) -> Res {
    let val = eval_cmd(db, arg)?;
    json_map(&val, key)
}

fn eval_flat(db: &mut InMemDb, arg: Cmd) -> Res {
    Ok(json_flat(eval_cmd(db, arg)?))
}

fn eval_numsort(db: &mut InMemDb, arg: Cmd, descend: bool) -> Res {
    Ok(json_numsort(eval_cmd(db, arg)?, descend))
}

fn eval_median(db: &mut InMemDb, arg: Cmd) -> Res {
    let mut val = eval_cmd(db, arg)?;
    json_median(&mut val)
}

fn eval_sortby(db: &mut InMemDb, arg: Cmd, key: String) -> Res {
    let mut val = eval_cmd(db, arg)?;
    json_sortby(&mut val, &key);
    Ok(val)
}

fn eval_eq(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_eq(&eval_cmd(db, lhs)?, &eval_cmd(db, rhs)?))
}

fn eval_not_eq(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_not_eq(&eval_cmd(db, lhs)?, &eval_cmd(db, rhs)?))
}

fn eval_lt(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_lt(&eval_cmd(db, lhs)?, &eval_cmd(db, rhs)?))
}

fn eval_gt(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_gt(&eval_cmd(db, lhs)?, &eval_cmd(db, rhs)?))
}

fn eval_lte(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_lte(&eval_cmd(db, lhs)?, &eval_cmd(db, rhs)?))
}

fn eval_gte(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_gte(&eval_cmd(db, lhs)?, &eval_cmd(db, rhs)?))
}

fn eval_and(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_lte(&eval_cmd(db, lhs)?, &eval_cmd(db, rhs)?))
}

fn eval_or(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_gte(&eval_cmd(db, lhs)?, &eval_cmd(db, rhs)?))
}

/// evaluate a command
pub fn eval_cmd(db: &mut InMemDb, cmd: Cmd) -> Res {
    match cmd {
        Cmd::Apply(lhs, rhs) => {
            let val = eval_cmd(db, *rhs)?;
            apply(*lhs, &val)
        }
        Cmd::Add(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_add),
        Cmd::Append(key, arg) => eval_append(db, &key, *arg),
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
        Cmd::Insert(key, arg) => eval_insert(db, &key, arg),
        Cmd::Json(val) => Ok(val),
        Cmd::Keys(page) => Ok(Json::Array(db.keys(page))),
        Cmd::Last(arg) => eval_unr_fn(db, *arg, |x| Ok(json_last(x))),
        Cmd::Max(arg) => Ok(json_max(&eval_cmd(db, *arg)?)
            .cloned()
            .unwrap_or(Json::Null)),
        Cmd::In(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, |x, y| Ok(json_in(x, y))),
        Cmd::Min(arg) => Ok(json_min(&eval_cmd(db, *arg)?)
            .cloned()
            .unwrap_or(Json::Null)),
        Cmd::Mul(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_mul),
        Cmd::Push(key, arg) => eval_push(db, &key, *arg),
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
        Cmd::Summary => Ok(db.summary()),
        Cmd::Unique(arg) => eval_unr_fn(db, *arg, unique),
        Cmd::Var(arg) => eval_unr_fn(db, *arg, json_var),
        Cmd::ToString(arg) => Ok(eval_cmd(db, *arg)?),
        Cmd::Key(key) => eval_key(db, key),
        Cmd::Reverse(arg) => eval_reverse(db, *arg),
        Cmd::Median(arg) => eval_median(db, *arg),
        Cmd::SortBy(arg, key) => eval_sortby(db, *arg, key),
        Cmd::Eval(cmds) => eval_evals(db, cmds),
        Cmd::Eq(lhs, rhs) => eval_eq(db, *lhs, *rhs),
        Cmd::NotEq(lhs, rhs) => eval_not_eq(db, *lhs, *rhs),
        Cmd::Gt(lhs, rhs) => eval_gt(db, *lhs, *rhs),
        Cmd::Lt(lhs, rhs) => eval_lt(db, *lhs, *rhs),
        Cmd::Gte(lhs, rhs) => eval_gte(db, *lhs, *rhs),
        Cmd::Lte(lhs, rhs) => eval_lte(db, *lhs, *rhs),
        Cmd::And(lhs, rhs) => eval_and(db, *lhs, *rhs),
        Cmd::Or(lhs, rhs) => eval_or(db, *lhs, *rhs),
        Cmd::Map(arg, f) => eval_map(db, *arg, f),
        Cmd::Flat(arg) => eval_flat(db, *arg),
        Cmd::NumSort(arg, descend) => eval_numsort(db, *arg, descend),
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
