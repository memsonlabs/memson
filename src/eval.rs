use crate::apply::apply;
use crate::cmd::{Cmd, QueryCmd};
use crate::db::Query;
use crate::inmem::InMemDb;
use crate::json::*;
use crate::Error;
use crate::Res;
use core::option::Option::Some;
use std::str::Split;



fn eval_nested_key<'a>(val: Option<&'a Json>, it: Split<'_, char>) -> Option<&'a Json> {
    unimplemented!()
}

/// evaluate the key command
fn eval_key<'a>(db: &'a InMemDb, key: String) -> Res<'a> {
    let mut it = key.split('.');
    let key = it.next().ok_or_else(|| Error::BadKey(key.clone()))?;
    let ref_val = db.key(key).ok_or_else(|| Error::BadKey(key.to_string()))?;
    let val = eval_nested_key(Some(&ref_val), it);
    let r = if let Some(val) = val {
        JsonVal::Ref(val)
    } else {
        JsonVal::Val(Json::Null)
    };
    Ok(r)
}

fn eval_append<'a>(db: &'a mut InMemDb, key: &str, arg: Cmd) -> Result<Json, Error> {
    let elem = eval_cmd(db, arg)?;
    let val = db.get_mut(&key)?;
    json_append(val, elem.into());
    Ok(Json::Null)
}

/// evaluate the sort command
fn eval_sort_cmd<'a>(db: &'a mut InMemDb, arg: Cmd) -> Res<'a> {
    let mut val: Json = eval_cmd(db, arg)?.into();
    json_sort(&mut val, false);
    Ok(JsonVal::Val(val))
}

/// evaluate the insert command
fn eval_insert<'a>(db: &'a mut InMemDb, key: &str, arg: Vec<JsonObj>) -> Res<'a> {
    let val = db.get_mut(&key)?;
    let n = arg.len();
    json_insert(val, arg);
    Ok(JsonVal::Val(Json::from(n)))
}

fn eval_push<'a>(db: &'a mut InMemDb, key: &str, arg: Cmd) -> Res<'a> {
    let val: Json = eval_cmd(db, arg)?.into();
    let kv = db.get_mut(&key)?;
    json_push(kv, val);
    Ok(JsonVal::Val(Json::Null))
}

fn eval_reverse(db: &mut InMemDb, arg: Cmd) -> Res {
    let mut val: Json = eval_cmd(db, arg)?.into();
    json_reverse(&mut val);
    Ok(JsonVal::Val(val))
}

fn eval_evals<'a>(db: &'a mut InMemDb, cmds: Vec<Cmd>) -> Result<Vec<JsonVal<'a>>, Error> {
    let vals: Result<Vec<JsonVal<'a>>, Error> = cmds.into_iter().map(|cmd| {
        eval_cmd(db, cmd)
    }).collect();
    vals
}

fn eval_map<'a>(db: &'a mut InMemDb, arg: Cmd, key: String) -> Res<'a> {
    let val = eval_cmd(db, arg)?;
    json_map(val.as_ref(), key).map(JsonVal::Val)
}

fn eval_flat<'a>(db: &'a mut InMemDb, arg: Cmd) -> Res<'a> {
    let val = eval_cmd(db, arg)?.into();
    Ok(JsonVal::Val(json_flat(val.as_ref())))
}

fn eval_numsort<'a>(db: &'a mut InMemDb, arg: Cmd, descend: bool) -> Res<'a> {
    let val: Json = eval_cmd(db, arg)?.into();
    Ok(JsonVal::Val(json_numsort(val, descend)))
}

fn eval_median<'a>(db: &'a mut InMemDb, arg: Cmd) -> Res<'a> {
    let val = eval_cmd(db, arg)?;
    json_median(val.as_ref()).map(JsonVal::Val)
}

fn eval_sortby(db: &mut InMemDb, arg: Cmd, key: String) -> Res {
    let mut val = eval_cmd(db, arg)?.into();
    json_sortby(&mut val, &key);
    Ok(val)
}

fn eval_eq<'a>(db: &'a mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res<'a> {
    let lhs = eval_cmd(db, lhs)?;
    let rhs = eval_cmd(db, rhs)?;
    Ok(JsonVal::Val(json_eq(lhs.as_ref(), rhs.as_ref())))
}

fn eval_not_eq<'a>(db: &'a mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res<'a> {
    let x = eval_cmd(db, lhs)?;
    let y = eval_cmd(db, rhs)?;
    Ok(JsonVal::Val(json_not_eq(x.as_ref(), y.as_ref())))
}

fn eval_lt<'a>(db: &'a mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res<'a> {
    let x = eval_cmd(db, lhs)?;
    let y = eval_cmd(db, rhs)?;
    Ok(JsonVal::Val(json_lt(x.as_ref(), y.as_ref())))
}


fn eval_gt<'a>(db: &'a mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res<'a> {
    let x = eval_cmd(db, lhs)?;
    let y = eval_cmd(db, rhs)?;
    Ok(JsonVal::Val(json_gt(x.as_ref(), y.as_ref())))
}

fn eval_lte<'a>(db: &'a mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res<'a> {
    Ok(json_lte(&eval_cmd(db, lhs)?.as_ref(), &eval_cmd(db, rhs)?.as_ref()))
}

fn eval_gte<'a>(db: &'a mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res<'a> {
    Ok(json_gte(&eval_cmd(db, lhs)?.as_ref(), &eval_cmd(db, rhs)?.as_ref()))
}

fn eval_and(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_lte(&eval_cmd(db, lhs)?.as_ref(), &eval_cmd(db, rhs)?.as_ref()))
}

fn eval_or(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Res {
    Ok(json_gte(&eval_cmd(db, lhs)?.as_ref(), &eval_cmd(db, rhs)?.as_ref()))
}

/// evaluate a command
pub fn eval_cmd<'a>(db: &'a mut InMemDb, cmd: Cmd) -> Result<JsonVal<'a>, Error> {
    match cmd {
        Cmd::Apply(lhs, rhs) => {
            let val = eval_cmd(db, *rhs)?;
            apply(*lhs, val.as_ref()).map(JsonVal::Val)
        }
        Cmd::Add(lhs, rhs) => {
            eval_bin_fn(db, *lhs, *rhs, json_add).map(JsonVal::Val)
        }
        Cmd::Append(key, arg) => eval_append(db, &key, *arg).map(JsonVal::Val),
        Cmd::Avg(arg) => eval_unr_fn(db, *arg, json_avg).map(JsonVal::Val),
        Cmd::Bar(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_bar).map(JsonVal::Val),
        Cmd::Len(arg) => eval_unr_fn(db, *arg, &count).map(JsonVal::Val),
        Cmd::Delete(key) => {
            Ok(JsonVal::Val(db.delete(&key).unwrap_or(Json::Null)))
        },
        Cmd::Div(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_div).map(JsonVal::Val),
        Cmd::First(arg) => eval_unr_fn(db, *arg, |x| Ok(json_first(x))).ma,
        Cmd::Get(key, arg) => {
            let val = eval_cmd(db, *arg)?;
            Ok(JsonVal::Val(json_get(&key, val.as_ref()).unwrap_or(Json::Null)))
        }
        Cmd::Insert(key, arg) => eval_insert(db, &key, arg),
        Cmd::Json(val) => Ok(JsonVal::Val(val)),
        Cmd::Keys(page) => Ok(JsonVal::Val(Json::Array(db.keys(page)))),
        Cmd::Last(arg) => eval_unr_fn(db, *arg, |x| Ok(json_last(x))),
        Cmd::Max(arg) => eval_max(db, arg),
        Cmd::In(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, |x, y| Ok(json_in(x, y))),
        Cmd::Min(arg) => {
            let val = eval_cmd(db, *arg)?;
            let min_val = json_min(val.as_ref());
            let val = if let Some(val) = min_val {
                JsonVal::Ref(val)
            } else {
                JsonVal::Val(Json::Null)
            };
            Ok(val)
        }
        Cmd::Mul(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_mul),
        Cmd::Push(key, arg) => eval_push(db, &key, *arg),
        Cmd::Pop(key) => {
            let val = pop(db, key)?;
            Ok(JsonVal::Val(val.unwrap_or(Json::Null)))
        }
        Cmd::Query(cmd) => eval_query(db, cmd),
        Cmd::Set(key, arg) => {
            let val: JsonVal = eval_cmd(db, *arg)?;
            let rval = db.set(key, val.into());
            let val: Json = rval.unwrap_or(Json::Null);
            Ok(JsonVal::Val(val))
        }
        Cmd::Slice(arg, range) => {
            let val = eval_cmd(db, *arg)?;
            unimplemented!()
            //json_slice(val.into(), range).map(JsonVal::Val)
        }
        Cmd::Sort(arg, _) => eval_sort_cmd(db, *arg),
        Cmd::Dev(arg) => eval_unr_fn(db, *arg, json_dev),
        Cmd::Sub(lhs, rhs) => eval_bin_fn(db, *lhs, *rhs, json_sub),
        Cmd::Sum(arg) => eval_unr_fn(db, *arg, |x| Ok(json_sum(x))),
        Cmd::Summary => Ok(JsonVal::Val(db.summary())),
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
        Cmd::Has(key) => Ok(JsonVal::Val(Json::Bool(db.has(&key)))),
    }
}

fn eval_max(db: &mut InMemDb, arg: Box<Cmd>) -> Result<JsonVal, Error> {
    let val = eval_cmd(db, *arg)?;
    let max_val = json_max(val.as_ref());
    let val: JsonVal = if let Some(val) = max_val {
        JsonVal::Ref(val)
    } else {
        JsonVal::Val(Json::Null)
    };
    Ok(val)
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
fn eval_bin_fn<'a, F>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd, f: F) -> Res
where
    F: Fn(&'a Json, &'a Json) -> Res<'a>,
{
    let x = eval_cmd(db, lhs)?;
    let y = eval_cmd(db, rhs)?;
    f(x.as_ref(), y.as_ref())
}

/// evaluate unary function (a fn with 1 arg)
fn eval_unr_fn<F>(db: &mut InMemDb, arg: Cmd, f: F) -> Res
where
    F: Fn(&Json) -> Res,
{
    let val = eval_cmd(db, arg)?;
    f(val.as_ref())
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

    #[test]
    fn test_eval_nested_key() {
        let it = "address.line1".split('.');
        assert_eq!(None, eval_nested_key(None, it));

        let val = json!({"orders": [1,2,3,4,5]});
        let it = "orders".split('.');
        assert_eq!(Some(json!([1,2,3,4,5])), eval_nested_key(Some(val), it));

        let val = json!({"orders": [
            {"qty": 1},
            {"qty": 2},
            {"qty": 3},
            {"qty": 4},
            {"qty": 5},
        ]});
        let it = "orders.qty".split('.');
        assert_eq!(Some(json!([1,2,3,4,5])), eval_nested_key(Some(val), it));

        let val = json!({"orders": [
            {"customer": {"name": "alice"}},
            {"customer": {"name": "bob"}},
            {"customer": {"name": "charles"}},
            {"customer": {"name": "dave"}},
            {"customer": {"name": "ewa"}},
            {"customer": {"surname": "perry"}},
            {"foo": {"bar": "baz"}},
        ]});
        let it = "orders.customer.name".split('.');
        assert_eq!(Some(json!(["alice", "bob", "charles", "dave", "ewa", Json::Null, Json::Null])), eval_nested_key(Some(val), it));
    }
}
