use crate::apply::apply;
use crate::cmd::{Cmd, QueryCmd};
use crate::db::Query;
use crate::inmem::InMemDb;
use crate::json::*;
use crate::Error;
use core::option::Option::Some;

pub fn eval_nested_key(val: Json, keys: &[&str]) -> Option<Json> {
    let mut opt_val = Some(val);
    for key in keys {
        if let Some(v) = opt_val {
            println!("v={:?}\nkey={:?}", v, key);
            opt_val = eval_key(&v, key)
        } else {
            return None;
        }
    }
    opt_val
}

/// evaluate the key command
pub fn eval_keys(db: &InMemDb, keys: &[&str]) -> Option<Json> {
    let key = keys[0];
    let val = db.key(key)?;
    eval_nested_key(val.clone(), &keys[1..])
}

fn eval_key(val: &Json, key: &str) -> Option<Json> {
    match val {
        Json::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for val in arr {
                let v: Json = val.get(key).cloned().unwrap_or(Json::Null);
                out.push(v);
            }
            Some(Json::from(out))
        }
        val => val.get(key).cloned(),
    }
}

pub fn eval_append(db: &mut InMemDb, key: &str, arg: Cmd) -> Result<Json, Error> {
    let elem = db.eval(arg)?;
    let val = db.get_mut(key)?;
    json_append(val, elem);
    Ok(Json::Null)
}

/// evaluate the sort command
pub fn eval_sort_cmd(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let mut val: Json = db.eval(arg)?;
    json_sort(&mut val, false);
    Ok(val)
}

/// evaluate the insert command
pub fn eval_insert(db: &mut InMemDb, key: &str, arg: Vec<JsonObj>) -> Result<Json, Error> {
    let val = db.get_mut(key)?;
    let n = arg.len();
    json_insert(val, arg);
    Ok(Json::from(n))
}

pub fn eval_push(db: &mut InMemDb, key: &str, arg: Cmd) -> Result<Json, Error> {
    let val: Json = db.eval(arg)?;
    let kv = db.get_mut(key)?;
    json_push(kv, val);
    Ok(Json::Null)
}

pub fn eval_reverse(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let mut val: Json = db.eval(arg)?;
    json_reverse(&mut val);
    Ok(val)
}

pub fn eval_map(db: &mut InMemDb, arg: Cmd, key: String) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    json_map(&val, key)
}

pub fn eval_flat(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    let val: Json = json_flat(&val);
    Ok(val)
}

pub fn eval_numsort(db: &mut InMemDb, arg: Cmd, descend: bool) -> Result<Json, Error> {
    let val: Json = db.eval(arg)?;
    Ok(json_numsort(val, descend))
}

pub fn eval_median(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    json_median(&val)
}

pub fn eval_sortby(db: &mut InMemDb, arg: Cmd, key: String) -> Result<Json, Error> {
    let mut val: Json = db.eval(arg)?;
    json_sortby(&mut val, &key);
    Ok(val)
}

pub fn eval_eq(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let lhs = db.eval(lhs)?;
    let rhs = db.eval(rhs)?;
    Ok(json_eq(&lhs, &rhs))
}

pub fn eval_not_eq(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(json_not_eq(&x, &y))
}

pub fn eval_lt(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(json_lt(&x, &y))
}

pub fn eval_gt(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(json_gt(&x, &y))
}

pub fn eval_lte(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_lte(&x, &y);
    Ok(val)
}

pub fn eval_gte(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val: Json = json_gte(&x, &y);
    Ok(val)
}

pub fn eval_and(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_lte(&x, &y);
    Ok(val)
}

pub fn eval_or(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_gte(&x, &y);
    Ok(val)
}

pub fn eval_max(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    let max_val = json_max(&val);
    let val: Json = if let Some(val) = max_val {
        val.clone()
    } else {
        Json::Null
    };
    Ok(val)
}

pub fn eval_min(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    let min_val = json_min(&val);
    let val = if let Some(val) = min_val {
        val.clone()
    } else {
        Json::Null
    };
    Ok(val)
}

// evaluate the query command
pub fn eval_query(db: &InMemDb, cmd: QueryCmd) -> Result<Json, Error> {
    let qry = Query::from(db, cmd);
    qry.exec()
}

// evaluation of the pop command
pub fn pop(db: &InMemDb, key: String) -> Result<Option<Json>, Error> {
    unimplemented!()
}

// evaluate binary function (a fn with 2 args)
pub fn eval_bin_fn(
    db: &mut InMemDb,
    lhs: Cmd,
    rhs: Cmd,
    f: &dyn Fn(&Json, &Json) -> Result<Json, Error>,
) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    f(&x, &y)
}

/// evaluate unary function (a fn with 1 arg)

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
    pub fn eval_in() {
        let mut db = InMemDb::new();
        db.set("nums".to_string(), json!([1, 2, 3, 4, 5]));
        let cmd = Cmd::In(
            Box::new(Cmd::Key("nums".to_string())),
            Box::new(Cmd::Json(json!(3))),
        );
        assert_eq!(Ok(json!([false, false, true, false, false])), db.eval(cmd));
    }

    #[test]
    fn test_eval_nested_key() {
        let it = "address.line1".split('.');
        let val = json!({"a": 1});
        assert_eq!(None, eval_nested_key(val, &["b"]));

        let val = json!({"orders": [1,2,3,4,5]});
        assert_eq!(
            Some(json!([1, 2, 3, 4, 5])),
            eval_nested_key(val, &["orders"])
        );

        let val = json!({"orders": [
            {"qty": 1},
            {"qty": 2},
            {"qty": 3},
            {"qty": 4},
            {"qty": 5},
        ]});
        assert_eq!(
            Some(json!([1, 2, 3, 4, 5])),
            eval_nested_key(val, &["orders", "qty"])
        );

        let val = json!({"orders": [
            {"customer": {"name": "alice"}},
            {"customer": {"name": "bob"}},
            {"customer": {"name": "charles"}},
            {"customer": {"name": "dave"}},
            {"customer": {"name": "ewa"}},
            {"customer": {"surname": "perry"}},
            {"foo": {"bar": "baz"}},
        ]});
        assert_eq!(
            Some(json!([
                "alice",
                "bob",
                "charles",
                "dave",
                "ewa",
                Json::Null,
                Json::Null
            ])),
            eval_nested_key(val, &["orders", "customer", "name"])
        );
    }
}
