use std::sync::Arc;
use crate::cmd::{Cmd, QueryCmd};
use crate::db::Query;
use crate::inmem::InMemDb;
use crate::json::*;
use crate::Error;
use core::option::Option::Some;


pub fn eval_nested_key<'a>(val: &'a Json, keys: &[&str]) -> Option<&'a Json> {
    let mut opt_val = Some(val);
    for key in keys {
        if let Some(v) = opt_val {
            opt_val = v.get(key)
        } else {
            return None
        }
    }
    return opt_val
}

/// evaluate the key command
pub fn eval_key<'a>(db: &mut InMemDb, keys: &[&str]) -> Result<Arc<Json>, Error> {

    let key = keys[0];
    let val = db.get(key)?;
    let val = eval_nested_key(&val, &keys[2..]);
    let r: Json = if let Some(val) = val {
        val.clone()
    } else {
        Json::Null
    };
    Ok(Arc::new(r))
}

pub fn eval_append<'a>(db: &mut InMemDb, key: &str, arg: Cmd) -> Result<Arc<Json>, Error> {
/*     let elem = db.eval(arg)?;
    let val = db.get_mut(&key)?;
    json_append(val, elem.into());
    Ok(Json::Null) */
    unimplemented!()
}

/// evaluate the sort command
pub fn eval_sort_cmd<'a>(db: &mut InMemDb, arg: Cmd) -> Result<Arc<Json>, Error> {
    let mut val: Json = db.eval(arg)?.as_ref().clone();
    json_sort(&mut val, false);
    Ok(Arc::new(val))
}

/// evaluate the insert command
pub fn eval_insert<'a>(db: &mut InMemDb, key: &str, arg: Vec<JsonObj>) -> Result<Arc<Json>, Error> {
 /*    
 let val = db.get_mut(&key)?;
    let n = arg.len();
    json_insert(val, arg);
    Ok(Arc::new(Json::from(n))) */
    unimplemented!()
}

pub fn eval_push<'a>(db: &mut InMemDb, key: &str, arg: Cmd) -> Result<Arc<Json>, Error> {
/*     let val: Json = db.eval(arg)?.into();
    let kv = db.get_mut(&key)?;
    json_push(kv, val);
    Ok(Arc::new(Json::Null)) */
    unimplemented!()
}

pub fn eval_reverse<'a>(db: &mut InMemDb, arg: Cmd) -> Result<Arc<Json>, Error> {
    let mut val: Json = db.eval(arg)?.as_ref().clone();
    json_reverse(&mut val);
    Ok(Arc::new(val))
}

pub fn eval_map<'a>(db: &mut InMemDb, arg: Cmd, key: String) -> Result<Arc<Json>, Error> {
    let val = db.eval(arg)?;
    json_map(val.as_ref(), key).map(Arc::new)
}

pub fn eval_flat<'a>(db: &mut InMemDb, arg: Cmd) -> Result<Arc<Json>, Error> {
    let val = db.eval(arg)?;
    let val: Json = json_flat(val.as_ref());
    Ok(Arc::new(val))
}

pub fn eval_numsort<'a>(db: &mut InMemDb, arg: Cmd, descend: bool) -> Result<Arc<Json>, Error> {
    let val: Json = db.eval(arg)?.as_ref().clone();
    Ok(Arc::new(json_numsort(val, descend)))
}

pub fn eval_median<'a>(db: &mut InMemDb, arg: Cmd) -> Result<Arc<Json>, Error> {
    let val = db.eval(arg)?;
    json_median(val.as_ref()).map(Arc::new)
}

pub fn eval_sortby<'a>(db: &mut InMemDb, arg: Cmd, key: String) -> Result<Arc<Json>, Error> {
    let mut val: Json = db.eval(arg)?.as_ref().clone();
    json_sortby(&mut val, &key);
    Ok(Arc::new(val))
}

pub fn eval_eq<'a>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Arc<Json>, Error> {
    let lhs = db.eval(lhs)?;
    let rhs = db.eval(rhs)?;
    Ok(Arc::new(json_eq(lhs.as_ref(), rhs.as_ref())))
}

pub fn eval_not_eq<'a>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Arc<Json>, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(Arc::new(json_not_eq(x.as_ref(), y.as_ref())))
}

pub fn eval_lt<'a>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Arc<Json>, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(Arc::new(json_lt(x.as_ref(), y.as_ref())))
}


pub fn eval_gt<'a>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Arc<Json>, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(Arc::new(json_gt(x.as_ref(), y.as_ref())))
}

pub fn eval_lte<'a>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Arc<Json>, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_lte(x.as_ref(), y.as_ref());
    Ok(Arc::new(val))
}

pub fn eval_gte<'a>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Arc<Json>, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val: Json = json_gte(&x, &y);
    Ok(Arc::new(val))
}

pub fn eval_and<'a>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Arc<Json>, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_lte(x.as_ref(), y.as_ref());
    Ok(Arc::new(val))
}

pub fn eval_or<'a>(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Arc<Json>, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_gte(x.as_ref(), y.as_ref());
    Ok(Arc::new(val))
}

pub fn eval_max(db: &mut InMemDb, arg: Box<Cmd>) -> Result<Arc<Json>, Error> {
    let val = db.eval(*arg)?;
    let max_val = json_max(val.as_ref());
    let val: Json = if let Some(val) = max_val {
        val.clone()
    } else {
        Json::Null
    };
    Ok(Arc::new(val))
}

pub fn eval_min<'a>(db: &'a mut InMemDb, arg: Cmd) -> Result<Arc<Json>, Error> {
    let val = db.eval(arg)?;
    let min_val = json_min(val.as_ref());
    let val = if let Some(val) = min_val {
        val.clone()
    } else {
        Json::Null
    };
    Ok(Arc::new(val))
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
pub fn eval_bin_fn(db: &mut InMemDb, lhs: Cmd, rhs: Cmd, f: &dyn Fn(&Json, &Json) -> Result<Arc<Json>, Error> ) -> Result<Arc<Json>, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    f(x.as_ref(), y.as_ref())
}

/// evaluate unary function (a fn with 1 arg)


/// evaluate filter to filter out data
pub fn eval_filter(cmd: Cmd, val: &Json) -> Option<bool> {
    /*
    let r = apply(cmd, val).ok();
    if let Some(g) = r {
        g.as_bool()
    } else {
        None
    }
    */
    unimplemented!()
}

//TODO refactor to take Json val instead of rows to make more generic
pub fn eval_rows_cmd(cmd: Cmd, rows: &Json) -> Option<Json> {
    //apply(cmd, rows).ok()
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    pub fn eval_in() {
        let mut db = InMemDb::new();
        db.set("nums".to_string(), Arc::new(json!([1, 2, 3, 4, 5])));
        let cmd = Cmd::In(
            Box::new(Cmd::Key("nums".to_string())),
            Box::new(Cmd::Json(json!(3))),
        );
        assert_eq!(
            Ok(Arc::new(json!([false, false, true, false, false]))),
            db.eval(cmd)
        );
    }

    #[test]
    fn test_eval_nested_key() {
        let it = "address.line1".split('.');
        let val = json!({"a": 1});
        assert_eq!(None, eval_nested_key(&val, &["b"]));

        let val = json!({"orders": [1,2,3,4,5]});
        assert_eq!(Some(&json!([1,2,3,4,5])), eval_nested_key(&val, &["orders"]));

        let val = json!({"orders": [
            {"qty": 1},
            {"qty": 2},
            {"qty": 3},
            {"qty": 4},
            {"qty": 5},
        ]});
        assert_eq!(Some(&json!([1,2,3,4,5])), eval_nested_key(&val, &["orders","qty"]));

        let val = json!({"orders": [
            {"customer": {"name": "alice"}},
            {"customer": {"name": "bob"}},
            {"customer": {"name": "charles"}},
            {"customer": {"name": "dave"}},
            {"customer": {"name": "ewa"}},
            {"customer": {"surname": "perry"}},
            {"foo": {"bar": "baz"}},
        ]});
        assert_eq!(Some(&json!(["alice", "bob", "charles", "dave", "ewa", Json::Null, Json::Null])), eval_nested_key(&val, &["orders.customer.name"]));
    }
}
