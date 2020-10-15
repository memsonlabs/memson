use crate::cmd::Cmd;
use crate::json::{
    gt, gte, json_add2, json_and, json_bar, json_gt, json_gte, json_lt, json_lte, json_map,
    json_median, json_not_eq, json_or, json_slice, json_sort_ascend, json_sortby, json_var, lt,
    lte, noteq, numsort, Json,
};
use crate::json::{
    json_add, json_avg, json_count, json_dev, json_div, json_eq, json_first, json_flat, json_get,
    json_in, json_last, json_max, json_min, json_mul, json_reverse, json_sub, json_sum,
    json_tostring, json_unique,
};
use crate::{Error, Res};
use rayon::prelude::*;

/// retrieves the key/val entry from a row by key
fn get_key(row: &Json, key: &str) -> Json {
    row.get(key).cloned().unwrap_or(Json::Null)
}

/// apply an unary function
fn apply_unr_fn<F>(arg: Cmd, rows: &[Json], f: F) -> Res
where
    F: FnOnce(&Json) -> Res,
{
    f(&apply_rows(arg, rows)?)
}

/// apply a binary function
fn apply_bin_fn<F>(lhs: Cmd, rhs: Cmd, rows: &[Json], f: F) -> Res
where
    F: FnOnce(&Json, &Json) -> Res,
{
    let x = apply_rows(lhs, rows)?;
    let y = apply_rows(rhs, rows)?;
    f(&x, &y)
}

/// apply boolean function to process binary gates
fn apply_bool_fn<F>(lhs: Cmd, rhs: Cmd, rows: &[Json], f: F) -> Res
where
    F: FnOnce(&Json, &Json) -> bool,
{
    let x = apply_rows(lhs, rows)?;
    let y = apply_rows(rhs, rows)?;
    Ok(Json::from(f(&x, &y)))
}

fn apply_key(key: String, rows: &[Json]) -> Json {
    Json::Array(
        rows.par_iter()
            .map(|x| x.get(&key))
            .filter(|x| x.is_some())
            .map(|x| x.unwrap().clone())
            .collect(),
    )
}

/// apply a cmd to rows of json
pub fn apply_rows(cmd: Cmd, rows: &[Json]) -> Res {
    match cmd {
        Cmd::Key(key) => Ok(apply_key(key, rows)),
        Cmd::Sum(arg) => match *arg {
            Cmd::Key(key) => {
                let val: Json = rows
                    .par_iter()
                    .map(|x| x.get(&key))
                    .filter(|x| x.is_some())
                    .map(|x| x.unwrap())
                    .fold(|| Json::from(0), |x, y| json_add2(&x, y))
                    .reduce(|| Json::from(0), |x, y| json_add2(&x, &y));
                Ok(val)
            }
            cmd => {
                let val = apply_rows(cmd, rows)?;
                Ok(json_sum(&val))
            }
        },
        Cmd::Max(arg) => match *arg {
            Cmd::Key(key) => {
                let val: Option<&Json> = rows
                    .par_iter()
                    .map(|x| x.get(&key))
                    .filter(|x| x.is_some())
                    .map(|x| x.unwrap())
                    .fold(
                        || None,
                        |x, y| {
                            if let Some(x) = x {
                                if json_gt(x, &y) {
                                    Some(x)
                                } else {
                                    Some(&y)
                                }
                            } else {
                                Some(&y)
                            }
                        },
                    )
                    .reduce(
                        || None,
                        |x, y| match (x, y) {
                            (Some(x), Some(y)) => Some(if json_gt(x, y) { x } else { y }),
                            (Some(x), None) | (None, Some(x)) => Some(x),
                            (None, None) => None,
                        },
                    );
                Ok(val.cloned().unwrap_or(Json::Null))
            }
            _ => unimplemented!(),
        },
        Cmd::Append(_, _) => Err(Error::BadCmd),
        Cmd::Apply(_, _) => Err(Error::BadCmd),
        Cmd::Bar(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_bar),
        Cmd::Set(_, _) => Err(Error::BadCmd),
        Cmd::Min(arg) => apply_unr_fn(*arg, rows, |x| {
            Ok(json_min(x).cloned().unwrap_or(Json::Null))
        }),
        Cmd::Avg(arg) => apply_unr_fn(*arg, rows, json_avg),
        Cmd::Delete(_) => Err(Error::BadCmd),
        Cmd::StdDev(arg) => apply_unr_fn(*arg, rows, json_dev),
        Cmd::Add(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_add),
        Cmd::Sub(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_sub),
        Cmd::Mul(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_mul),
        Cmd::Div(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_div),
        Cmd::First(arg) => apply_unr_fn(*arg, rows, |x| Ok(json_first(x))),
        Cmd::Last(arg) => apply_unr_fn(*arg, rows, |x| Ok(json_last(x))),
        Cmd::Var(arg) => apply_unr_fn(*arg, rows, json_var),
        Cmd::Push(_, _) => Err(Error::BadCmd),
        Cmd::Pop(_) => Err(Error::BadCmd),
        Cmd::Query(_) => Err(Error::BadCmd),
        Cmd::Insert(_, _) => Err(Error::BadCmd),
        Cmd::Keys(_) => unimplemented!(),
        Cmd::Len(arg) => apply_unr_fn(*arg, rows, |x| Ok(json_count(x))),
        Cmd::Unique(arg) => apply_unr_fn(*arg, rows, |x| Ok(json_unique(x))),
        Cmd::Json(val) => Ok(val),
        Cmd::Summary => Err(Error::BadCmd),
        Cmd::Get(_, _) => unimplemented!(),
        Cmd::ToString(arg) => apply_unr_fn(*arg, rows, |x| Ok(Json::from(json_tostring(x)))),
        Cmd::Sort(_, _) => unimplemented!(),
        Cmd::Reverse(arg) => {
            let mut val = apply_rows(*arg, rows)?;
            json_reverse(&mut val);
            Ok(val)
        }
        Cmd::SortBy(_, _) => unimplemented!(),
        Cmd::Median(arg) => {
            let mut val = apply_rows(*arg, rows)?;
            json_median(&mut val)
        }
        Cmd::Eval(_) => unimplemented!(),
        Cmd::Eq(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, |x, y| Ok(json_eq(x, y))),
        Cmd::NotEq(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, |x, y| Ok(json_not_eq(x, y))),
        Cmd::Gt(lhs, rhs) => apply_bool_fn(*lhs, *rhs, rows, json_gt),
        Cmd::Lt(lhs, rhs) => apply_bool_fn(*lhs, *rhs, rows, json_lt),
        Cmd::Gte(lhs, rhs) => apply_bool_fn(*lhs, *rhs, rows, json_gte),
        Cmd::Lte(lhs, rhs) => apply_bool_fn(*lhs, *rhs, rows, json_lte),
        Cmd::And(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_and),
        Cmd::Or(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_or),
        Cmd::Map(arg, f) => {
            let val = apply_rows(*arg, rows)?;
            json_map(&val, f)
        }
        Cmd::In(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, |x, y| Ok(json_in(x, y))),
        Cmd::Flat(arg) => {
            let val = apply_rows(*arg, rows)?;
            Ok(json_flat(val))
        }
        Cmd::NumSort(_, _) => unimplemented!(),
        Cmd::Has(key) => Ok(Json::Array(
            rows.par_iter()
                .map(|x| Json::Bool(x.get(&key).is_some()))
                .collect(),
        )),
        Cmd::Slice(_, _) => unimplemented!(),
    }
}

/// apply a command to a json value
pub fn apply(cmd: Cmd, val: &Json) -> Res {
    match cmd {
        Cmd::Apply(_lhs, _rhs) => unimplemented!(),
        Cmd::Key(key) => Ok(match val {
            Json::Array(arr) => Json::Array(
                arr.par_iter()
                    .map(|x| x.get(&key))
                    .filter(|x| x.is_some())
                    .map(|x| x.unwrap().clone())
                    .collect(),
            ),
            val => get_key(val, &key),
        }),
        Cmd::Sum(arg) => {
            match *arg {
                Cmd::Key(_key) => {
                    if let Some(_arr) = val.as_array() {
                        //arr.par_iter().reduce_with(|x,y| json_add(x, y))
                        unimplemented!()
                    } else if val.is_number() {
                        Ok(val.clone())
                    } else {
                        Ok(Json::Null)
                    }
                }
                cmd => {
                    let val = apply(cmd, val)?;
                    Ok(json_sum(&val))
                }
            }
        }
        Cmd::Eq(lhs, rhs) => {
            let x = apply(*lhs, &val)?;
            let y = apply(*rhs, &val)?;
            Ok(json_eq(&x, &y))
        }
        Cmd::Json(val) => Ok(val),
        Cmd::Append(_lhs, _rhs) => unimplemented!(),
        Cmd::Bar(_, _) => unimplemented!(),
        Cmd::Set(_, _) => unimplemented!(),
        Cmd::Max(arg) => Ok(json_max(&apply(*arg, val)?).cloned().unwrap_or(Json::Null)),
        Cmd::Min(arg) => Ok(json_min(&apply(*arg, val)?).cloned().unwrap_or(Json::Null)),
        Cmd::Avg(arg) => json_avg(&apply(*arg, val)?),
        Cmd::Delete(_) => unimplemented!(),
        Cmd::StdDev(arg) => json_dev(&apply(*arg, val)?),
        Cmd::Add(x, y) => json_add(&apply(*x, val)?, &apply(*y, val)?),
        Cmd::Sub(x, y) => json_sub(&apply(*x, val)?, &apply(*y, val)?),
        Cmd::Mul(x, y) => json_mul(&apply(*x, val)?, &apply(*y, val)?),
        Cmd::Div(x, y) => json_div(&apply(*x, val)?, &apply(*y, val)?),
        Cmd::First(arg) => Ok(json_first(&apply(*arg, val)?)),
        Cmd::Last(arg) => Ok(json_last(&apply(*arg, val)?)),
        Cmd::Var(arg) => json_var(&apply(*arg, val)?),
        Cmd::Push(_, _) => unimplemented!(),
        Cmd::Pop(_) => unimplemented!(),
        Cmd::Query(_) => unimplemented!(),
        Cmd::Insert(_, _) => unimplemented!(),
        Cmd::Keys(_) => Err(Error::BadCmd),
        Cmd::Len(arg) => Ok(json_count(&apply(*arg, val)?)),
        Cmd::Unique(arg) => Ok(json_unique(&apply(*arg, val)?)),
        Cmd::Summary => unimplemented!(),
        Cmd::Get(key, arg) => Ok(json_get(&key, &apply(*arg, val)?).unwrap_or(Json::Null)),
        Cmd::ToString(arg) => Ok(Json::from(json_tostring(&apply(*arg, val)?))),
        Cmd::Sort(arg, _descend) => {
            let mut val = apply(*arg, val)?;
            json_sort_ascend(&mut val);
            Ok(val)
        }
        Cmd::Reverse(arg) => {
            let mut val = apply(*arg, val)?;
            json_reverse(&mut val);
            Ok(val)
        }
        Cmd::SortBy(arg, key) => {
            let mut val = apply(*arg, val)?;
            json_sortby(&mut val, &key);
            Ok(val)
        }
        Cmd::Median(arg) => {
            let mut val = apply(*arg, val)?;
            json_median(&mut val)?;
            Ok(val)
        }
        Cmd::Eval(cmds) => {
            let mut out = Vec::new();
            for cmd in cmds {
                out.push(apply(cmd, val)?);
            }
            Ok(Json::from(out))
        }
        Cmd::NotEq(lhs, rhs) => Ok(noteq(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Gt(lhs, rhs) => Ok(gt(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Lt(lhs, rhs) => Ok(lt(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Gte(lhs, rhs) => Ok(gte(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Lte(lhs, rhs) => Ok(lte(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::And(lhs, rhs) => json_and(&apply(*lhs, val)?, &apply(*rhs, val)?),
        Cmd::Or(lhs, rhs) => json_or(&apply(*lhs, val)?, &apply(*rhs, val)?),
        Cmd::Map(arg, f) => json_map(&apply(*arg, val)?, f),
        Cmd::In(lhs, rhs) => Ok(json_in(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Flat(arg) => Ok(json_flat(apply(*arg, val)?)),
        Cmd::NumSort(arg, descend) => Ok(numsort(apply(*arg, val)?, descend)),
        Cmd::Has(ref key) => {
            let f = |x: &Json| Json::from(x.get(&key).is_some());
            let out: Json = match val {
                Json::Array(arr) => {
                    let mut out = Vec::new();
                    for val in arr {
                        out.push(f(val));
                    }
                    Json::Array(out)
                }
                val => f(val),
            };
            Ok(out)
        }
        Cmd::Slice(arg, range) => json_slice(apply(*arg, val)?, range),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn apply_key_arr() {
        let cmd = Cmd::Key("a".to_string());
        let val = apply(cmd, &json!([{"a": 1}, {"a": 2}, {"a": 3}]));
        assert_eq!(Ok(json!([1, 2, 3])), val);
    }

    #[test]
    fn apply_key_obj() {
        let cmd = Cmd::Key("a".to_string());
        let val = apply(cmd, &json!({"a": 1, "b": 2}));
        assert_eq!(Ok(json!(1)), val);
    }

    #[test]
    fn apply_key_obj_not_found() {
        let cmd = Cmd::Key("c".to_string());
        let val = apply(cmd, &json!({"a": 1, "b": 2}));
        assert_eq!(Ok(Json::Null), val);
    }

    #[test]
    fn apply_key_arr_not_found() {
        let cmd = Cmd::Key("c".to_string());
        let val = apply(cmd, &json!([{"a": 1}, {"a": 2}, {"a": 3}]));
        assert_eq!(Ok(json!([])), val);
    }

    #[test]
    fn apply_eq_arr() {
        let cmd = Cmd::Eq(
            Box::new(Cmd::Key("a".to_string())),
            Box::new(Cmd::Json(Json::from(2))),
        );
        let val = apply(cmd, &json!([{"a": 1}, {"a": 2}, {"a": 3}]));
        assert_eq!(Ok(json!([false, true, false])), val);
    }
}
