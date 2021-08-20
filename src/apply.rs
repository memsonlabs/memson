use crate::cmd::{Cmd, Range};
use crate::db::PAGE_SIZE;
use crate::json::*;
use crate::Error;
use rayon::prelude::*;

/// retrieves the key/val entry from a row by key
fn get_key(row: &Json, key: &str) -> Json {
    row.get(key).cloned().unwrap_or(Json::Null)
}

/// apply an unary function
fn apply_unr_fn<F>(arg: Cmd, rows: &[Json], f: F) -> Result<Json, Error>
where
    F: FnOnce(&Json) -> Result<Json, Error>,
{
    let val = apply_rows(arg, rows)?;
    f(&val)
}

/// apply a binary function
fn apply_bin_fn<F>(lhs: Cmd, rhs: Cmd, rows: &[Json], f: F) -> Result<Json, Error>
where
    F: FnOnce(&Json, &Json) -> Result<Json, Error>,
{
    let x = apply_rows(lhs, rows)?;
    let y = apply_rows(rhs, rows)?;
    f(&x, &y)
}

/// apply boolean function to process binary gates
fn apply_bool_fn<F>(lhs: Cmd, rhs: Cmd, rows: &[Json], f: F) -> Result<Json, Error>
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

fn apply_sum(arg: Cmd, rows: &[Json]) -> Result<Json, Error> {
    match arg {
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
    }
}

fn apply_max(arg: Cmd, rows: &[Json]) -> Result<Json, Error> {
    match arg {
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
                            if gt(x, y) {
                                Some(x)
                            } else {
                                Some(y)
                            }
                        } else {
                            Some(y)
                        }
                    },
                )
                .reduce(
                    || None,
                    |x, y| match (x, y) {
                        (Some(x), Some(y)) => Some(if gt(x, y) { x } else { y }),
                        (Some(x), None) | (None, Some(x)) => Some(x),
                        (None, None) => None,
                    },
                );
            Ok(val.cloned().unwrap_or(Json::Null))
        }
        val => Ok(json_max(&apply_rows(val, rows)?)
            .cloned()
            .unwrap_or(Json::Null)),
    }
}

fn apply_eval(cmd: Cmd, rows: &[Json]) -> Result<Json, Error> {
    apply_rows(cmd, rows)
}

fn apply_get(key: String, arg: Cmd, rows: &[Json]) -> Result<Json, Error> {
    let val = apply_rows(arg, rows)?;
    json_get(&key, val).ok_or(Error::BadKey(key))
}

fn apply_median(arg: Cmd, rows: &[Json]) -> Result<Json, Error> {
    let val = apply_rows(arg, rows)?;
    json_median(&val)
}

fn apply_map(arg: Cmd, f: String, rows: &[Json]) -> Result<Json, Error> {
    let val = apply_rows(arg, rows)?;
    json_map(&val, f)
}

fn apply_flat(arg: Cmd, rows: &[Json]) -> Result<Json, Error> {
    let val = apply_rows(arg, rows)?;
    Ok(json_flat(&val))
}

fn apply_has(key: String, rows: &[Json]) -> Result<Json, Error> {
    Ok(Json::Array(
        rows.par_iter()
            .map(|x| Json::Bool(x.get(&key).is_some()))
            .collect(),
    ))
}

fn apply_reverse(arg: Cmd, rows: &[Json]) -> Result<Json, Error> {
    let mut val = apply_rows(arg, rows)?;
    json_reverse(&mut val);
    Ok(val)
}

fn apply_sort(arg: Cmd, descend: Option<bool>, rows: &[Json]) -> Result<Json, Error> {
    let mut val = apply_rows(arg, rows)?;
    json_sort(&mut val, descend.unwrap_or(false));
    Ok(val)
}

fn apply_keys(page: Option<Range>, rows: &[Json]) -> Result<Json, Error> {
    if let Some(page) = page {
        let start = page.start.unwrap_or(0);
        let n = page.size.unwrap_or(PAGE_SIZE);
        Ok(Json::Array(
            rows.par_iter().skip(start).take(n).cloned().collect(),
        ))
    } else {
        Ok(Json::Array(
            rows.par_iter().take(PAGE_SIZE).cloned().collect(),
        ))
    }
}

fn apply_numsort(arg: Cmd, descend: bool, rows: &[Json]) -> Result<Json, Error> {
    let val = apply_rows(arg, rows)?;
    Ok(json_numsort(val, descend))
}

fn apply_slice(arg: Cmd, range: Range, rows: &[Json]) -> Result<Json, Error> {
    let val = apply_rows(arg, rows)?;
    json_slice(val, range)
}

/// apply a cmd to rows of json
pub fn apply_rows(cmd: Cmd, rows: &[Json]) -> Result<Json, Error> {
    match cmd {
        Cmd::Key(key) => Ok(apply_key(key, rows)),
        Cmd::Sum(arg) => apply_sum(*arg, rows),
        Cmd::Max(arg) => apply_max(*arg, rows),
        Cmd::Append(_, _) => Err(Error::BadCmd),
        Cmd::Apply(_, _) => Err(Error::BadCmd),
        Cmd::Bar(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_bar),
        Cmd::Set(_, _) => Err(Error::BadCmd),
        Cmd::Min(arg) => apply_unr_fn(*arg, rows, |x| {
            Ok(json_min(x).cloned().unwrap_or(Json::Null))
        }),
        Cmd::Avg(arg) => apply_unr_fn(*arg, rows, json_avg),
        Cmd::Delete(_) => Err(Error::BadCmd),
        Cmd::Dev(arg) => apply_unr_fn(*arg, rows, json_dev),
        Cmd::Add(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_add),
        Cmd::Sub(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_sub),
        Cmd::Mul(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_mul),
        Cmd::Div(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_div),
        Cmd::First(arg) => apply_unr_fn(*arg, rows, |x| Ok(json_first(x).unwrap_or(Json::Null))),
        Cmd::Last(arg) => apply_unr_fn(*arg, rows, |x| {
            Ok(json_last(x).cloned().unwrap_or(Json::Null))
        }),
        Cmd::Var(arg) => apply_unr_fn(*arg, rows, json_var),
        Cmd::Push(_, _) => Err(Error::BadCmd),
        Cmd::Pop(_) => Err(Error::BadCmd),
        Cmd::Query(_) => Err(Error::BadCmd),
        Cmd::Insert(_, _) => Err(Error::BadCmd),
        Cmd::Keys(page) => apply_keys(page, rows),
        Cmd::Len(arg) => apply_unr_fn(*arg, rows, |x| Ok(json_count(x))),
        Cmd::Unique(arg) => apply_unr_fn(*arg, rows, |x| Ok(json_unique(x))),
        Cmd::Json(val) => Ok(val),
        Cmd::Summary => Err(Error::BadCmd),
        Cmd::Get(key, arg) => apply_get(key, *arg, rows),
        Cmd::ToString(arg) => apply_unr_fn(*arg, rows, |x| Ok(Json::from(json_tostring(x)))),
        Cmd::Sort(arg, descend) => apply_sort(*arg, descend, rows),
        Cmd::Reverse(arg) => apply_reverse(*arg, rows),
        Cmd::SortBy(arg, key) => apply_sortby(*arg, key, rows),
        Cmd::Median(arg) => apply_median(*arg, rows),
        Cmd::Eval(cmd) => apply_eval(*cmd, rows),
        Cmd::Eq(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, |x, y| Ok(json_eq(x, y))),
        Cmd::NotEq(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, |x, y| Ok(json_not_eq(x, y))),
        Cmd::Gt(lhs, rhs) => apply_bool_fn(*lhs, *rhs, rows, gt),
        Cmd::Lt(lhs, rhs) => apply_bool_fn(*lhs, *rhs, rows, lt),
        Cmd::Gte(lhs, rhs) => apply_bool_fn(*lhs, *rhs, rows, gte),
        Cmd::Lte(lhs, rhs) => apply_bool_fn(*lhs, *rhs, rows, lte),
        Cmd::And(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_and),
        Cmd::Or(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, json_or),
        Cmd::Map(arg, f) => apply_map(*arg, f, rows),
        Cmd::In(lhs, rhs) => apply_bin_fn(*lhs, *rhs, rows, |x, y| Ok(json_in(x, y))),
        Cmd::Flat(arg) => apply_flat(*arg, rows),
        Cmd::NumSort(arg, descend) => apply_numsort(*arg, descend, rows),
        Cmd::Has(key) => apply_has(key, rows),
        Cmd::Slice(arg, range) => apply_slice(*arg, range, rows),
        Cmd::InnerJoin(_, _, _, _, _) => unimplemented!(),
        Cmd::OuterJoin(_, _, _, _, _) => unimplemented!(),
    }
}

fn apply_sortby(arg: Cmd, key: String, rows: &[Json]) -> Result<Json, Error> {
    let mut val = apply_rows(arg, rows)?;
    json_sortby(&mut val, &key);
    Ok(val)
}

fn apply_key2(key: String, val: &Json) -> Result<Json, Error> {
    Ok(match val {
        Json::Array(arr) => Json::Array(
            arr.par_iter()
                .map(|x| x.get(&key))
                .filter(|x| x.is_some())
                .map(|x| x.unwrap().clone())
                .collect(),
        ),
        val => get_key(val, &key),
    })
}

fn apply_sum2(arg: Cmd, val: &Json) -> Result<Json, Error> {
    match arg {
        Cmd::Key(_key) => {
            if let Some(arr) = val.as_array() {
                let val: Json = arr
                    .par_iter()
                    .fold(|| Json::from(0), json_fold_add)
                    .reduce(|| Json::from(0), json_reduce_add);
                Ok(val)
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

fn apply_eq2(lhs: Cmd, rhs: Cmd, val: &Json) -> Result<Json, Error> {
    let x = apply(lhs, val)?;
    let y = apply(rhs, val)?;
    Ok(json_eq(&x, &y))
}

fn apply_bar2(lhs: Cmd, rhs: Cmd, val: &Json) -> Result<Json, Error> {
    let x = apply(lhs, val)?;
    let y = apply(rhs, val)?;
    json_bar(&x, &y)
}

/// apply a command to a json value
pub fn apply(cmd: Cmd, val: &Json) -> Result<Json, Error> {
    match cmd {
        Cmd::Apply(_lhs, _rhs) => Err(Error::BadCmd),
        Cmd::Key(key) => apply_key2(key, val),
        Cmd::Sum(arg) => apply_sum2(*arg, val),
        Cmd::Eq(lhs, rhs) => apply_eq2(*lhs, *rhs, val),
        Cmd::Json(val) => Ok(val),
        Cmd::Append(_, _) => Err(Error::BadCmd),
        Cmd::Bar(lhs, rhs) => apply_bar2(*lhs, *rhs, val),
        Cmd::Set(_, _) => Err(Error::BadCmd),
        Cmd::Max(arg) => Ok(json_max(&apply(*arg, val)?).cloned().unwrap_or(Json::Null)),
        Cmd::Min(arg) => Ok(json_min(&apply(*arg, val)?).cloned().unwrap_or(Json::Null)),
        Cmd::Avg(arg) => json_avg(&apply(*arg, val)?),
        Cmd::Delete(_) => Err(Error::BadCmd),
        Cmd::Dev(arg) => json_dev(&apply(*arg, val)?),
        Cmd::Add(x, y) => json_add(&apply(*x, val)?, &apply(*y, val)?),
        Cmd::Sub(x, y) => json_sub(&apply(*x, val)?, &apply(*y, val)?),
        Cmd::Mul(x, y) => json_mul(&apply(*x, val)?, &apply(*y, val)?),
        Cmd::Div(x, y) => json_div(&apply(*x, val)?, &apply(*y, val)?),
        Cmd::First(arg) => {
            let val: Json = apply(*arg, val)?;
            let arg = json_first(&val).unwrap_or(Json::Null);
            Ok(arg)
        }
        Cmd::Last(arg) => {
            let val = apply(*arg, val)?;
            let val = json_last(&val).cloned().unwrap_or(Json::Null);
            Ok(val)
        }
        Cmd::Var(arg) => json_var(&apply(*arg, val)?),
        Cmd::Push(_, _) => Err(Error::BadCmd),
        Cmd::Pop(_) => Err(Error::BadCmd),
        Cmd::Query(_) => Err(Error::BadCmd),
        Cmd::Insert(_, _) => Err(Error::BadCmd),
        Cmd::Keys(_) => Err(Error::BadCmd),
        Cmd::Len(arg) => Ok(json_count(&apply(*arg, val)?)),
        Cmd::Unique(arg) => Ok(json_unique(&apply(*arg, val)?)),
        Cmd::Summary => Err(Error::BadCmd),
        Cmd::Get(key, arg) => Ok(json_get(&key, apply(*arg, val)?).unwrap_or(Json::Null)),
        Cmd::ToString(arg) => Ok(Json::from(json_tostring(&apply(*arg, val)?))),
        Cmd::Sort(arg, descend) => {
            let mut val = apply(*arg, val)?;
            json_sort(&mut val, descend.unwrap_or(false));
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
            let val = apply(*arg, val)?;
            json_median(&val)?;
            Ok(val)
        }
        Cmd::Eval(cmd) => {
            let val = apply(*cmd, val)?;
            Ok(Json::from(vec![val]))
        }
        Cmd::NotEq(lhs, rhs) => Ok(noteq(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Gt(lhs, rhs) => Ok(json_gt(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Lt(lhs, rhs) => Ok(json_lt(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Gte(lhs, rhs) => Ok(json_gte(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Lte(lhs, rhs) => Ok(json_lte(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::And(lhs, rhs) => json_and(&apply(*lhs, val)?, &apply(*rhs, val)?),
        Cmd::Or(lhs, rhs) => json_or(&apply(*lhs, val)?, &apply(*rhs, val)?),
        Cmd::Map(arg, f) => json_map(&apply(*arg, val)?, f),
        Cmd::In(lhs, rhs) => Ok(json_in(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Flat(arg) => {
            let val: Json = apply(*arg, val)?;
            Ok(json_flat(&val))
        }
        Cmd::NumSort(arg, descend) => Ok(json_numsort(apply(*arg, val)?, descend)),
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
        Cmd::Slice(arg, range) => {
            let val: Json = apply(*arg, val)?;
            let val = json_slice(val, range)?;
            Ok(val)
        }
        Cmd::InnerJoin(_, _, _, _, _) => {
            unimplemented!()
        }
        Cmd::OuterJoin(_, _, _, _, _) => {
            unimplemented!()
        }
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
