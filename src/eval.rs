use crate::apply::apply;
use crate::cmd::{Cache, Cmd, Query, QueryCmd};
use crate::json::*;
use crate::Error;
use crate::Res;
use core::option::Option::Some;
use std::collections::HashMap;

fn eval_sort_cmd(cache: &mut Cache, arg: Cmd) -> Res {
    let mut val = eval_cmd(cache, arg)?;
    json_sort_ascend(&mut val);
    Ok(val)
}

fn set<K: Into<String>>(cache: &mut Cache, key: K, val: Json) -> Option<Json> {
    cache.insert(key.into(), val)
}

fn delete(cache: &mut Cache, key: &str) -> Res {
    Ok(cache.remove(key).unwrap_or(null()))
}

pub fn eval_cmd(cache: &mut Cache, cmd: Cmd) -> Res {
    match cmd {
        Cmd::Apply(lhs, rhs) => {
            let val = eval_cmd(cache, *rhs)?;
            apply(*lhs, &val)
        }
        Cmd::Add(lhs, rhs) => eval_bin_fn(cache, *lhs, *rhs, json_add),
        Cmd::Append(_key, _arg) => {
            //let val = self.eval(*arg)?;
            //self.append(key, val.into_val())
            unimplemented!()
        }
        Cmd::Avg(arg) => eval_unr_fn(cache, *arg, json_avg),
        Cmd::Bar(lhs, rhs) => eval_bin_fn(cache, *lhs, *rhs, json_bar),
        Cmd::Len(arg) => eval_unr_fn(cache, *arg, &count),
        Cmd::Delete(key) => delete(cache, &key),
        Cmd::Div(lhs, rhs) => eval_bin_fn(cache, *lhs, *rhs, json_div),
        Cmd::First(arg) => eval_unr_fn(cache, *arg, |x| Ok(json_first(x))),
        Cmd::Get(key, arg) => {
            let val = eval_cmd(cache, *arg)?;
            Ok(json_get(&key, &val).unwrap_or(Json::Null))
        }
        Cmd::Insert(key, arg) => {
            let val = get_mut(cache, key)?;
            let n = arg.len();
            json_insert(val, arg);
            Ok(Json::from(n))
        }
        Cmd::Json(val) => Ok(val),
        Cmd::Keys(_page) => unimplemented!(),
        Cmd::Last(arg) => eval_unr_fn(cache, *arg, |x| Ok(json_last(x))),
        Cmd::Max(arg) => Ok(json_max(&eval_cmd(cache, *arg)?).cloned().unwrap_or(Json::Null)),
        Cmd::In(lhs, rhs) => eval_bin_fn(cache, *lhs, *rhs, |x, y| Ok(Json::from(json_in(x, y)))),
        Cmd::Min(arg) => Ok(json_min(&eval_cmd(cache, *arg)?).cloned().unwrap_or(Json::Null)),
        Cmd::Mul(lhs, rhs) => eval_bin_fn(cache, *lhs, *rhs, json_mul),
        Cmd::Push(key, arg) => {
            let val = eval_cmd(cache, *arg)?;
            let kv = cache.get_mut(&key).ok_or(Error::BadKey(key))?;
            json_push(kv, val);
            Ok(Json::Null)
        }
        Cmd::Pop(key) => Ok(pop(cache, key)?.unwrap_or(Json::Null)),
        Cmd::Query(cmd) => query(cache, cmd),
        Cmd::Set(key, arg) => {
            let val = eval_cmd(cache, *arg)?;
            Ok(set(cache, key, val).unwrap_or(null()))
        }
        Cmd::Sort(arg, _) => eval_sort_cmd(cache, *arg),
        Cmd::StdDev(arg) => eval_unr_fn(cache, *arg, json_dev),
        Cmd::Sub(lhs, rhs) => eval_bin_fn(cache, *lhs, *rhs, json_sub),
        Cmd::Sum(arg) => eval_unr_fn(cache, *arg, |x| Ok(json_sum(x))),
        Cmd::Summary => unimplemented!(),
        Cmd::Unique(arg) => eval_unr_fn(cache, *arg, unique),
        Cmd::Var(arg) => eval_unr_fn(cache, *arg, json_var),
        Cmd::ToString(arg) => Ok(eval_cmd(cache, *arg)?),
        Cmd::Key(key) => {
            let mut it = key.split('.');
            let key = it.next().ok_or(Error::BadKey(key.clone()))?;
            let mut ref_val = cache.get(key).ok_or(Error::BadKey(key.to_string()))?;
            let mut fat_val = None;
            for key in it {
                match ref_val {
                    Json::Array(arr) => {
                        let mut out = Vec::new();
                        for val in arr {
                            if let Some(val) = val.get(key) {
                                out.push(val.clone());
                            }
                        }
                        fat_val = Some(out);
                    }
                    val => {
                        ref_val = val.get(key).ok_or(Error::BadKey(key.to_string()))?;
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
            let mut val = eval_cmd(cache, *arg)?;
            json_reverse(&mut val);
            Ok(val)
        }
        Cmd::Median(arg) => {
            let mut val = eval_cmd(cache, *arg)?;
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
            let val = eval_cmd(cache, *arg)?;
            json_map(&val, f)
        }
        Cmd::Flat(arg) => {
            let val = eval_cmd(cache, *arg)?;
            Ok(json_flat(val))
        }
        Cmd::NumSort(arg, descend) => {
            let val = eval_cmd(cache, *arg)?;
            Ok(json_numsort(val, descend))
        }
        Cmd::Has(key) => Ok(Json::Bool(cache.get(&key).is_some())),
    }
}

fn query(cache: &Cache, cmd: QueryCmd) -> Res {
    let qry = Query::from(cache, cmd);
    qry.exec()
}

pub fn pop(cache: &mut Cache, key: String) -> Result<Option<Json>, Error> {
    let val = get_mut(cache, key)?;
    json_pop(val)
}

fn get_mut(cache: &mut Cache, key: String) -> Result<&mut Json, Error> {
    Ok(cache.get_mut(&key).ok_or(Error::BadKey(key))?)
}

fn eval_bin_fn<F>(cache: &mut Cache, lhs: Cmd, rhs: Cmd, f: F) -> Res
where
    F: Fn(&Json, &Json) -> Result<Json, Error>,
{
    let x = eval_cmd(cache, lhs)?;
    let y = eval_cmd(cache, rhs)?;
    f(&x, &y)
}

fn eval_unr_fn<F>(cache: &mut Cache, arg: Cmd, f: F) -> Res
where
    F: Fn(&Json) -> Result<Json, Error>,
{
    let val = eval_cmd(cache, arg)?;
    f(&val)
}

pub fn eval_filter(cmd: Cmd, val: &Json) -> Option<bool> {
    let r= apply(cmd.clone(), val).ok();
    if let Some(g) = r {
        g.as_bool()
    } else {
        None
    }
}

pub fn eval_row_cmd(cmd: Cmd, row: &Json) -> Option<Json> {
    apply(cmd, row).ok()
    /*
    match cmd {
        Cmd::Key(key) => row.get(key).cloned(),
        Cmd::Mul(lhs, rhs) => {
            eval_row_bin_cmd(lhs.as_ref(), rhs.as_ref(), row, |x, y| json_mul(x, y).ok())
        }
        Cmd::Append(_, _) => todo!("throw error instead of None"),
        Cmd::Bar(arg, interval) => {
            let val = eval_row_cmd(arg.as_ref(), row)?;
            let interval = eval_row_cmd(interval.as_ref(), row)?;
            json_bar(&val, &interval).ok()
        }
        Cmd::Set(_, _) => todo!("throw error instead of None"),
        Cmd::Max(_) => None,
        Cmd::Min(_) => None,
        Cmd::Avg(_) => None,
        Cmd::Delete(_) => todo!("throw error instead of None"),
        Cmd::StdDev(_) => None,
        Cmd::Sum(_) => None,
        Cmd::Add(lhs, rhs) => match (lhs.as_ref(), rhs.as_ref()) {
            (Cmd::Key(lhs), Cmd::Key(rhs)) => {
                if let (Some(x), Some(y)) = (row.get(lhs), row.get(rhs)) {
                    json_add(x, y).ok()
                } else {
                    None
                }
            }
            (lhs, rhs) => {
                let x = eval_row_cmd(lhs, row)?;
                let y = eval_row_cmd(rhs, row)?;
                json_add(&x, &y).ok()
            }
        },
        Cmd::Sub(_, _) => unimplemented!(),
        Cmd::Div(_, _) => unimplemented!(),
        Cmd::First(_) => None,
        Cmd::Last(_) => None,
        Cmd::Var(_) => None,
        Cmd::Push(_, _) => todo!("throw error instead of None"),
        Cmd::Pop(_) => todo!("throw error instead of None"),
        Cmd::Query(_) => todo!("throw error instead of None"),
        Cmd::Insert(_, _) => todo!("throw error instead of None"),
        Cmd::Keys(_) => todo!("throw error instead of None"),
        Cmd::Len(_) => None,
        Cmd::Unique(_) => None,
        Cmd::Json(val) => Some(val.clone()),
        Cmd::Summary => todo!("throw error instead of None"),
        Cmd::Get(key, arg) => {
            let val = eval_row_cmd(arg.as_ref(), row)?;
            val.get(key).cloned()
        }
        Cmd::ToString(arg) => {
            let val = eval_row_cmd(arg.as_ref(), row)?;
            Some(json_string(&val))
        }
        Cmd::Sort(_, _) => None,
        Cmd::Reverse(_) => None,
        Cmd::SortBy(_, _) => None,
        Cmd::Median(_) => None,
        Cmd::Eval(_) => todo!("throw an error here"),
        Cmd::Eq(lhs, rhs) => {
            let x = eval_row_cmd(lhs, row)?;
            let y = eval_row_cmd(rhs, row)?;
            Some(Json::from(json_equal(&x, &y)))
        }
        Cmd::NotEq(lhs, rhs) => {
            let x = eval_row_cmd(lhs, row)?;
            let y = eval_row_cmd(rhs, row)?;
            Some(Json::from(json_neq(&x, &y)))
        }
        Cmd::Gt(lhs, rhs) => {
            let x = eval_row_cmd(lhs, row)?;
            let y = eval_row_cmd(rhs, row)?;
            Some(Json::from(json_gt(&x, &y)))
        }
        Cmd::Lt(lhs, rhs) => {
            let x = eval_row_cmd(lhs, row)?;
            let y = eval_row_cmd(rhs, row)?;
            Some(Json::from(json_lt(&x, &y)))
        }
        Cmd::Gte(lhs, rhs) => {
            let x = eval_row_cmd(lhs, row)?;
            let y = eval_row_cmd(rhs, row)?;
            Some(Json::from(json_gte(&x, &y)))
        }
        Cmd::Lte(lhs, rhs) => {
            let x = eval_row_cmd(lhs, row)?;
            let y = eval_row_cmd(rhs, row)?;
            Some(Json::from(json_lte(&x, &y)))
        }
        Cmd::And(lhs, rhs) => {
            let x = eval_row_cmd(lhs, row)?;
            let y = eval_row_cmd(rhs, row)?;
            Some(Json::from(json_and(&x, &y)))
        }
        Cmd::Or(lhs, rhs) => {
            let x = eval_row_cmd(lhs, row)?;
            let y = eval_row_cmd(rhs, row)?;
            Some(Json::from(json_or(&x, &y)))
        }
        Cmd::NumSort(_, _) => unimplemented!(),
        Cmd::Map(_, _) => unimplemented!(),
        Cmd::In(_, _) => unimplemented!(),
        Cmd::Flat(_) => unimplemented!(),
    }
    */
}

pub fn eval_row_bin_cmd<F>(_lhs: &Cmd, _rhs: &Cmd, _row: &JsonObj, _f: F) -> Option<Json>
where
    F: Fn(&Json, &Json) -> Option<Json>,
{
    unimplemented!()
    //let x = eval_row_cmd(lhs, row)?;
    //let y = eval_row_cmd(rhs, row)?;
    //f(&x, &y)
}

pub fn eval_nonaggregate(selects: &HashMap<String, Cmd>, rows: &[Json]) -> Result<Json, Error> {
    let mut out = Vec::new();
    for row in rows {
        let mut obj = JsonObj::new();
        for (col_name, cmd) in selects {
            //eval_row(&mut obj, select, row)?;
            if let Some(_) = row.as_object() {
                if let Some(val) = eval_row_cmd(cmd.clone(), row) {
                    obj.insert(col_name.to_string(), val);
                }
            }
        }
        if !obj.is_empty() {
            out.push(Json::from(obj));
        }
    }
    Ok(Json::Array(out))
}

pub fn eval_reduce_bin_cmd<F>(_lhs: &Cmd, _rhs: &Cmd, _rows: &[Json], _f: F) -> Option<Json>
where
    F: FnOnce(&Json, &Json) -> Result<Json, Error>,
{
    unimplemented!()
}

//TODO refactor to take Json val instead of rows to make more generic
pub fn eval_rows_cmd(cmd: Cmd, rows: &Json) -> Option<Json> {
    apply(cmd, rows).ok()
    /*
    match cmd {
        Cmd::Append(_, _) => None,
        Cmd::Get(key, arg) => {
            if let Some(val) = eval_rows_cmd(arg.as_ref(), rows) {
                json_get(key, &val) //TODO remove cloned
            } else {
                None
            }
        }
        Cmd::Key(key) => {
            let mut output = Vec::new();
            for row in rows {
                if let Some(val) = json_get(key, row) {
                    output.push(val.clone());
                }
            }
            if output.is_empty() {
                None
            } else {
                Some(Json::from(output))
            }
        }
        Cmd::First(arg) => eval_rows_cmd(arg.as_ref(), rows).map(|x| json_first(&x)),
        Cmd::Last(arg) => eval_rows_cmd(arg.as_ref(), rows).map(|x| json_last(&x)),
        Cmd::Sum(arg) => eval_rows_cmd(arg.as_ref(), rows).map(|x| json_sum(&x)),
        Cmd::Len(arg) => eval_rows_cmd(arg.as_ref(), rows).map(|x| json_count(&x)),
        Cmd::Min(arg) => eval_rows_cmd(arg.as_ref(), rows).map(|x| json_min(&x).clone()),
        Cmd::Max(arg) => eval_rows_cmd(arg.as_ref(), rows).map(|x| json_max(&x).clone()),
        Cmd::Flat(arg) => eval_rows_cmd(arg.as_ref(), rows).map(json_flat),
        Cmd::Avg(arg) => json_avg(&eval_rows_cmd(arg.as_ref(), rows)?).ok(),
        Cmd::Var(arg) => {
            if let Some(ref arg) = eval_rows_cmd(arg.as_ref(), rows) {
                json_var(arg).ok()
            } else {
                None
            }
        }
        Cmd::StdDev(arg) => {
            if let Some(ref arg) = eval_rows_cmd(arg.as_ref(), rows) {
                json_dev(arg).ok()
            } else {
                None
            }
        }
        Cmd::Json(val) => Some(val.clone()),
        Cmd::Unique(arg) => eval_rows_cmd(arg.as_ref(), rows).map(|x| json_unique(&x)),
        Cmd::Bar(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_bar),
        Cmd::Add(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_add),
        Cmd::Sub(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_sub),
        Cmd::Mul(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_mul),
        Cmd::Div(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_div),
        Cmd::Set(_, _) => None,
        Cmd::Delete(_) => None,
        Cmd::Pop(_) => None,
        Cmd::Query(_) => None,
        Cmd::Insert(_, _) => None,
        Cmd::Push(_, _) => None,
        Cmd::Keys(_) => None,
        Cmd::Summary => None,
        Cmd::ToString(arg) => eval_rows_cmd(arg.as_ref(), rows).map(|x| json_string(&x)),
        Cmd::Sort(_, _) => unimplemented!(),
        Cmd::Median(_) => unimplemented!(),
        Cmd::SortBy(_, _) => unimplemented!(),
        Cmd::Reverse(_) => unimplemented!(),
        Cmd::Eval(_) => unimplemented!(),
        Cmd::Eq(_, _) => unimplemented!(),
        Cmd::NotEq(_, _) => unimplemented!(),
        Cmd::Gt(_, _) => unimplemented!(),
        Cmd::Lt(_, _) => unimplemented!(),
        Cmd::Gte(_, _) => unimplemented!(),
        Cmd::Lte(_, _) => unimplemented!(),
        Cmd::And(_, _) => unimplemented!(),
        Cmd::Or(_, _) => unimplemented!(),
        Cmd::Map(_, _) => unimplemented!(),
        Cmd::In(_, _) => unimplemented!(),
        Cmd::NumSort(_, _) => unimplemented!(),
    }
    */
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn eval_in() {
        let mut cache = Cache::new();
        cache.insert("nums".to_string(), json!([1, 2, 3, 4, 5]));
        let cmd = Cmd::In(
            Box::new(Cmd::Key("nums".to_string())),
            Box::new(Cmd::Json(json!(3))),
        );
        assert_eq!(
            Ok(json!([false, false, true, false, false])),
            eval_cmd(&mut cache, cmd)
        );
    }
}
