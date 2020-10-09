use crate::cmd::Cmd;
use crate::json::{
    gt, gte, json_and, json_map, json_median, json_or, json_sort_ascend, json_sortby, json_var, lt,
    lte, noteq, numsort, Json,
};
use crate::json::{
    json_add, json_avg, json_count, json_dev, json_div, json_eq, json_first, json_flat, json_get,
    json_in, json_last, json_max, json_min, json_mul, json_reverse, json_sub, json_sum,
    json_tostring, json_unique,
};
use crate::{Error, Res};

pub fn apply(cmd: Cmd, val: &Json) -> Res {
    match cmd {
        Cmd::Apply(_lhs, _rhs) => unimplemented!(),
        Cmd::Key(key) => {
            if let Some(arr) = val.as_array() {
                let mut out = Vec::new();
                for val in arr {
                    if let Some(val) = val.get(&key) {
                        out.push(val.clone());
                    }
                }
                Ok(Json::Array(out))
            } else {
                Ok(if let Some(v) = val.get(&key) {
                    v.clone()
                } else {
                    Json::Null
                })
            }
        }
        Cmd::Sum(arg) => {
            let val = apply(*arg, val)?;
            Ok(json_sum(&val))
        }
        Cmd::Eq(lhs, rhs) => {
            let x = apply(*lhs, &val)?;
            let y = apply(*rhs, &val)?;
            println!("x={:?}\ny={:?}", x, y);
            Ok(json_eq(&x, &y))
        }
        Cmd::Json(val) => Ok(val),
        Cmd::Append(_lhs, _rhs) => unimplemented!(),
        Cmd::Bar(_, _) => unimplemented!(),
        Cmd::Set(_, _) => unimplemented!(),
        Cmd::Max(arg) => Ok(json_max(&apply(*arg, val)?).clone()),
        Cmd::Min(arg) => Ok(json_min(&apply(*arg, val)?).clone()),
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
        Cmd::And(lhs, rhs) => Ok(json_and(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Or(lhs, rhs) => Ok(json_or(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Map(arg, f) => json_map(&apply(*arg, val)?, f),
        Cmd::In(lhs, rhs) => Ok(json_in(&apply(*lhs, val)?, &apply(*rhs, val)?)),
        Cmd::Flat(arg) => Ok(json_flat(apply(*arg, val)?)),
        Cmd::NumSort(arg, descend) => Ok(numsort(apply(*arg, val)?, descend)),
        Cmd::Has(ref key) => {
            let f = |x:&Json| x.get(&key).is_some();
            Ok(match val {
                Json::Array(arr) => {
                    let mut out = Vec::new();
                    for val in arr {
                        out.push(apply(cmd.clone(), val)?);
                    }
                    Json::Array(out)
                }
                val => Json::Bool(f(val)),
            })
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
