use crate::cmd::Cmd;
use crate::err::Error;
use crate::json::*;

fn eval_keyed_bin_cmd<F>(lhs: &Cmd, rhs: &Cmd, val: &Json, f: F) -> Result<Json, Error>
where
    F: FnOnce(&Json, &Json) -> Result<Json, Error>,
{
    let x = eval_keyed_cmd(lhs, val)?;
    let y = eval_keyed_cmd(rhs, val)?;
    f(&x, &y)
}

fn eval_keyed_unr_cmd<F>(arg: &Cmd, val: &Json, f: F) -> Result<Json, Error>
where
    F: FnOnce(&Json) -> Result<Json, Error>,
{
    let val = eval_keyed_cmd(arg, val)?;
    f(&val)
}

fn eval_keyed_unr_mut_cmd<F>(arg: &Cmd, val: &Json, f: F) -> Result<Json, Error>
where
    F: FnOnce(&mut Json),
{
    let mut val = eval_keyed_cmd(arg, val)?;
    f(&mut val);
    Ok(val)
}

pub fn eval_keyed_cmd(cmd: &Cmd, val: &Json) -> Result<Json, Error> {
    match cmd {
        Cmd::Add(lhs, rhs) => eval_keyed_bin_cmd(lhs, rhs, val, json_add),
        Cmd::Append(_, _) => Err(Error::BadCmd),
        Cmd::Avg(arg) => eval_keyed_unr_cmd(arg, val, json_avg),
        Cmd::Bar(lhs, rhs) => eval_keyed_bin_cmd(lhs, rhs, val, json_bar),
        Cmd::Len(arg) => eval_keyed_unr_cmd(arg, val, |x| Ok(json_count(x))),
        Cmd::Delete(_) => Err(Error::BadCmd),

        Cmd::Div(lhs, rhs) => eval_keyed_bin_cmd(lhs, rhs, val, json_div),
        Cmd::First(arg) => eval_keyed_unr_cmd(arg, val, |x| Ok(json_first(x))),
        Cmd::Get(key, arg) => {
            let val = eval_keyed_cmd(arg.as_ref(), val)?;
            json_get(key, &val).ok_or(Error::BadKey)
        }
        Cmd::Insert(_, _) => Err(Error::BadCmd),
        Cmd::Json(val) => Ok(val.clone()),
        Cmd::Key(key) => json_get(key, val).ok_or(Error::BadKey),
        Cmd::Keys(_) => Err(Error::BadCmd),
        Cmd::Last(arg) => eval_keyed_unr_cmd(arg, val, |x| Ok(json_last(x))),
        Cmd::Max(arg) => eval_keyed_unr_cmd(arg, val, |x| Ok(json_max(x).clone())),
        Cmd::Min(arg) => eval_keyed_unr_cmd(arg, val, |x| Ok(json_min(x).clone())),
        Cmd::Mul(lhs, rhs) => {
            match (lhs.as_ref(), rhs.as_ref()) {
                (Cmd::Key(lhs), Cmd::Key(rhs)) => {
                    let mut out = Vec::new();
                    if let Some(rows) = val.as_array() {
                        for row in rows {
                            match (row.get(lhs), row.get(rhs)) {
                                (Some(x), Some(y)) => {
                                    out.push(json_mul(x, y)?);
                                }
                                _ => ()
                            }
                        }

                    }
                    Ok(Json::Array(out))
                }
                (lhs, rhs) =>  eval_keyed_bin_cmd(lhs, rhs, val, json_mul)
            }

        },
        Cmd::Pop(_) => Err(Error::BadCmd),
        Cmd::Push(_, _) => Err(Error::BadCmd),
        Cmd::Query(_) => Err(Error::BadCmd),
        Cmd::Set(_, _) => Err(Error::BadCmd),
        Cmd::StdDev(arg) => eval_keyed_unr_cmd(arg, val, json_dev),
        Cmd::Sub(lhs, rhs) => eval_keyed_bin_cmd(lhs, rhs, val, json_sub),
        Cmd::Sum(arg) => eval_keyed_unr_cmd(arg, val, |x| Ok(json_sum(x))),
        Cmd::Summary => Err(Error::BadCmd),
        Cmd::ToString(arg) => eval_keyed_unr_cmd(arg, val, |x| Ok(Json::from(json_tostring(x)))),
        Cmd::Unique(arg) => eval_keyed_unr_cmd(arg, val, |x| Ok(json_unique(x))),
        Cmd::Var(arg) => eval_keyed_unr_cmd(arg, val, json_var),
        Cmd::Sort(arg) => eval_keyed_unr_mut_cmd(arg, val, json_sort),
        Cmd::Reverse(arg) => eval_keyed_unr_mut_cmd(arg, val, json_reverse),
        Cmd::SortBy(arg, sort_key) => {
            let mut val = eval_keyed_cmd(arg.as_ref(), val)?;
            json_sortby(&mut val, sort_key);
            Ok(val)
        }
        Cmd::Median(arg) => {
            let mut val = eval_keyed_cmd(arg.as_ref(), val)?;
            json_median(&mut val)
        }
        Cmd::Eval(_) => Err(Error::BadCmd),
    }
}

pub fn keyed_reduce(input: &JsonObj, reductions: &[(&String, &Cmd)]) -> Result<JsonObj, Error> {
    let mut output = JsonObj::new();
    for (key, val) in input {
        let mut obj = JsonObj::new();
        for select in reductions {
            let keyed_val = eval_keyed_cmd(select.1, val)?;
            obj.insert(select.0.to_string(), keyed_val);
        }
        if !obj.is_empty() {
            output.insert(key.to_string(), Json::from(obj));
        }
    }
    Ok(output)
}

fn obj(val: Json) -> JsonObj {
    match val {
        Json::Object(obj) => obj,
        _ => panic!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn len(key: &str) -> Cmd {
        Cmd::Len(Box::new(Cmd::Key(key.to_string())))
    }

    fn max(key: &str) -> Cmd {
        Cmd::Max(Box::new(Cmd::Key(key.to_string())))
    }

    fn min(key: &str) -> Cmd {
        Cmd::Min(Box::new(Cmd::Key(key.to_string())))
    }

    fn data() -> Json {
        json!({
        "\"james\"": [{"age": 20},{"age": 40},{"age": 22},{"age": 35}],
        "\"ania\"": [{"age": 21},{"age": 20}],
        "\"misha\"": [{"age": 9}]})
    }

    #[test]
    fn keyed_len_aggregate() {
        use serde_json::json;
        let data = data();
        let data = obj(data);
        let name = "total".to_string();
        let output = keyed_reduce(&data, &[(&name, &len("age"))]).unwrap();

        assert_eq!(
            json!({
                "\"james\"": {"total": 4},
                "\"ania\"": {"total": 2},
                "\"misha\"": {"total": 1},
            }),
            Json::from(output)
        );
    }

    #[test]
    fn keyed_max_reduce() {
        use serde_json::json;
        let data = data();
        let data = obj(data);
        let output = keyed_reduce(&data, &[(&"oldestAge".to_string(), &max("age"))]).unwrap();

        assert_eq!(
            json!({
                "\"james\"": {"oldestAge": 40},
                "\"ania\"": {"oldestAge": 21},
                "\"misha\"": {"oldestAge": 9},
            }),
            Json::from(output)
        );
    }

    #[test]
    fn keyed_min_reduce() {
        use serde_json::json;
        let data = data();
        let data = obj(data);
        let output = keyed_reduce(&data, &[(&"youngestAge".to_string(), &min("age"))]).unwrap();

        assert_eq!(
            json!({
                "\"james\"": {"youngestAge": 20},
                "\"ania\"": {"youngestAge": 20},
                "\"misha\"": {"youngestAge": 9},
            }),
            Json::from(output)
        );
    }
}
