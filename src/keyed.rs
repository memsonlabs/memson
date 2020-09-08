use crate::cmd::Cmd;
use crate::err::Error;
use crate::json::{json_count, json_get, json_sum, JsonObj};
use crate::json::{json_first, json_last, json_max, json_min, Json};

pub fn eval_keyed_cmd(cmd: &Cmd, val: &Json) -> Result<Json, Error> {
    match cmd {
        Cmd::Count(arg) => {
            let val = eval_keyed_cmd(arg.as_ref(), val)?;
            Ok(json_count(&val))
        }
        Cmd::Key(ref key) => json_get(key, val).ok_or(
            Error::BadKey),
        Cmd::Sum(arg) => {
            let val = eval_keyed_cmd(arg.as_ref(), val)?;
            Ok(json_sum(&val))
        }
        Cmd::Min(arg) => {
            let val = eval_keyed_cmd(arg.as_ref(), val)?;
            Ok(json_min(&val).clone())
        }
        Cmd::Max(arg) => {
            let val = eval_keyed_cmd(arg.as_ref(), val)?;
            Ok(json_max(&val).clone())
        }
        Cmd::First(arg) => {
            let val = eval_keyed_cmd(arg.as_ref(), val)?;
            Ok(json_first(&val).clone())
        }
        Cmd::Last(arg) => {
            let val = eval_keyed_cmd(arg.as_ref(), val)?;
            Ok(json_last(&val).clone())
        }
        _ => unimplemented!(),
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

mod tests {
    use super::*;
    use serde_json::json;

    fn count(key: &str) -> Cmd {
        Cmd::Count(Box::new(Cmd::Key(key.to_string())))
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
    fn keyed_count_aggregate() {
        use serde_json::json;
        let data = json!({
            "\"james\"": [{"age": 20},{"age": 21},{"age": 22},{"age": 35}],
            "\"ania\"": [{"age": 20},{"age": 21}],
            "\"misha\"": [{"age": 9}]
        });
        let data = obj(data);
        let name = "total".to_string();
        let output = keyed_reduce(&data, &[(&name, &count("age"))]).unwrap();

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
