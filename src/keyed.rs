use crate::cmd::Cmd;
use crate::err::Error;
use crate::json::{json_count, json_max, json_obj, json_obj_mut_ref, JsonObj, json_sum, json_gt, json_lt};
use crate::json::{json_min, Json};

pub trait Reduce {
    fn reduce(&self, input: &Json, output: &mut JsonObj);
    fn name(&self) -> &str;
    fn key(&self) -> &str;
}

fn entry<K: Into<String>>(out: &mut JsonObj, key: K) -> &mut Json {
    out.entry(key.into()).or_insert_with(json_obj)
}

fn keyed_key<'a>(key: &str, val: &'a Json) -> Option<&'a Json> {
    match val {
        Json::Object(obj) => obj.get(key),
        _ => None,
    }
}

fn keyed_max<'a>(key: &str, val: &'a Json) -> Option<&'a Json> {
    match val {
        Json::Array(ref arr) => {
            let mut max_val: Option<&Json> = None;
            for x in arr {
                let x = match keyed_key(key, x) {
                    Some(x) => x,
                    None => continue,
                };

                if let Some(y) = max_val {
                    println!("x={:?}>y={:?}", x, y);
                    if json_gt(x, y) {
                        max_val = Some(x);
                    }
                } else {
                    max_val = Some(x)
                }
            }
            max_val
        }
        Json::Object(ref obj) => {
            obj.get(key)
        }
        _ => None,
    }
}


fn keyed_min<'a>(key: &str, val: &'a Json) -> Option<&'a Json> {
    match val {
        Json::Array(ref arr) => {
            let mut max_val: Option<&Json> = None;
            for x in arr {
                let x = match keyed_key(key, x) {
                    Some(x) => x,
                    None => continue,
                };

                if let Some(y) = max_val {
                    println!("x={:?}<y={:?}", x, y);
                    if json_lt(x, y) {
                        max_val = Some(x);
                    }
                } else {
                    max_val = Some(x)
                }
            }
            max_val
        }
        Json::Object(ref obj) => {
            obj.get(key)
        }
        _ => None,
    }
}

fn reduce(select: (&String,&Cmd), val: &Json, obj: &mut JsonObj)  {
    match select.1 {
        Cmd::Sum(arg) => {
            match arg.as_ref() {
                Cmd::Key(key) => {
                    obj.insert(select.0.clone(), json_sum(val));
                }
                _ => unimplemented!(),
            }
        },
        Cmd::Count(arg) => {
            match arg.as_ref() {
                Cmd::Key(key) => {
                    obj.insert(select.0.clone(), json_count(val));
                }
                _ => unimplemented!(),
            }
        }
        Cmd::Max(arg) => {
            match arg.as_ref() {
                Cmd::Key(key) => {
                    if let Some(val) = keyed_max(key, val) {
                        obj.insert(select.0.clone(), val.clone());
                    }
                }
                _ => unimplemented!(),
            }
        }
        Cmd::Min(arg) => {
            match arg.as_ref() {
                Cmd::Key(key) => {
                    if let Some(val) = keyed_min(key, val) {
                        obj.insert(select.0.clone(), val.clone());
                    }
                }
                _ => unimplemented!(),
            }
        }
        _ => unimplemented!()
    }
}

pub fn keyed_reduce(input: &JsonObj, reductions: &[(&String,&Cmd)]) -> Result<JsonObj, Error> {
    let mut output = JsonObj::new();
    for (key, val) in input {
        let entry = output.entry(key.to_string()).or_insert_with(json_obj);
        let obj = json_obj_mut_ref(entry);
        for select in reductions {
            reduce(*select, val, obj);
        }
    }
    Ok(output)
}

#[derive(Debug)]
struct Count {
    name: String,
    key: String,
}

impl Count {
    fn new<S: Into<String>>(name: S, key: S) -> Self {
        Self {
            name: name.into(),
            key: key.into(),
        }
    }
}

impl Reduce for Count {
    fn reduce(&self, input: &Json, output: &mut JsonObj) {
        output.insert(self.name.clone(), json_count(input));
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn key(&self) -> &str {
        &self.key
    }
}

#[derive(Debug)]
struct Max {
    name: String,
    key: String,
}

impl Max {
    fn new<S: Into<String>>(name: S, key: S) -> Self {
        Max {
            name: name.into(),
            key: key.into(),
        }
    }
}

impl Reduce for Max {
    fn reduce(&self, input: &Json, output: &mut JsonObj) {
        output.insert(self.name.clone(), json_max(input).clone());
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn key(&self) -> &str {
        &self.key
    }
}

fn obj(val: Json) -> JsonObj {
    match val {
        Json::Object(obj) => obj,
        _ => panic!(),
    }
}

pub fn parse_reduce(col_name: &str, cmd: &Cmd) -> Result<Option<Box<dyn Reduce>>, Error> {
    match cmd {
        Cmd::Count(arg) => match **arg {
            Cmd::Key(ref key) => Ok(Some(Box::new(Count::new(col_name, key)))),
            _ => unimplemented!(),
        },
        Cmd::Max(arg) => match **arg {
            Cmd::Key(ref key) => Ok(Some(Box::new(Max::new(col_name, &key)))),
            _ => unimplemented!(),
        },
        Cmd::Min(arg) => match **arg {
            Cmd::Key(ref key) => Ok(Some(Box::new(Min::new(col_name, key)))),
            _ => unimplemented!(),
        },
        Cmd::Key(_) => Ok(None),
        _ => Ok(None),
    }
}

#[derive(Debug)]
struct Min {
    name: String,
    key: String,
}

impl Min {
    fn new<S: Into<String>>(name: S, key: S) -> Self {
        Min {
            name: name.into(),
            key: key.into(),
        }
    }
}

impl Reduce for Min {
    fn reduce(&self, input: &Json, output: &mut JsonObj) {
        output.insert(self.name.clone(), json_min(input).clone());
    }

    fn name(&self) -> &str {
        unimplemented!()
    }

    fn key(&self) -> &str {
        unimplemented!()
    }
}

mod tests {
    use super::*;

    fn count(key: &str) -> Cmd {
        Cmd::Count(Box::new(Cmd::Key(key.to_string())))
    }


    fn max(key: &str) -> Cmd {
        Cmd::Max(Box::new(Cmd::Key(key.to_string())))
    }

    fn min(key: &str) -> Cmd {
        Cmd::Min(Box::new(Cmd::Key(key.to_string())))
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
        let data = json!({
        "\"james\"": [{"age": 20},{"age": 40},{"age": 22},{"age": 35}],
        "\"ania\"": [{"age": 21},{"age": 20}],
        "\"misha\"": [{"age": 9}]
    });
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
        let data = json!({
        "\"james\"": [{"age": 20},{"age": 40},{"age": 22},{"age": 35}],
        "\"ania\"": [{"age": 21},{"age": 20}],
        "\"misha\"": [{"age": 9}]
    });
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
