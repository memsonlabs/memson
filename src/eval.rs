use crate::json::Json;

fn get(key: &str, val: &Json) -> Option<Json> {
    match val {
        Json::Array(ref arr) => {
            let mut vals = Vec::new();
            for val in arr {
                if let Some(val) = get(key, val) {
                    vals.push(val);
                }
            }
            if vals.is_empty() {
                None
            } else {
                Some(Json::Array(vals))
            }
        }
        Json::Object(obj) => obj.get(key).cloned(),
        _ => None,
    }
}

pub fn key(keys: &[String], mut val: Option<Json>) -> Option<Json> {
    for key in keys {
        if let Some(ref v) = val {
            val = get(key, v);
        } else {
            return None;
        }
    }
    val
}



mod tests {
    use crate::eval::*;
    use serde_json::json;

    #[test]
    fn nested_obj_key_none() {
        let val = json!({"a": 1,  "b": 2, "c": 3});
        assert_eq!(None, key(&["d".to_string()], Some(val)));
    }

    #[test]
    fn nested_obj_key_some() {
        let val = json!({"a": 1,  "b": 2, "c": 3});
        assert_eq!(Some(Json::from(3)), key(&["c".to_string()], Some(val)));
    }

    #[test]
    fn nested_arr_objs_key_some() {
        let val = json!([
            {"age": 20,  "name": {"first": "james", "last":"perry"}, "job": "looser"},
            {"age": 30,  "name": {"first": "ania", "last":"protsenko"}, "job": "english teacher"},
            {"age": 9,  "name": {"first": "misha"}},
        ]);
        assert_eq!(
            Some(json!([20, 30, 9])),
            key(&["age".to_string()], Some(val.clone()))
        );
        assert_eq!(
            Some(json!(["looser", "english teacher"])),
            key(&["job".to_string()], Some(val.clone()))
        );
        assert_eq!(
            Some(json!(["james", "ania", "misha"])),
            key(&["name".to_string(), "first".to_string()], Some(val.clone()))
        );
        assert_eq!(
            Some(json!(["perry", "protsenko"])),
            key(&["name".to_string(), "last".to_string()], Some(val.clone()))
        );
    }

    #[test]
    fn nested_arr_arrs_key_some() {
        let val = json!([
            {"age": 20,  "name": {"first": "james", "last":"perry"}, "job": "looser"},
            {"age": 30,  "name": {"first": "ania", "last":"protsenko"}, "job": "english teacher"},
            {"age": 9,  "name": {"first": "misha"}},
        ]);
        assert_eq!(
            Some(json!([20, 30, 9])),
            key(&["age".to_string()], Some(val.clone()))
        );
        assert_eq!(
            Some(json!(["looser", "english teacher"])),
            key(&["job".to_string()], Some(val.clone()))
        );
        assert_eq!(
            Some(json!(["james", "ania", "misha"])),
            key(&["name".to_string(), "first".to_string()], Some(val.clone()))
        );
        assert_eq!(
            Some(json!(["perry", "protsenko"])),
            key(&["name".to_string(), "last".to_string()], Some(val.clone()))
        );
    }
}
