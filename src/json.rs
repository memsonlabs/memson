

use serde_json::Number as JsonNum;
use serde_json::Number;
pub use serde_json::{Map, Value as JsonVal};
use crate::{Res, Reply, Error};

/*
const BAD_TYPE: &str = "bad type";

const BAD_WRITE: &str = "bad write";

const BAD_KEY: &str = "bad key";

const BAD_IO: &str = "bad io";

const BAD_JSON: &str = "bad json";

const Error::BadType: &str = "bad number";
*/

pub fn first(val: &JsonVal) -> Option<&JsonVal> {
    match val {
        JsonVal::Null => None,
        JsonVal::Array(ref arr) => arr_first(arr),        
        val => Some(val),
    }
}

pub fn last(val: &JsonVal) -> Option<&JsonVal> {
    match val {
        JsonVal::Array(ref arr) => arr_last(arr),
        val => Some(val),
    }
}

pub fn sum(val: &JsonVal) -> Result<JsonVal, Error> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_sum(arr),
        val => Err(Error::BadType),
    }
}

pub fn json_avg(val: &JsonVal) -> Result<JsonVal, Error> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_avg(arr),
        val => Err(Error::BadType),
    }
}

pub fn json_var(val: &JsonVal) -> Result<JsonVal, Error> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_var(arr),
        val => Err(Error::BadType),
    }
}

pub fn json_dev(val: &JsonVal) -> Result<JsonVal, Error> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_dev(arr),
        val => Err(Error::BadType),
    }
}

pub fn json_max(val: &JsonVal) -> Result<JsonVal, Error> {
    match val {
        JsonVal::Array(ref arr) => arr_max(arr),        
        val => Ok(val.clone()),
    }
}

//TODO(jaupe) add more cases
fn json_add<'a>(lhs: &'a JsonVal, rhs: &'a JsonVal) -> Result<JsonVal, Error> {
    match (lhs, rhs) {
        (JsonVal::Array(lhs), JsonVal::Array(rhs)) => json_add_arrs(lhs, rhs),
        (JsonVal::Array(lhs), JsonVal::Number(rhs)) => json_add_arr_num(lhs, rhs),
        (JsonVal::Number(rhs), JsonVal::Array(lhs)) => json_add_arr_num(lhs, rhs),
        (JsonVal::Number(lhs), JsonVal::Number(rhs)) => json_add_nums(lhs, rhs),
        (JsonVal::String(lhs), JsonVal::String(rhs)) => json_add_str(lhs, rhs),    
        (JsonVal::String(lhs), JsonVal::Array(rhs)) => add_str_arr(lhs, rhs),
        (JsonVal::Array(lhs), JsonVal::String(rhs)) => add_arr_str(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

fn json_sub<'a>(lhs: &'a JsonVal, rhs: &'a JsonVal) -> Result<JsonVal, Error> {
    match (lhs, rhs) {
        (JsonVal::Array(lhs), JsonVal::Array(rhs)) => json_sub_arrs(lhs, rhs),
        (JsonVal::Array(lhs), JsonVal::Number(rhs)) => json_sub_arr_num(lhs, rhs),
        (JsonVal::Number(lhs), JsonVal::Array(rhs)) => json_sub_num_arr(lhs, rhs),
        (JsonVal::Number(lhs), JsonVal::Number(rhs)) => json_sub_nums(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

fn json_mul<'a>(lhs: &'a JsonVal, rhs: &'a JsonVal) -> Result<JsonVal, Error> {
    match (lhs, rhs) {
        (JsonVal::Array(x), JsonVal::Array(y)) => mul_arrs(x, y),
        (JsonVal::Array(x), JsonVal::Number(y)) => mul_arr_num(x, y),
        (JsonVal::Number(x), JsonVal::Array(y)) => mul_arr_num(y, x),
        (JsonVal::Number(x), JsonVal::Number(y)) => mul_nums(x, y),
        _ => Err(Error::BadType),
    }
}

fn mul_vals<'a>(x: &'a JsonVal, y: &'a JsonVal) -> Result<JsonVal, Error> {
    match (x, y) {
        (JsonVal::Number(x), JsonVal::Number(y)) => mul_nums(x, y),
        _ => Err(Error::BadType),
    }
}

fn mul_nums<'a>(x: &'a JsonNum, y: &'a JsonNum) -> Result<JsonVal, Error> {
    let val = x.as_f64().unwrap() * y.as_f64().unwrap();
    Ok(JsonVal::from(val))
}

fn mul_arr_num<'a>(x: &'a[JsonVal], y: &'a JsonNum) -> Result<JsonVal, Error> {
    let mut arr = Vec::new();
    for x in x.iter() {
        arr.push(mul_val_num(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn mul_val_num<'a>(x: &'a JsonVal, y: &'a JsonNum) -> Result<JsonVal, Error> {
    match x {
        JsonVal::Number(ref x) => mul_nums(x, y),
        JsonVal::Array(ref arr) => mul_arr_num(arr, y),
        _ => Err(Error::BadType),
    }
}

//TODO(jaupe) optimize by removing the temp allocs
fn mul_arrs<'a>(lhs: &'a [JsonVal], rhs: &'a [JsonVal]) -> Result<JsonVal, Error> {
    let mut arr: Vec<JsonVal> = Vec::new();
    for (x, y) in lhs.iter().zip(rhs.iter()) {
        arr.push(mul_vals(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn json_div<'a>(lhs: &'a JsonVal, rhs: &'a JsonVal) -> Result<JsonVal, Error> {
    println!("{:?}, {:?}", lhs, rhs);
    match (lhs, rhs) {
        (JsonVal::Array(ref lhs), JsonVal::Array(ref rhs)) => div_arrs(lhs, rhs),
        (JsonVal::Array(ref lhs), JsonVal::Number(ref rhs)) => div_arr_num(lhs, rhs),
        (JsonVal::Number(ref lhs), JsonVal::Array(ref rhs)) => div_num_arr(lhs, rhs),
        (JsonVal::Number(ref lhs), JsonVal::Number(ref rhs)) => div_nums(lhs, rhs),
        _ => return Err(Error::BadType),
    }
}

fn div_nums<'a>(x: &'a JsonNum, y: &'a JsonNum) -> Result<JsonVal, Error> {
    let val = x.as_f64().unwrap() / y.as_f64().unwrap();
    Ok(JsonVal::from(val))
}

fn div_arrs<'a>(x: &'a [JsonVal], y: &'a [JsonVal]) -> Result<JsonVal, Error> {
    let mut arr = Vec::new();
    for (x, y) in x.iter().zip(y.iter()) {
        arr.push(json_div(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn div_arr_num<'a>(x: &'a [JsonVal], y: &'a JsonNum) -> Result<JsonVal, Error> {
    let mut arr: Vec<JsonVal> = Vec::new();
    for x in x {
        arr.push(div_val_num(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn div_val_num<'a>(x: &'a JsonVal, y: &'a JsonNum) -> Result<JsonVal, Error> {
    match x {
        JsonVal::Number(ref x) => div_nums(x, y),
        JsonVal::Array(ref x) => div_arr_num(x, y),
        _ => Err(Error::BadType),
    }
}

fn div_num_arr<'a>(x: &'a JsonNum, y: &'a [JsonVal]) -> Result<JsonVal, Error> {
    let mut arr: Vec<JsonVal> = Vec::new();
    for y in y {
        arr.push(div_num_val(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn div_num_val<'a>(x: &'a JsonNum, y: &'a JsonVal) -> Result<JsonVal, Error> {
    match y {
        JsonVal::Number(ref y) => div_nums(x, y),
        JsonVal::Array(ref y) => div_num_arr(x, y),
        _ => Err(Error::BadType),
    }
}

fn json_add_str<'a>(x: &'a str, y: &'a str) -> Result<JsonVal, Error> {
    let val = x.to_string() + y;
    Ok(JsonVal::String(val))
}

fn add_str_arr<'a> (x: &'a str, y: &'a [JsonVal]) -> Result<JsonVal, Error> {
    let mut arr = Vec::with_capacity(y.len());
    for e in y {
        arr.push(add_str_val(x, e)?);
    }
    Ok(JsonVal::Array(arr))
}

fn add_str_val<'a>(x: &'a str, y: &'a JsonVal) -> Result<JsonVal, Error> {
    match y {
        JsonVal::String(y) => Ok(JsonVal::from(x.to_string() + y)),
        _ => Err(Error::BadType),
    }
}

fn add_val_str<'a>(x: &'a JsonVal, y: &'a str) -> Result<JsonVal, Error> {
    match x {
        JsonVal::String(x) => Ok(JsonVal::from(x.to_string() + y)),
        _ => Err(Error::BadType),
    }
}


fn add_arr_str<'a>(lhs: &'a [JsonVal], rhs: &'a str) -> Result<JsonVal, Error> {
    let mut arr = Vec::with_capacity(lhs.len());
    for x in lhs {
        arr.push(add_val_str(x, rhs)?);
    }
    Ok(JsonVal::Array(arr))
}

//TODO(jaupe) add better error handlinge
fn json_add_arr_num<'a>(x: &'a [JsonVal], y: &'a JsonNum) -> Result<JsonVal, Error>{
    let arr: Vec<JsonVal> = x
        .iter()
        .map(|x| JsonVal::from(x.as_f64().unwrap() + y.as_f64().unwrap()))
        .collect();
    Ok(JsonVal::Array(arr))
}

fn json_add_arrs<'a>(lhs: &'a [JsonVal], rhs: &'a [JsonVal]) -> Result<JsonVal, Error>{
    let vec = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(x, y)| json_add(x, y).unwrap())
        .collect();
    Ok(JsonVal::Array(vec))
}

fn json_add_nums<'a>(x: &'a JsonNum, y: &'a JsonNum) -> Result<JsonVal, Error>{
    let val = x.as_f64().unwrap() + y.as_f64().unwrap();
    Ok(JsonVal::from(val))
}

fn json_sub_arr_num<'a>(x: &'a [JsonVal], y: &'a JsonNum) -> Result<JsonVal, Error>{
    let arr = x
        .iter()
        .map(|x| JsonVal::from(x.as_f64().unwrap() - y.as_f64().unwrap()))
        .collect();
    Ok(JsonVal::Array(arr))
}

fn json_sub_num_arr<'a>(x: &'a JsonNum, y: &'a [JsonVal]) -> Result<JsonVal, Error>{
    let arr = y
        .iter()
        .map(|y| JsonVal::from(x.as_f64().unwrap() - y.as_f64().unwrap()))
        .collect();
    Ok(JsonVal::Array(arr))
}

fn json_sub_arrs<'a>(lhs: &'a [JsonVal], rhs: &'a [JsonVal]) -> Result<JsonVal, Error>{
    let vec = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(x, y)| json_sub(x, y).unwrap())
        .collect();
    Ok(JsonVal::Array(vec))
}

fn json_sub_nums<'a>(x: &'a JsonNum, y: &'a JsonNum) -> Result<JsonVal, Error>{
    let val = x.as_f64().unwrap() - y.as_f64().unwrap();
    Ok(JsonVal::from(val))
}

pub fn json_min<'a>(val: &'a JsonVal) -> Result<JsonVal, Error>{
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => arr_min(arr),
        val => Ok(val.clone()),
    }
}

fn json_arr_sum<'a>(s: &'a [JsonVal]) -> Result<JsonVal, Error>{
    let mut total = 0.0f64;
    for val in s {
        match val {
            JsonVal::Number(num) => {
                total += num.as_f64().unwrap();
            }
            _ => return Err(Error::BadNumber),
        }
    }
    let num = Number::from_f64(total).ok_or(Error::BadNumber)?;
    Ok(JsonVal::Number(num))
}

fn arr_first<'a>(s: &'a [JsonVal]) -> Option<&'a JsonVal> {
    if s.is_empty() {
        None
    } else {
        Some(&s[0])
    }
}

fn arr_last<'a>(s: &'a [JsonVal]) -> Option<&JsonVal> {
    if s.is_empty() {
        None
    } else {
        Some(&s[s.len() - 1])
    }
}

fn json_arr_avg(s: &[JsonVal]) -> Result<JsonVal, Error> {
    let mut total = 0.0f64;
    for val in s {
        total += json_f64(val).ok_or(Error::BadType)?;
    }
    let val = total / (s.len() as f64);
    let num = Number::from_f64(val).ok_or(Error::BadType)?;
    Ok(JsonVal::Number(num))
}

fn json_arr_var(s: &[JsonVal]) -> Result<JsonVal, Error> {
    let mut sum = 0.0f64;
    for val in s {
        sum += json_f64(val).ok_or(Error::BadType)?;
    }
    let mean = sum / ((s.len() - 1) as f64);
    let mut var = 0.0f64;
    for val in s {
        var += (json_f64(val).ok_or(Error::BadType)? - mean).powf(2.0);
    }
    var /= (s.len()) as f64;
    let num = Number::from_f64(var).ok_or(Error::BadType)?;
    Ok(JsonVal::Number(num))
}

fn json_arr_dev(s: &[JsonVal]) -> Result<JsonVal, Error> {
    let mut sum = 0.0f64;
    for val in s {
        sum += json_f64(val).ok_or(Error::BadType)?;
    }
    let avg = sum / (s.len() as f64);
    let mut var = 0.0f64;
    for val in s {
        var += (json_f64(val).ok_or(Error::BadType)? - avg).powf(2.0);
    }
    var /= s.len() as f64;
    let num = Number::from_f64(var.sqrt()).ok_or(Error::BadType)?;
    Ok(JsonVal::Number(num))
}

fn json_f64(val: &JsonVal) -> Option<f64> {
    match val {
        JsonVal::Number(num) => num.as_f64(),
        _ => None,
    }
}

fn arr_max<'a>(s: &[JsonVal]) -> Result<JsonVal, Error> {
    if s.is_empty() {
        return Ok(JsonVal::Null);
    }
    let mut max = match json_f64(&s[0]) {
        Some(val) => val,
        None => return Err(Error::BadType),
    };
    for val in s.iter().skip(1) {
        match val {
            JsonVal::Number(num) => {
                let v = num.as_f64().unwrap();
                if v > max {
                    max = v;
                }
            }
            _ => return Err(Error::BadType),
        }
    }
    Ok(JsonVal::from(max))
}

fn arr_min<'a>(s: &[JsonVal]) -> Result<JsonVal, Error> {
    if s.is_empty() {
        return Err(Error::EmptySequence);
    }
    let mut min = match json_f64(&s[0]) {
        Some(val) => val,
        None => return Err(Error::BadType),
    };
    for val in s.iter().skip(1) {
        match val {
            JsonVal::Number(num) => {
                let v = num.as_f64().unwrap();
                if v < min {
                    min = v;
                }
            }
            _ => return Err(Error::BadType),
        }
    }
    Ok(JsonVal::from(min))
}

/*
pub fn parse_json_str<S: Into<String>>(s: S) -> Res<Cmd> {
    let json_val = serde_json::from_str(&s.into()).map_err(|_| BAD_JSON)?;
    parse_json_val(json_val)
}

fn parse_json_val(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => parse_obj(obj),
        val => Ok(Cmd::Val(val)),
    }
}

fn parse_obj(obj: Map<String, JsonVal>) -> Res<Cmd> {
    if obj.len() != 1 {
        return Err("not one key");
    }
    for (key, val) in obj {
        match key.as_ref() {
            "get" => return parse_get(val),
            "del" => return parse_del(val),
            "set" => return parse_set(val),
            "min" => return parse_min(val),
            "max" => return parse_max(val),
            "sum" => return parse_sum(val),
            "avg" => return parse_avg(val),
            "var" => return parse_var(val),
            "dev" => return parse_dev(val),
            "first" => return parse_first(val),
            "last" => return parse_last(val),
            "+" => return parse_add(val),
            "-" => return parse_sub(val),
            "*" => return parse_mul(val),
            "/" => return parse_div(val),
            _ => unimplemented!(),
        }
    }
    Ok(Cmd::Set("k1".to_string(), JsonVal::Bool(true)))
}

fn parse_min(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Min(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Min(Box::new(Cmd::Val(val)))),
    }
}

fn parse_max(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Max(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Max(Box::new(Cmd::Val(val)))),
    }
}

fn parse_avg(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Avg(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_var(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Var(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_dev(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Dev(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_sum(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Sum(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_get(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Get(key)),
        _ => Err(Error::BadType),
    }
}

fn parse_del(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Del(key)),
        _ => Err(Error::BadType),
    }
}

fn parse_set(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let val = arr.remove(1);
            let key = arr.remove(0);
            let key = match key {
                JsonVal::String(key) => key,
                _ => unimplemented!(),
            };
            Ok(Cmd::Set(key, val))
        }
        JsonVal::Object(_obj) => unimplemented!(),
        _ => unimplemented!(),
    }
}

fn parse_first(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::First(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::First(Box::new(Cmd::Val(val)))),
    }
}

fn parse_last(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Last(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Last(Box::new(Cmd::Val(val)))),
    }
}

fn parse_add(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Add(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_sub(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Sub(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_mul(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Mul(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_div(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Div(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}
*/
/*
pub fn eval_json_cmd(cmd: Cmd, db: &mut Database) -> Res {
    match cmd {
        Cmd::Get(ref key) => db.get(key).map(|x| x.clone()).ok_or(BAD_KEY),
        Cmd::Del(ref key) => match db.del(key) {
            Ok(Some(val)) => Ok(val),
            Ok(None) => Ok(JsonVal::Null),
            Err(_) => Err(BAD_IO),
        },
        Cmd::Set(key, val) => db_write(db, key, val),
        Cmd::Sum(arg) => eval_sum(*arg, db),
        Cmd::Min(arg) => eval_min(*arg, db),
        Cmd::Max(arg) => eval_max(*arg, db),
        Cmd::Val(val) => Ok(val),
        Cmd::Avg(arg) => eval_avg(*arg, db),
        Cmd::Dev(arg) => eval_dev(*arg, db),
        Cmd::Var(arg) => eval_var(*arg, db),
        Cmd::First(arg) => eval_first(*arg, db),
        Cmd::Last(arg) => eval_last(*arg, db),
        Cmd::Add(lhs, rhs) => eval_add(*lhs, *rhs, db),
        Cmd::Sub(lhs, rhs) => eval_sub(*lhs, *rhs, db),
        Cmd::Mul(lhs, rhs) => eval_mul(*lhs, *rhs, db),
        Cmd::Div(lhs, rhs) => eval_div(*lhs, *rhs, db),
    }
}

fn db_write(db: &mut Database, key: String, val: JsonVal) -> Res {
    match db.set(key, val) {
        Ok(Some(val)) => Ok(val),
        Ok(None) => Ok(JsonVal::Null),
        Err(_) => Err(BAD_WRITE),
    }
}

fn eval_sum(arg: Cmd, db: &mut Database) -> Res {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_sum(val),
        Err(err) => Err(err),
    }
}

fn eval_avg(arg: Cmd, db: &mut Database) -> Res {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_avg(val),
        Err(err) => Err(err),
    }
}

fn eval_dev(arg: Cmd, db: &mut Database) -> Res {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_dev(val),
        Err(err) => Err(err),
    }
}

fn eval_var(arg: Cmd, db: &mut Database) -> Res {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_var(val),
        Err(err) => Err(err),
    }
}

fn eval_first(arg: Cmd, db: &mut Database) -> Res {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_first(val),
        Err(err) => Err(err),
    }
}

fn eval_add(lhs: Cmd, rhs: Cmd, db: &mut Database) -> Res {
    let x = eval_json_cmd(lhs, db)?;
    let y = eval_json_cmd(rhs, db)?;
    json_add(&x, &y)
}

fn eval_sub(lhs: Cmd, rhs: Cmd, db: &mut Database) -> Res {
    let x = eval_json_cmd(lhs, db)?;
    let y = eval_json_cmd(rhs, db)?;
    json_sub(&x, &y)
}

fn eval_mul(lhs: Cmd, rhs: Cmd, db: &mut Database) -> Res {
    let x = eval_json_cmd(lhs, db)?;
    let y = eval_json_cmd(rhs, db)?;
    json_mul(&x, &y)
}

fn eval_div(lhs: Cmd, rhs: Cmd, db: &mut Database) -> Res {
    let x = eval_json_cmd(lhs, db)?;
    let y = eval_json_cmd(rhs, db)?;
    json_div(&x, &y)
}

fn eval_last(arg: Cmd, db: &mut Database) -> Res {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_last(val),
        Err(err) => Err(err),
    }
}

fn eval_max(arg: Cmd, db: &mut Database) -> Res {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_max(val),
        Err(err) => Err(err),
    }
}

fn eval_min(arg: Cmd, db: &mut Database) -> Res {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_min(val),
        Err(err) => Err(err),
    }
}

*/