use crate::err::Error;

pub use serde_json::{json, Map};

use std::cmp::PartialOrd;
use std::mem;

pub type Json = serde_json::Value;
pub type JsonObj = Map<String, Json>;
pub type JsonNum = serde_json::Number;

pub fn json_obj() -> Json {
    Json::Object(JsonObj::new())
}

pub fn count(val: &Json) -> Result<Json, Error> {
    Ok(json_count(val))
}

pub fn unique(val: &Json) -> Result<Json, Error> {
    Ok(json_unique(val))
}

//TODO make generic comparison
pub fn json_num_gt(x: &JsonNum, y: &JsonNum) -> bool {
    match (x.as_f64(), y.as_f64()) {
        (Some(x), Some(y)) => x > y,
        _ => false,
    }
}

pub fn json_increment(val: &mut Json) {
    match val {
        Json::Number(num) => {
            *num = if num.is_i64() {
                JsonNum::from(num.as_i64().unwrap() + 1)
            } else {
                JsonNum::from_f64(num.as_f64().unwrap() + 1.0).unwrap()
            };
        }
        _ => (),
    }
}

//TODO make generic comparison
pub fn json_num_lt(x: &JsonNum, y: &JsonNum) -> Option<bool> {
    match (x.is_i64(), y.is_i64()) {
        (true, true) => {
            let xv = x.as_i64().unwrap();
            let yv = y.as_i64().unwrap();
            Some(xv < yv)
        }
        _ => None,
    }
}

trait Compare {
    fn gt(&self) -> bool;
    fn gte(&self) -> bool;
    fn lt(&self) -> bool;
    fn lte(&self) -> bool;
}

struct Cmp<T> {
    x: T,
    y: T,
}

impl<T: PartialOrd> Compare for Cmp<T> {
    fn gt(&self) -> bool {
        self.x > self.y
    }

    fn gte(&self) -> bool {
        self.x >= self.y
    }

    fn lt(&self) -> bool {
        self.x < self.y
    }

    fn lte(&self) -> bool {
        self.x <= self.y
    }
}

impl<T: PartialOrd> Cmp<T> {
    fn new(x: T, y: T) -> Cmp<T> {
        Self { x, y }
    }
}

trait GtLt {
    fn apply<T>(&self, x: T, y: T) -> bool
    where
        T: Ord;
}

struct Gt {}

impl GtLt for Gt {
    fn apply<T: Ord>(&self, x: T, y: T) -> bool {
        x > y
    }
}

struct Lt {}

impl GtLt for Lt {
    fn apply<T: Ord>(&self, x: T, y: T) -> bool {
        x < y
    }
}

pub fn json_add_num(x: &Json, y: &JsonNum) -> Option<Json> {
    let x = match x {
        Json::Number(x) => x,
        _ => return None,
    };
    let val = match (x.is_i64(), y.is_i64()) {
        (true, true) => {
            let x: i64 = x.as_i64().unwrap();
            let y: i64 = y.as_i64().unwrap();
            Json::from(x + y)
        }
        _ => {
            let x = x.as_f64().unwrap();
            let y = y.as_f64().unwrap();
            Json::from(x + y)
        }
    };
    Some(val)
}

pub fn json_eq(x: &Json, y: &Json) -> bool {
    x == y
}

pub fn json_neq(x: &Json, y: &Json) -> bool {
    x != y
}

fn json_cmp<'a>(x: &'a Json, y: &'a Json, p: &dyn Fn(&dyn Compare) -> bool) -> bool {
    match (x, y) {
        (Json::String(x), Json::String(y)) => {
            let cmp = Cmp::new(x, y);
            p(&cmp)
        }
        (Json::Number(x), Json::Number(y)) => match (x.is_i64(), y.is_i64()) {
            (true, true) => {
                let cmp = Cmp::new(x.as_i64().unwrap(), y.as_i64().unwrap());
                p(&cmp)
            }
            _ => {
                let cmp = Cmp::new(x.as_f64().unwrap(), y.as_f64().unwrap());
                p(&cmp)
            }
        },
        (Json::Bool(x), Json::Bool(y)) => {
            let cmp = Cmp::new(*x, *y);
            p(&cmp)
        }
        _ => false,
    }
}

pub fn json_gt(x: &Json, y: &Json) -> bool {
    json_cmp(x, y, &|x| x.gt())
}

pub fn json_lt(x: &Json, y: &Json) -> bool {
    json_cmp(x, y, &|x| x.lt())
}

pub fn json_gte(x: &Json, y: &Json) -> bool {
    json_cmp(x, y, &|x| x.gte())
}

pub fn json_lte(x: &Json, y: &Json) -> bool {
    json_cmp(x, y, &|x| x.lte())
}

pub fn json_count(val: &Json) -> Json {
    match val {
        Json::Array(ref arr) => Json::from(arr.len()),
        _ => Json::from(1),
    }
}

pub fn json_append(val: &mut Json, elem: Json) {
    match val {
        Json::Array(ref mut arr) => {
            arr.push(elem);
        }
        Json::Object(ref mut obj) => match elem {
            Json::Object(o) => {
                obj.extend(o);
            }
            _json => {
                let mut elem = Json::Array(Vec::new());
                mem::swap(&mut elem, val);
                json_append(val, elem);
            }
        },
        Json::Null => {
            *val = elem;
        }
        val => {
            let arr = vec![val.clone(), elem];
            *val = Json::from(arr);
        }
    }
}

pub fn insert_rows(val: &mut Json, rows: Vec<Json>) -> Result<usize, Error> {
    match val {
        Json::Array(ref mut arr) => {
            let n = rows.len();
            for row in rows {
                arr.push(row);
            }
            Ok(n)
        }
        _ => Err(Error::BadType),
    }
}

pub fn json_first(val: &Json) -> &Json {
    match val {
        Json::Array(ref arr) if !arr.is_empty() => &arr[0],
        val => val,
    }
}

pub fn json_last(val: &Json) -> &Json {
    match val {
        Json::Array(ref arr) if !arr.is_empty() => &arr[arr.len() - 1],
        val => val,
    }
}

pub fn json_sum(val: &Json) -> Json {
    match val {
        Json::Number(val) => Json::Number(val.clone()),
        Json::Array(ref arr) => json_arr_sum(arr),
        _ => Json::Null,
    }
}

pub fn json_pop(val: &mut Json) -> Result<Option<Json>, Error> {
    match val {
        Json::Array(ref mut arr) => Ok(arr_pop(arr)),
        _ => Err(Error::ExpectedArr),
    }
}

fn arr_pop(arr: &mut Vec<Json>) -> Option<Json> {
    arr.pop()
}

pub fn json_avg(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => json_arr_avg(arr),
        _ => Err(Error::BadType),
    }
}

pub fn json_var(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => json_arr_var(arr),
        _ => Err(Error::BadType),
    }
}

pub fn json_dev(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => json_arr_dev(arr),
        _ => Err(Error::BadType),
    }
}

pub fn json_max(val: &Json) -> &Json {
    match val {
        Json::Array(ref arr) if !arr.is_empty() => arr_max(arr),
        val => val,
    }
}

//TODO(jaupe) add more cases
pub fn json_add(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(lhs), Json::Array(rhs)) => json_add_arrs(lhs, rhs),
        (Json::Array(lhs), Json::Number(rhs)) => json_add_arr_num(lhs, rhs),
        (Json::Number(rhs), Json::Array(lhs)) => json_add_arr_num(lhs, rhs),
        (Json::Number(lhs), Json::Number(rhs)) => Ok(Json::Number(json_add_nums(lhs, rhs))),
        (Json::String(lhs), Json::String(rhs)) => json_add_str(lhs, rhs),
        (Json::String(lhs), Json::Array(rhs)) => add_str_arr(lhs, rhs),
        (Json::Array(lhs), Json::String(rhs)) => add_arr_str(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

pub fn json_sub(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(lhs), Json::Array(rhs)) => json_sub_arrs(lhs, rhs),
        (Json::Array(lhs), Json::Number(rhs)) => json_sub_arr_num(lhs, rhs),
        (Json::Number(lhs), Json::Array(rhs)) => json_sub_num_arr(lhs, rhs),
        (Json::Number(lhs), Json::Number(rhs)) => json_sub_nums(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

pub fn json_mul(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(x), Json::Array(y)) => mul_arrs(x, y),
        (Json::Array(x), Json::Number(y)) => mul_arr_num(x, y),
        (Json::Number(x), Json::Array(y)) => mul_arr_num(y, x),
        (Json::Number(x), Json::Number(y)) => mul_nums(x, y),
        _ => Err(Error::BadType),
    }
}

pub fn json_unique(val: &Json) -> Json {
    match val {
        Json::Array(arr) => arr_unique(arr),
        val => val.clone(),
    }
}

fn arr_unique(arr: &[Json]) -> Json {
    let mut unique: Vec<Json> = Vec::new();
    for val in arr {
        let pos = unique.iter().find(|x| *x == val);
        if pos.is_none() {
            unique.push(val.clone());
        }
    }
    Json::Array(unique)
}

fn mul_vals(x: &Json, y: &Json) -> Result<Json, Error> {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => mul_nums(x, y),
        _ => Err(Error::BadType),
    }
}

fn mul_nums(x: &JsonNum, y: &JsonNum) -> Result<Json, Error> {
    let val = x.as_f64().unwrap() * y.as_f64().unwrap();
    Ok(Json::from(val))
}

fn mul_arr_num(x: &[Json], y: &JsonNum) -> Result<Json, Error> {
    let mut arr = Vec::new();
    for x in x.iter() {
        arr.push(mul_val_num(x, y)?);
    }
    Ok(Json::from(arr))
}

fn mul_val_num(x: &Json, y: &JsonNum) -> Result<Json, Error> {
    match x {
        Json::Number(ref x) => mul_nums(x, y),
        Json::Array(ref arr) => mul_arr_num(arr, y),
        _ => Err(Error::BadType),
    }
}

//TODO(jaupe) optimize by removing the temp allocs
fn mul_arrs(lhs: &[Json], rhs: &[Json]) -> Result<Json, Error> {
    let mut arr: Vec<Json> = Vec::new();
    for (x, y) in lhs.iter().zip(rhs.iter()) {
        arr.push(mul_vals(x, y)?);
    }
    Ok(Json::from(arr))
}

pub fn json_div(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(ref lhs), Json::Array(ref rhs)) => div_arrs(lhs, rhs),
        (Json::Array(ref lhs), Json::Number(ref rhs)) => div_arr_num(lhs, rhs),
        (Json::Number(ref lhs), Json::Array(ref rhs)) => div_num_arr(lhs, rhs),
        (Json::Number(ref lhs), Json::Number(ref rhs)) => div_nums(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

fn div_nums(x: &JsonNum, y: &JsonNum) -> Result<Json, Error> {
    let val = x.as_f64().unwrap() / y.as_f64().unwrap();
    Ok(Json::from(val))
}

fn div_arrs(x: &[Json], y: &[Json]) -> Result<Json, Error> {
    let mut arr = Vec::new();
    for (x, y) in x.iter().zip(y.iter()) {
        arr.push(json_div(x, y)?);
    }
    Ok(Json::from(arr))
}

fn div_arr_num(x: &[Json], y: &JsonNum) -> Result<Json, Error> {
    let mut arr: Vec<Json> = Vec::new();
    for x in x {
        arr.push(div_val_num(x, y)?);
    }
    Ok(Json::from(arr))
}

fn div_val_num(x: &Json, y: &JsonNum) -> Result<Json, Error> {
    match x {
        Json::Number(ref x) => div_nums(x, y),
        Json::Array(ref x) => div_arr_num(x, y),
        _ => Err(Error::BadType),
    }
}

fn div_num_arr(x: &JsonNum, y: &[Json]) -> Result<Json, Error> {
    let mut arr: Vec<Json> = Vec::new();
    for y in y {
        arr.push(div_num_val(x, y)?);
    }
    Ok(Json::from(arr))
}

fn div_num_val(x: &JsonNum, y: &Json) -> Result<Json, Error> {
    match y {
        Json::Number(ref y) => div_nums(x, y),
        Json::Array(ref y) => div_num_arr(x, y),
        _ => Err(Error::BadType),
    }
}

fn json_add_str(x: &str, y: &str) -> Result<Json, Error> {
    let val = x.to_string() + y;
    Ok(Json::String(val))
}

fn add_str_arr(x: &str, y: &[Json]) -> Result<Json, Error> {
    let mut arr = Vec::with_capacity(y.len());
    for e in y {
        arr.push(add_str_val(x, e)?);
    }
    Ok(Json::Array(arr))
}

fn add_str_val(x: &str, y: &Json) -> Result<Json, Error> {
    match y {
        Json::String(y) => Ok(Json::from(x.to_string() + y)),
        _ => Err(Error::BadType),
    }
}

fn add_val_str(x: &Json, y: &str) -> Result<Json, Error> {
    match x {
        Json::String(x) => Ok(Json::from(x.to_string() + y)),
        _ => Err(Error::BadType),
    }
}

fn add_arr_str(lhs: &[Json], rhs: &str) -> Result<Json, Error> {
    let mut arr = Vec::with_capacity(lhs.len());
    for x in lhs {
        arr.push(add_val_str(x, rhs)?);
    }
    Ok(Json::Array(arr))
}

//TODO(jaupe) add better error handlinge
fn json_add_arr_num(x: &[Json], y: &JsonNum) -> Result<Json, Error> {
    let arr: Vec<Json> = x
        .iter()
        .map(|x| Json::from(x.as_f64().unwrap() + y.as_f64().unwrap()))
        .collect();
    Ok(Json::Array(arr))
}

fn json_add_arrs(lhs: &[Json], rhs: &[Json]) -> Result<Json, Error> {
    let vec = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(x, y)| json_add(x, y).unwrap())
        .collect();
    Ok(Json::Array(vec))
}

pub(crate) fn json_add_nums(x: &JsonNum, y: &JsonNum) -> JsonNum {
    match (x.is_i64(), y.is_i64()) {
        (true, true) => JsonNum::from(x.as_i64().unwrap() + y.as_i64().unwrap()),
        _ => JsonNum::from_f64(x.as_f64().unwrap() + y.as_f64().unwrap()).unwrap(),
    }
}

fn json_sub_arr_num(x: &[Json], y: &JsonNum) -> Result<Json, Error> {
    let arr = x
        .iter()
        .map(|x| Json::from(x.as_f64().unwrap() - y.as_f64().unwrap()))
        .collect();
    Ok(Json::Array(arr))
}

fn json_sub_num_arr(x: &JsonNum, y: &[Json]) -> Result<Json, Error> {
    let arr = y
        .iter()
        .map(|y| Json::from(x.as_f64().unwrap() - y.as_f64().unwrap()))
        .collect();
    Ok(Json::Array(arr))
}

fn json_sub_arrs(lhs: &[Json], rhs: &[Json]) -> Result<Json, Error> {
    let vec = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(x, y)| json_sub(x, y).unwrap())
        .collect();
    Ok(Json::Array(vec))
}

fn json_sub_nums(x: &JsonNum, y: &JsonNum) -> Result<Json, Error> {
    let val = x.as_f64().unwrap() - y.as_f64().unwrap();
    Ok(Json::from(val))
}

pub fn json_min(val: &Json) -> &Json {
    match val {
        Json::Array(ref arr) if !arr.is_empty() => arr_min(arr),
        val => val,
    }
}

fn add_nums(x: &JsonNum, y: &JsonNum) -> JsonNum {
    match (x.is_i64(), y.is_i64()) {
        (true, true) => {
            let x = x.as_u64().unwrap();
            let y = y.as_u64().unwrap();
            JsonNum::from(x + y)
        }
        _ => {
            let x = x.as_f64().unwrap();
            let y = y.as_f64().unwrap();
            JsonNum::from_f64(x + y).unwrap()
        }
    }
}

fn add_jsons(lhs: &Option<JsonNum>, rhs: &Json) -> Result<JsonNum, Error> {
    match (lhs, rhs) {
        (None, Json::Number(n)) => Ok(n.clone()),
        (Some(x), Json::Number(y)) => Ok(add_nums(x, y)),
        _ => Err(Error::BadType),
    }
}

fn json_arr_sum(s: &[Json]) -> Json {
    let mut total = JsonNum::from(0);
    for val in s {
        match val {
            Json::Number(num) => {
                total = json_add_nums(&total, num);
            }
            _ => continue,
        }
    }
    Json::Number(total)
}

pub fn json_obj_ref(val: &Json) -> Result<&JsonObj, Error> {
    match val {
        Json::Object(obj) => Ok(obj),
        _ => Err(Error::ExpectedObj),
    }
}

pub fn json_obj_mut_ref(val: &mut Json) -> &mut JsonObj {
    match val {
        Json::Object(obj) => obj,
        _ => panic!(),
    }
}

fn json_arr_avg(s: &[Json]) -> Result<Json, Error> {
    let mut total = 0.0f64;
    for val in s {
        total += json_f64(val).ok_or(Error::BadType)?;
    }
    let val = total / (s.len() as f64);
    let num = JsonNum::from_f64(val).ok_or(Error::BadType)?;
    Ok(Json::Number(num))
}

fn json_arr_var(s: &[Json]) -> Result<Json, Error> {
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
    let num = JsonNum::from_f64(var).ok_or(Error::BadType)?;
    Ok(Json::Number(num))
}

fn json_arr_dev(s: &[Json]) -> Result<Json, Error> {
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
    let num = JsonNum::from_f64(var.sqrt()).ok_or(Error::BadType)?;
    Ok(Json::Number(num))
}

pub fn json_f64(val: &Json) -> Option<f64> {
    match val {
        Json::Number(num) => num.as_f64(),
        _ => None,
    }
}

fn arr_max(s: &[Json]) -> &Json {
    let mut max = &s[0];
    for val in &s[1..] {
        if json_gt(val, max) {
            max = val;
        }
    }
    max
}

fn arr_min(s: &[Json]) -> &Json {
    let mut min = &s[0];
    for val in &s[1..] {
        if json_lt(val, min) {
            min = val;
        }
    }
    min
}

pub fn to_rows(val: Json) -> Result<Vec<Json>, Error> {
    match val {
        Json::Array(arr) => Ok(arr),
        _ => Err(Error::ExpectedArr),
    }
}

/*
pub fn parse_json_str<S: Into<String>>(s: S) -> Res<Cmd> {
    let json_val = serde_json::from_str(&s.into()).map_err(|_| BAD_JSON)?;
    parse_json_val(json_val)
}

fn parse_json_val(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => parse_obj(obj),
        val => Ok(Cmd::Val(val)),
    }
}

fn parse_obj(obj: Map<String, Json>) -> Res<Cmd> {
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
    Ok(Cmd::Set("k1".to_string(), Json::Bool(true)))
}

fn parse_min(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => Ok(Cmd::Min(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Min(Box::new(Cmd::Val(val)))),
    }
}

fn parse_max(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => Ok(Cmd::Max(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Max(Box::new(Cmd::Val(val)))),
    }
}

fn parse_avg(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => Ok(Cmd::Avg(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_var(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => Ok(Cmd::Var(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_dev(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => Ok(Cmd::Dev(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_sum(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => Ok(Cmd::Sum(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_get(val: Json) -> Res<Cmd> {
    match val {
        Json::String(key) => Ok(Cmd::Key(key)),
        _ => Err(Error::BadType),
    }
}

fn parse_del(val: Json) -> Res<Cmd> {
    match val {
        Json::String(key) => Ok(Cmd::Del(key)),
        _ => Err(Error::BadType),
    }
}

fn parse_set(val: Json) -> Res<Cmd> {
    match val {
        Json::Array(mut arr) => {
            let val = arr.remove(1);
            let key = arr.remove(0);
            let key = match key {
                Json::String(key) => key,
                _ => unimplemented!(),
            };
            Ok(Cmd::Set(key, val))
        }
        Json::Object(_obj) => unimplemented!(),
        _ => unimplemented!(),
    }
}

fn parse_first(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => Ok(Cmd::First(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::First(Box::new(Cmd::Val(val)))),
    }
}

fn parse_last(val: Json) -> Res<Cmd> {
    match val {
        Json::Object(obj) => Ok(Cmd::Last(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Last(Box::new(Cmd::Val(val)))),
    }
}

fn parse_add(val: Json) -> Res<Cmd> {
    match val {
        Json::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Add(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_sub(val: Json) -> Res<Cmd> {
    match val {
        Json::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Sub(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_mul(val: Json) -> Res<Cmd> {
    match val {
        Json::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Mul(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_div(val: Json) -> Res<Cmd> {
    match val {
        Json::Array(mut arr) => {
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
        Cmd::Key(ref key) => db.get(key).map(|x| x.clone()).ok_or(BAD_KEY),
        Cmd::Del(ref key) => match db.del(key) {
            Ok(Some(val)) => Ok(val),
            Ok(None) => Ok(Json::Null),
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

fn db_write(db: &mut Database, key: String, val: Json) -> Res {
    match db.set(key, val) {
        Ok(Some(val)) => Ok(val),
        Ok(None) => Ok(Json::Null),
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

pub fn json_get(key: &str, val: &Json) -> Option<Json> {
    match val {
        Json::Array(arr) => {
            if arr.is_empty() {
                return None;
            }
            let mut out = Vec::new();
            for val in arr {
                if let Some(v) = json_get(key, val) {
                    out.push(v);
                }
            }
            if out.is_empty() {
                None
            } else {
                Some(Json::Array(out))
            }
        }
        Json::Object(obj) => {
            if let Some(val) = obj.get(key) {
                let mut obj = JsonObj::new();
                obj.insert(key.to_string(), val.clone());
                Some(Json::Object(obj))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn json_push(to: &mut Json, val: Json) {
    match to {
        Json::Array(ref mut arr) => {
            arr.push(val);
        }
        v => {
            //TODO optimize this to not clone the val
            *v = Json::from(vec![v.clone(), val])
        }
    };
}

pub fn json_mut_append(val: &mut Json, arg: Json) {
    match val {
        Json::Null => *val = arg,
        Json::Array(ref mut arr) => arr.push(arg),
        val => *val = Json::Array(vec![arg]),
    }
}

#[test]
fn append_obj_ok() {
    use serde_json::json;
    let mut obj = json!({"name":"james"});
    let elem = json!({"age": 45});
    json_append(&mut obj, elem);
    assert_eq!(obj, json!({"name": "james", "age": 45}));
}

#[test]
fn json_append_obj() {
    let mut obj = json!({"name":"anna", "age": 28});
    json_append(&mut obj, json!({"email": "anna@gmail.com"}));
    assert_eq!(
        obj,
        json!({"name": "anna", "age": 28, "email": "anna@gmail.com"})
    )
}
