use crate::db::{Error, Reply, Res};
use serde_json::Number as JsonNum;
use serde_json::Number;
pub use serde_json::{json, Map, Value as Json};
use std::cmp::PartialOrd;
use std::mem;

//TODO make generic comparison
pub fn json_num_gt(x: &JsonNum, y: &JsonNum) -> Option<bool> {
    match (x.is_i64(), y.is_i64()) {
        (true, true) => {
            let xv = x.as_i64().unwrap();
            let yv = y.as_i64().unwrap();
            Some(xv > yv)
        }
        _ => None,
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

pub fn json_eq(x: &Json, y: &Json) -> Option<bool> {
    Some(x == y)
}

pub fn json_neq(x: &Json, y: &Json) -> Option<bool> {
    Some(x != y)
}

fn json_cmp<'a>(x: &'a Json, y: &'a Json, p: &dyn Fn(&dyn Compare) -> bool) -> Option<bool> {
    match (x, y) {
        (Json::String(x), Json::String(y)) => {
            let cmp = Cmp::new(x, y);
            Some(p(&cmp))
        }
        (Json::Number(x), Json::Number(y)) => {
            let x = match x.as_i64() {
                Some(val) => val,
                None => return None,
            };
            let y = match y.as_i64() {
                Some(val) => val,
                None => return None,
            };
            let cmp = Cmp::new(x, y);
            Some(p(&cmp))
        }
        (Json::Bool(x), Json::Bool(y)) => Some(x > y),
        _ => None,
    }
}

pub fn json_gt(x: &Json, y: &Json) -> Option<bool> {
    json_cmp(x, y, &|x| x.gt())
}

pub fn json_lt(x: &Json, y: &Json) -> Option<bool> {
    json_cmp(x, y, &|x| x.lt())
}

pub fn json_gte(x: &Json, y: &Json) -> Option<bool> {
    json_cmp(x, y, &|x| x.gte())
}

pub fn json_lte(x: &Json, y: &Json) -> Option<bool> {
    json_cmp(x, y, &|x| x.lte())
}

pub fn len(val: &Json) -> Json {
    match val {
        Json::Array(ref arr) => Json::from(arr.len()),
        _ => Json::from(1),
    }
}

pub fn append(val: &mut Json, elem: Json) {
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
                append(val, elem);
            }
        },
        val => {
            let arr = vec![val.clone(), elem];
            *val = Json::from(arr);
        }
    }
}

pub fn first(val: &Json) -> Res<'_> {
    match val {
        Json::Array(ref arr) => arr_first(arr),
        val => Ok(Reply::Ref(val)),
    }
}

pub fn last(val: &Json) -> Res<'_> {
    match val {
        Json::Array(ref arr) => arr_last(arr),
        val => Ok(Reply::Ref(val)),
    }
}

pub fn sum(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => json_arr_sum(arr),
        _ => Err(Error::BadType),
    }
}

pub fn pop(val: &mut Json) -> Res<'_> {
    match val {
        Json::Array(ref mut arr) => arr_pop(arr).map(Reply::Val),
        _ => Err(Error::BadType),
    }
}

fn arr_pop(arr: &mut Vec<Json>) -> Result<Json, Error> {
    match arr.pop() {
        Some(val) => Ok(val),
        None => Err(Error::EmptySequence),
    }
}

pub fn avg(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => json_arr_avg(arr),
        _ => Err(Error::BadType),
    }
}

pub fn var(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => json_arr_var(arr),
        _ => Err(Error::BadType),
    }
}

pub fn dev(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => json_arr_dev(arr),
        _ => Err(Error::BadType),
    }
}

pub fn max(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Array(ref arr) => arr_max(arr),
        val => Ok(val.clone()),
    }
}

//TODO(jaupe) add more cases
pub fn add(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(lhs), Json::Array(rhs)) => json_add_arrs(lhs, rhs),
        (Json::Array(lhs), Json::Number(rhs)) => json_add_arr_num(lhs, rhs),
        (Json::Number(rhs), Json::Array(lhs)) => json_add_arr_num(lhs, rhs),
        (Json::Number(lhs), Json::Number(rhs)) => json_add_nums(lhs, rhs),
        (Json::String(lhs), Json::String(rhs)) => json_add_str(lhs, rhs),
        (Json::String(lhs), Json::Array(rhs)) => add_str_arr(lhs, rhs),
        (Json::Array(lhs), Json::String(rhs)) => add_arr_str(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

pub fn sub(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(lhs), Json::Array(rhs)) => json_sub_arrs(lhs, rhs),
        (Json::Array(lhs), Json::Number(rhs)) => json_sub_arr_num(lhs, rhs),
        (Json::Number(lhs), Json::Array(rhs)) => json_sub_num_arr(lhs, rhs),
        (Json::Number(lhs), Json::Number(rhs)) => json_sub_nums(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

pub fn mul(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(x), Json::Array(y)) => mul_arrs(x, y),
        (Json::Array(x), Json::Number(y)) => mul_arr_num(x, y),
        (Json::Number(x), Json::Array(y)) => mul_arr_num(y, x),
        (Json::Number(x), Json::Number(y)) => mul_nums(x, y),
        _ => Err(Error::BadType),
    }
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

pub fn div(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
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
        arr.push(div(x, y)?);
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
        .map(|(x, y)| add(x, y).unwrap())
        .collect();
    Ok(Json::Array(vec))
}

pub(crate) fn json_add_nums(x: &JsonNum, y: &JsonNum) -> Result<Json, Error> {
    match (x.is_i64(), y.is_i64()) {
        (true, true) => Ok(Json::from(x.as_i64().unwrap() + y.as_i64().unwrap())),
        _ => Ok(Json::from(x.as_f64().unwrap() + y.as_f64().unwrap())),
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
        .map(|(x, y)| sub(x, y).unwrap())
        .collect();
    Ok(Json::Array(vec))
}

fn json_sub_nums(x: &JsonNum, y: &JsonNum) -> Result<Json, Error> {
    let val = x.as_f64().unwrap() - y.as_f64().unwrap();
    Ok(Json::from(val))
}

pub fn min(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => arr_min(arr),
        val => Ok(val.clone()),
    }
}

fn json_arr_sum(s: &[Json]) -> Result<Json, Error> {
    let mut total = 0.0f64;
    for val in s {
        match val {
            Json::Number(num) => {
                total += num.as_f64().unwrap();
            }
            _ => return Err(Error::BadNumber),
        }
    }
    let num = Number::from_f64(total).ok_or(Error::BadNumber)?;
    Ok(Json::Number(num))
}

fn arr_first(s: &[Json]) -> Res<'_> {
    if s.is_empty() {
        Err(Error::EmptySequence)
    } else {
        Ok(Reply::Ref(&s[0]))
    }
}

fn arr_last(s: &[Json]) -> Res<'_> {
    if s.is_empty() {
        Err(Error::EmptySequence)
    } else {
        Ok(Reply::Ref(&s[s.len() - 1]))
    }
}

fn json_arr_avg(s: &[Json]) -> Result<Json, Error> {
    let mut total = 0.0f64;
    for val in s {
        total += json_f64(val).ok_or(Error::BadType)?;
    }
    let val = total / (s.len() as f64);
    let num = Number::from_f64(val).ok_or(Error::BadType)?;
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
    let num = Number::from_f64(var).ok_or(Error::BadType)?;
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
    let num = Number::from_f64(var.sqrt()).ok_or(Error::BadType)?;
    Ok(Json::Number(num))
}

pub fn json_f64(val: &Json) -> Option<f64> {
    match val {
        Json::Number(num) => num.as_f64(),
        _ => None,
    }
}

fn arr_max(s: &[Json]) -> Result<Json, Error> {
    if s.is_empty() {
        return Ok(Json::Null);
    }
    let mut max = match json_f64(&s[0]) {
        Some(val) => val,
        None => return Err(Error::BadType),
    };
    for val in s.iter().skip(1) {
        match val {
            Json::Number(num) => {
                let v = num.as_f64().unwrap();
                if v > max {
                    max = v;
                }
            }
            _ => return Err(Error::BadType),
        }
    }
    Ok(Json::from(max))
}

fn arr_min(s: &[Json]) -> Result<Json, Error> {
    if s.is_empty() {
        return Err(Error::EmptySequence);
    }
    let mut min = match json_f64(&s[0]) {
        Some(val) => val,
        None => return Err(Error::BadType),
    };
    for val in s.iter().skip(1) {
        match val {
            Json::Number(num) => {
                let v = num.as_f64().unwrap();
                if v < min {
                    min = v;
                }
            }
            _ => return Err(Error::BadType),
        }
    }
    Ok(Json::from(min))
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
        Json::String(key) => Ok(Cmd::Get(key)),
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
        Cmd::Get(ref key) => db.get(key).map(|x| x.clone()).ok_or(BAD_KEY),
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

pub fn json_push(to: &mut Json, val: Json) {
    match to {
        Json::Array(ref mut arr) => {
            arr.push(val);
        }
        val => {
            //TODO optimize this to not clone the val
            *val = Json::from(vec![val.clone()])
        }
    };
}

pub fn json_to_string(val: &Json) -> Option<String> {
    match val {
        Json::String(s) => Some(s.to_string()),
        Json::Array(_) | Json::Object(_) | Json::Null => None,
        Json::Number(n) => Some(n.to_string()),
        Json::Bool(b) => Some(b.to_string()),
    }
}

#[test]
fn append_obj_ok() {
    use serde_json::json;
    let mut obj = json!({"name":"james"});
    let elem = json!({"age": 45});
    append(&mut obj, elem);
    assert_eq!(obj, json!({"name": "james", "age": 45}));
}

#[test]
fn json_str_to_string() {
    use serde_json::json;
    let val = json!("abc");

    assert_eq!(json_to_string(&val), Some(String::from("abc")));
}
