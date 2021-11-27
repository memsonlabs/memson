use crate::cmd::Range;
use crate::err::Error;
use rayon::prelude::*;
use serde_json::Number;
pub use serde_json::{json, Map};
use std::cmp::Ordering;
use std::mem;

pub type Json = serde_json::Value;
pub type JsonObj = Map<String, Json>;
pub type JsonNum = serde_json::Number;

// wrapper around json_count to return as a Result
pub fn count(val: &Json) -> Result<Json, Error> {
    Ok(json_count(val))
}

// wrapper around json_unqiue to return as Result
pub fn unique(val: &Json) -> Result<Json, Error> {
    Ok(json_unique(val))
}

pub fn json_upsert(lhs: &mut Json, rhs: Json) {
    if let Json::Array(ref mut arr) = lhs {
        arr.push(rhs);
    } else {
        *lhs = json!([lhs.clone(), rhs]);
    }
}

/// Vectorized equality test between two json values. Returns back a json value of a boolean or an array of booleans depending
/// if any of the arguments is an array.
///
pub fn json_eq(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Array(x), Json::Array(y)) => {
            let val: Vec<Json> = x
                .par_iter()
                .zip(y.par_iter())
                .map(|(x, y)| Json::from(x == y))
                .collect();
            Json::Array(val)
        }
        (Json::Array(x), val) | (val, Json::Array(x)) => {
            Json::Array(x.par_iter().map(|x| Json::from(x == val)).collect())
        }
        (x, y) => Json::from(x == y),
    }
}

/// Equality test between two json values. Returns back a json value of a boolean or an array of booleans depending
/// if any of the arguments is an array.
///
pub fn json_not_eq(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(
            x.par_iter()
                .zip(y.par_iter())
                .map(|(x, y)| Json::from(x != y))
                .collect(),
        ),
        (Json::Array(x), val) | (val, Json::Array(x)) => {
            Json::Array(x.par_iter().map(|x| Json::from(x != val)).collect())
        }
        (x, y) => Json::from(x != y),
    }
}

/// Vectorized Greater than test between two json values.
/// Returns back a json value of a boolean or an array of booleans.
///
pub fn json_gt(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(
            x.par_iter()
                .zip(y.par_iter())
                .map(|(x, y)| gt(x, y))
                .map(Json::from)
                .collect(),
        ),
        (Json::Array(x), val) | (val, Json::Array(x)) => {
            Json::Array(x.par_iter().map(|x| gt(x, val)).map(Json::from).collect())
        }
        (x, y) => Json::from(gt(x, y)),
    }
}

/// Vectorized Less than test between two json values.
/// Returns back a json value of a boolean or an array of booleans.
///
pub fn json_lt(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(
            x.par_iter()
                .zip(y.par_iter())
                .map(|(x, y)| lt(x, y))
                .map(Json::from)
                .collect(),
        ),
        (Json::Array(x), val) | (val, Json::Array(x)) => {
            Json::Array(x.par_iter().map(|x| lt(x, val)).map(Json::from).collect())
        }
        (x, y) => Json::from(lt(x, y)),
    }
}

/// Vectorized Less than or equals to test between two json values.
/// Returns back a json value of a boolean or an array of booleans.
///
pub fn json_lte(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(
            x.par_iter()
                .zip(y.par_iter())
                .map(|(x, y)| lte(x, y))
                .map(Json::from)
                .collect(),
        ),
        (Json::Array(x), val) | (val, Json::Array(x)) => {
            Json::Array(x.par_iter().map(|x| lt(x, val)).map(Json::from).collect())
        }
        (x, y) => Json::from(lte(x, y)),
    }
}

/// Vectorized greater than or equals to test between two json values.
/// Returns back a json value of a boolean or an array of booleans.
///
pub fn json_gte(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Array(x), Json::Array(y)) => Json::Array(
            x.par_iter()
                .zip(y.par_iter())
                .map(|(x, y)| gte(x, y))
                .map(Json::from)
                .collect(),
        ),
        (Json::Array(x), val) | (val, Json::Array(x)) => {
            Json::Array(x.par_iter().map(|x| lt(x, val)).map(Json::from).collect())
        }
        (x, y) => Json::from(gte(x, y)),
    }
}

/// Json equality comparison test
pub fn json_equal(x: &Json, y: &Json) -> bool {
    x == y
}

// Vectorised or gate
fn or_arr(x: &[Json], y: bool) -> Result<Json, Error> {
    let r: Result<Vec<Json>, Error> = x.par_iter().map(|x| json_or(x, &Json::Bool(y))).collect();
    r.map(Json::Array)
}

/// or gate between two json values. Returns back a json value of a boolean.
pub fn json_or(x: &Json, y: &Json) -> Result<Json, Error> {
    match (x, y) {
        (Json::Bool(x), Json::Bool(y)) => Ok(Json::from(*x || *y)),
        (Json::Bool(val), Json::Array(vec)) | (Json::Array(vec), Json::Bool(val)) => {
            or_arr(vec, *val)
        }
        (x, y) => {
            let val = json!([x.clone(), y.clone()]);
            Err(Error::BadArg(val))
        }
    }
}

/// and gate between two json values. Returns back a json value of a boolean.
pub fn json_and(x: &Json, y: &Json) -> Result<Json, Error> {
    match (x, y) {
        (Json::Bool(x), Json::Bool(y)) => Ok(Json::from(*x && *y)),
        (x, y) => Err(Error::BadArg(json!([x.clone(), y.clone()]))),
    }
}

// not equals gate
pub fn json_neq(x: &Json, y: &Json) -> bool {
    x != y
}

/// Compares two json values for order.
fn json_cmp<'a>(x: &'a Json, y: &'a Json) -> Result<Ordering, Error> {
    match (x, y) {
        (Json::String(x), Json::String(y)) => Ok(x.cmp(y)),
        (Json::Number(x), Json::Number(y)) => Ok(num_cmp(x, y)),
        (Json::Number(_), _) => Err(Error::BadType),
        (_, Json::Number(_)) => Err(Error::BadType),
        (Json::Bool(x), Json::Bool(y)) => Ok(x.cmp(y)),
        (Json::Bool(_), _) | (_, Json::Bool(_)) => Err(Error::BadType),
        (Json::Null, Json::Null) => Ok(Ordering::Equal),
        (Json::Null, _) | (_, Json::Null) => Err(Error::BadType),
        (Json::Object(_), _) | (_, Json::Object(_)) | (Json::Array(_), _) | (_, Json::Array(_)) => {
            Err(Error::BadType)
        }
    }
}

// noteq compares that two json values do not equal each other and wraps the boolean into a json value.
pub fn noteq(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Array(lhs), Json::Array(rhs)) => {
            let v = lhs
                .iter()
                .zip(rhs.iter())
                .map(|(x, y)| json_neq(x, y))
                .map(Json::Bool)
                .collect();
            Json::Array(v)
        }
        (Json::Array(lhs), y) => {
            Json::Array(lhs.iter().map(|x| json_neq(x, y)).map(Json::Bool).collect())
        }
        (x, y) => Json::Bool(json_neq(x, y)),
    }
}

// greater than gate to test if x is greater than to y.
// Returns a boolean and returns false if incomparable json types.
pub fn gt(x: &Json, y: &Json) -> bool {
    matches!(json_cmp(x, y), Ok(Ordering::Greater))
}

// less than gate to test if x is less than to y.
// Returns a boolean and returns false if incomparable json types.
pub fn lt(x: &Json, y: &Json) -> bool {
    matches!(json_cmp(x, y), Ok(Ordering::Less))
}

// greater than or equals to gate to test if x is greater than or equal to y.
// Returns a boolean and returns false if incomparable json types.
pub fn gte(x: &Json, y: &Json) -> bool {
    matches!(json_cmp(x, y), Ok(Ordering::Greater) | Ok(Ordering::Equal))
}

// less than or equals to gate to test if x is less than or equal to y.
// Returns a boolean and returns false if incomparable json types.
pub fn lte(x: &Json, y: &Json) -> bool {
    matches!(json_cmp(x, y), Ok(Ordering::Less) | Ok(Ordering::Equal))
}

// counts the numbers of elements in the json value
pub fn json_count(val: &Json) -> Json {
    match val {
        Json::Array(ref arr) => Json::from(arr.len()),
        Json::Object(ref obj) => Json::from(obj.len()),
        _ => Json::from(1),
    }
}

// appends json to an existing json value.
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

/// retrieves the first element in the json value.
//TODO refactor arg from ref to val
pub fn json_first(val: &Json) -> Option<Json> {
    match val {
        Json::Array(ref arr) => {
            if !arr.is_empty() {
                Some(arr[0].clone())
            } else {
                None
            }
        }
        Json::String(s) => {
            let mut it = s.chars();
            it.next().map(|c| Json::from(c.to_string()))
        }
        val => Some(val.clone()),
    }
}

/// retrieves the last element in the json value.
pub fn json_last(val: &Json) -> Option<&Json> {
    match val {
        Json::Array(ref arr) => {
            if arr.is_empty() {
                return None;
            }
            let i = arr.len() - 1;
            Some(&arr[i])
        }
        val => Some(val),
    }
}

/// sums the json value.
pub fn json_sum(val: &Json) -> Json {
    match val {
        Json::Number(val) => Json::Number(val.clone()),
        Json::Array(ref arr) => json_arr_sum(arr),
        _ => Json::Null,
    }
}

/// pops off the last element of the json value.
pub fn json_pop(val: &mut Json) -> Result<Option<Json>, Error> {
    match val {
        Json::Array(ref mut arr) => Ok(arr.pop()),
        _ => Err(Error::ExpectedArr),
    }
}

/// calculates the average of the json value.
pub fn json_avg(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(val) => Ok(Json::Number(val.clone())),
        Json::Array(ref arr) => json_arr_avg(arr),
        _ => Err(Error::BadType),
    }
}

/// calculates the variance of the json value.
pub fn json_var(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(_) => Ok(Json::from(0)),
        Json::Array(ref arr) => json_arr_var(arr),
        _ => Err(Error::BadType),
    }
}

/// calculates the standard deviation of the json value.
pub fn json_dev(val: &Json) -> Result<Json, Error> {
    match val {
        Json::Number(_) => Ok(Json::from(0)),
        Json::Array(ref arr) => {
            if arr.len() < 2 {
                Ok(Json::from(0))
            } else {
                json_arr_dev(arr)
            }
        }
        _ => Err(Error::BadType),
    }
}

/// calculates the maximum value of the json value.
pub fn json_max(val: &Json) -> Option<&Json> {
    match val {
        Json::Array(ref a) if !a.is_empty() => a.par_iter().reduce_with(max),
        val => Some(val),
    }
}

//TODO(jaupe) add more cases
/// adds two json values together.
pub fn json_add(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Null, _) => Ok(Json::Null),
        (_, Json::Null) => Ok(Json::Null),
        (Json::Array(lhs), Json::Array(rhs)) => json_add_arrs(lhs, rhs),
        (Json::Array(lhs), rhs) => json_add_arr_val(lhs, rhs),
        (lhs, Json::Array(rhs)) => json_add_val_arr(lhs, rhs),
        (Json::Number(lhs), Json::Number(rhs)) => Ok(Json::Number(json_add_nums(lhs, rhs))),
        (Json::String(lhs), rhs) => json_add_str(lhs, json_tostring(rhs)),
        (lhs, Json::String(rhs)) => json_add_str(json_tostring(lhs), rhs),
        _ => Err(Error::BadType),
    }
}

pub fn json_add2(x: &Json, y: &Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => Json::Number(json_add_nums(x, y)),
        (Json::Number(x), _) => Json::Number(x.clone()),
        (_, Json::Number(y)) => Json::Number(y.clone()),
        _ => Json::from(0),
    }
}

//TODO(jaupe) add more cases
/// bars two json values together.
pub fn json_bar(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(lhs), Json::Array(rhs)) => json_bar_arrs(lhs, rhs),
        (Json::Array(lhs), rhs) => json_bar_arr_val(lhs, rhs),
        (Json::Number(lhs), Json::Number(rhs)) => json_bar_num_num(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

/// vectorised bar of  two json arrays.
fn json_bar_arrs(lhs: &[Json], rhs: &[Json]) -> Result<Json, Error> {
    let mut out = Vec::new();
    for (x, y) in lhs.iter().zip(rhs.iter()) {
        let val = json_bar(x, y)?;
        out.push(val);
    }
    Ok(Json::Array(out))
}

/// vectorised bar of an array and scalar.
fn json_bar_arr_val(lhs: &[Json], rhs: &Json) -> Result<Json, Error> {
    let mut out = Vec::new();
    for val in lhs {
        let val = json_bar(val, rhs)?;
        out.push(val);
    }
    Ok(Json::Array(out))
}

/// bar of two json numbers
fn json_bar_num_num(lhs: &Number, rhs: &Number) -> Result<Json, Error> {
    match (lhs.as_i64(), rhs.as_i64()) {
        (Some(x), Some(y)) => Ok(Json::from(x / y * y)),
        _ => Err(Error::BadType),
    }
}

/// subtraction of two json values
pub fn json_sub(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    println!("lhs={:?}\nrhs={:?}", lhs, rhs);
    match (lhs, rhs) {
        (Json::Array(lhs), Json::Array(rhs)) => json_sub_arrs(lhs, rhs),
        (Json::Array(lhs), rhs) => json_sub_arr_num(lhs, rhs),
        (lhs, Json::Array(rhs)) => json_sub_num_arr(lhs, rhs),
        (Json::Number(lhs), Json::Number(rhs)) => Ok(json_sub_nums(lhs, rhs)),
        _ => Err(Error::BadType),
    }
}

/// multiplication of two json values
pub fn json_mul(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(x), Json::Array(y)) => mul_arrs(x, y),
        (Json::Array(x), Json::Number(y)) => mul_arr_num(x, y),
        (Json::Number(x), Json::Array(y)) => mul_arr_num(y, x),
        (Json::Number(x), Json::Number(y)) => mul_nums(x, y),
        _ => Err(Error::BadType),
    }
}

/// compute the unique elements of the json value
pub fn json_unique(val: &Json) -> Json {
    match val {
        Json::Array(arr) => arr_unique(arr),
        val => val.clone(),
    }
}

/// compute the unique elements of the json array
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

/// compute the multiplication of two json scalars
fn mul_vals(x: &Json, y: &Json) -> Result<Json, Error> {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => mul_nums(x, y),
        _ => Err(Error::BadType),
    }
}

/// compute the multiplication of two json numbers
fn mul_nums(x: &JsonNum, y: &JsonNum) -> Result<Json, Error> {
    let val = match (x.is_i64(), y.is_i64()) {
        (true, true) => Json::from(x.as_i64().unwrap() * y.as_i64().unwrap()),
        _ => Json::from(x.as_f64().unwrap() * y.as_f64().unwrap()),
    };
    Ok(val)
}

/// compute the vectorized multiplication of a two json numbers
fn mul_arr_num(x: &[Json], y: &JsonNum) -> Result<Json, Error> {
    let mut arr = Vec::new();
    for x in x.iter() {
        arr.push(mul_val_num(x, y)?);
    }
    Ok(Json::from(arr))
}

/// compute the vectorized multiplication of a json value and json number
fn mul_val_num(x: &Json, y: &JsonNum) -> Result<Json, Error> {
    match x {
        Json::Number(ref x) => mul_nums(x, y),
        Json::Array(ref arr) => mul_arr_num(arr, y),
        _ => Err(Error::BadType),
    }
}

//TODO(jaupe) optimize by removing the temp allocs
/// compute the vectorized multiplication of a json arrays
fn mul_arrs(lhs: &[Json], rhs: &[Json]) -> Result<Json, Error> {
    let mut arr: Vec<Json> = Vec::new();
    for (x, y) in lhs.iter().zip(rhs.iter()) {
        arr.push(mul_vals(x, y)?);
    }
    Ok(Json::from(arr))
}

/// compute the vectorized division of two json values
pub fn json_div(lhs: &Json, rhs: &Json) -> Result<Json, Error> {
    match (lhs, rhs) {
        (Json::Array(ref lhs), Json::Array(ref rhs)) => div_arrs(lhs, rhs),
        (Json::Array(ref lhs), Json::Number(ref rhs)) => div_arr_num(lhs, rhs),
        (Json::Number(ref lhs), Json::Array(ref rhs)) => div_num_arr(lhs, rhs),
        (Json::Number(ref lhs), Json::Number(ref rhs)) => div_nums(lhs, rhs),
        _ => Err(Error::BadType),
    }
}

pub fn json_sort(val: &mut Json, descend: bool) {
    if let Json::Array(ref mut arr) = val {
        if descend {
            arr.par_sort_by(|x, y| json_str(x).cmp(&json_str(y)).reverse())
        } else {
            arr.par_sort_by(|x, y| json_str(x).cmp(&json_str(y)))
        }
    }
}

/// divide two json numbers
fn div_nums(x: &JsonNum, y: &JsonNum) -> Result<Json, Error> {
    let val = x.as_f64().unwrap() / y.as_f64().unwrap();
    Ok(Json::from(val))
}

/// vectorised division of two json arrays
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

fn json_add_str<X, Y>(x: X, y: Y) -> Result<Json, Error>
where
    X: Into<String>,
    Y: Into<String>,
{
    let val = x.into() + &y.into();
    Ok(Json::String(val))
}

//TODO(jaupe) add better error handlinge
fn json_add_arr_val(x: &[Json], y: &Json) -> Result<Json, Error> {
    let mut arr = Vec::new();
    for x in x {
        arr.push(json_add(x, y)?);
    }
    Ok(Json::Array(arr))
}

fn json_add_val_arr(x: &Json, y: &[Json]) -> Result<Json, Error> {
    let mut arr = Vec::new();
    for y in y {
        arr.push(json_add(x, y)?);
    }
    Ok(Json::Array(arr))
}

fn json_add_arrs(lhs: &[Json], rhs: &[Json]) -> Result<Json, Error> {
    let vec: Result<Vec<Json>, _> = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(x, y)| json_add(x, y))
        .collect();
    Ok(Json::Array(vec?))
}

pub(crate) fn json_add_nums(x: &JsonNum, y: &JsonNum) -> JsonNum {
    match (x.is_i64(), y.is_i64()) {
        (true, true) => JsonNum::from(x.as_i64().unwrap() + y.as_i64().unwrap()),
        _ => JsonNum::from_f64(x.as_f64().unwrap() + y.as_f64().unwrap()).unwrap(),
    }
}

fn json_sub_arr_num(x: &[Json], y: &Json) -> Result<Json, Error> {
    let mut arr = Vec::new();
    for x in x {
        arr.push(json_sub(x, y)?);
    }
    Ok(Json::Array(arr))
}

fn json_sub_num_arr(x: &Json, y: &[Json]) -> Result<Json, Error> {
    let mut out = Vec::new();
    for y in y {
        out.push(json_sub(x, y)?);
    }
    Ok(Json::Array(out))
}

fn json_sub_arrs(lhs: &[Json], rhs: &[Json]) -> Result<Json, Error> {
    let vec = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(x, y)| json_sub(x, y).unwrap())
        .collect();
    Ok(Json::Array(vec))
}

fn json_sub_nums(x: &JsonNum, y: &JsonNum) -> Json {
    match (x.as_i64(), y.as_i64()) {
        (Some(x), Some(y)) => Json::from(x - y),
        (Some(x), None) => Json::from(x as f64 - y.as_f64().unwrap()),
        (None, Some(y)) => Json::from(x.as_f64().unwrap() - y as f64),
        (None, None) => Json::from(x.as_f64().unwrap() - y.as_f64().unwrap()),
    }
}

pub fn json_min(val: &Json) -> Option<&Json> {
    match val {
        Json::Array(ref arr) if !arr.is_empty() => arr_min(arr),
        val => Some(val),
    }
}

pub fn json_tostring(val: &Json) -> String {
    match val {
        Json::String(s) => s.to_string(),
        val => val.to_string(),
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
        Json::Number(num) => {
            if let Some(x) = num.as_f64() {
                Some(x)
            } else {
                num.as_i64().map(|x| x as f64)
            }
        }
        _ => None,
    }
}

fn max<'a>(x: &'a Json, y: &'a Json) -> &'a Json {
    if gt(x, y) {
        x
    } else {
        y
    }
}

fn min<'a>(x: &'a Json, y: &'a Json) -> &'a Json {
    if lt(x, y) {
        x
    } else {
        y
    }
}

fn arr_min(s: &[Json]) -> Option<&Json> {
    s.par_iter().reduce_with(min)
}

pub fn json_string(x: &Json) -> Json {
    match x {
        Json::Array(arr) => Json::Array(arr.par_iter().map(json_string).collect()),
        Json::String(s) => Json::String(s.to_string()),
        val => Json::String(val.to_string()),
    }
}

pub fn json_get(key: &str, val: Json) -> Option<Json> {
    match val {
        Json::Array(arr) => {
            if arr.is_empty() {
                return None;
            }
            let mut out = Vec::new();
            for val in arr {
                if let Some(v) = json_get(key, val) {
                    out.push(v.clone());
                }
            }
            if out.is_empty() {
                None
            } else {
                Some(Json::Array(out))
            }
        }
        Json::Object(obj) => obj.get(key).cloned(),
        val => Some(val),
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

pub fn json_insert(val: &mut Json, rows: Vec<JsonObj>) {
    match val {
        Json::Array(ref mut arr) => arr.extend(rows.into_iter().map(Json::Object)),
        val => {
            let mut arr = vec![val.clone()];
            arr.extend(rows.into_iter().map(Json::Object));
            *val = Json::Array(arr);
        }
    }
}

pub fn json_str(val: &Json) -> String {
    match val {
        Json::String(s) => s.clone(),
        val => val.to_string(),
    }
}

pub fn json_median(_val: &Json) -> Result<Json, Error> {
    unimplemented!()
}

type F = fn(&Json) -> Result<Json, Error>;

fn map(f: &str) -> Option<F> {
    match f {
        "avg" => Some(|x| json_avg(x)),
        "dev" => Some(|x| json_dev(x)),
        "first" => Some(|x| Ok(json_first(x).unwrap_or(Json::Null))),
        "flat" => Some(|x| Ok(json_flat(x))), //TODO remove clone
        "json" => Some(|x| Ok(x.clone())),
        //"last" => Some(|x| Ok(json_last(x))),
        "len" => Some(|x| Ok(json_count(x))),
        //"max" => Some(|x| Ok(wrap(json_max(x)))),
        //"min" => Some(|x| Ok(wrap(json_min(x)))),
        "sum" => Some(|x| Ok(json_sum(x))),
        "unique" => Some(|x| Ok(json_unique(x))),
        "var" => Some(|x| json_var(x)),
        _ => None,
    }
}

pub fn json_has(val: &Json, key: &str) -> Json {
    match val {
        Json::Array(arr) => arr_has(arr, key),
        Json::Object(map) => obj_has(map, key),
        _ => Json::from(false),
    }
}

pub fn arr_has(arr: &[Json], key: &str) -> Json {
    let v = arr
        .par_iter()
        .map(|x| x.get(key).is_some())
        .map(Json::Bool)
        .collect();
    Json::Array(v)
}

pub fn obj_has(obj: &JsonObj, key: &str) -> Json {
    Json::Bool(obj.get(key).is_some())
}

pub fn json_in(lhs: &Json, rhs: &Json) -> Json {
    if let Json::Array(arr) = lhs {
        Json::Array(arr.iter().map(|x| Json::Bool(x == rhs)).collect())
    } else {
        Json::Bool(lhs == rhs)
    }
}

pub fn json_merge(x: &Json, y: &Json) -> Json {
    let mut out = Vec::new();
    json_arr_merge(x, &mut out);
    json_arr_merge(y, &mut out);
    Json::Array(out)
}

fn json_arr_merge(val: &Json, out: &mut Vec<Json>) {
    match val {
        Json::Array(arr) => {
            for val in arr {
                match val {
                    Json::Array(arr) => {
                        for val in arr {
                            out.push(val.clone());
                        }
                    }
                    val => out.push(val.clone()),
                }
            }
        }
        val => out.push(val.clone()),
    };
}

pub fn json_flat(val: &Json) -> Json {
    match val {
        Json::Array(arr) => {
            let mut out: Vec<Json> = Vec::new();
            for val in arr {
                match val {
                    Json::Array(arr) => out.extend(arr.clone()),
                    val => out.push(val.clone()),
                }
            }
            Json::Array(out)
        }
        val => val.clone(),
    }
}

pub fn json_fold_add(x: Json, y: &Json) -> Json {
    match y {
        Json::Number(y) => {
            if let Json::Number(x) = x {
                Json::Number(json_add_nums(&x, y))
            } else {
                Json::Number(y.clone())
            }
        }
        _ => x,
    }
}

pub fn json_reduce_add(x: Json, y: Json) -> Json {
    match (x, y) {
        (Json::Number(x), Json::Number(y)) => Json::Number(json_add_nums(&x, &y)),
        (x, _) => x,
    }
}

pub fn json_numsort(val: Json, descend: bool) -> Json {
    match val {
        Json::Array(mut arr) => {
            if descend {
                arr.sort_by_key(|x| std::cmp::Reverse(x.as_i64()));
            } else {
                arr.sort_by_key(|x| x.as_i64());
            }
            Json::Array(arr)
        }
        val => val,
    }
}

pub fn json_map(val: &Json, f: String) -> Result<Json, Error> {
    let f = map(&f).ok_or(Error::BadCmd)?;
    match val {
        Json::Array(arr) => {
            let mut v = Vec::with_capacity(arr.len());
            for val in arr {
                v.push(f(val)?);
            }
            Ok(Json::Array(v))
        }
        val => f(val),
    }
}

fn arr_slice(
    _arr: Vec<Json>,
    start: Option<usize>,
    end: Option<usize>,
) -> Result<Vec<Json>, Error> {
    let _s = match (start, end) {
        (Some(_s), Some(_e)) => {
            unimplemented!()
        }
        (Some(_s), None) => unimplemented!(),
        (None, Some(_s)) => unimplemented!(),
        (None, None) => unimplemented!(),
    };
    //Ok(Json::Array(s))
}

pub fn json_slice(val: Json, range: Range) -> Result<Json, Error> {
    match val {
        Json::Array(vec) => {
            let s = arr_slice(vec, range.start, range.size)?;
            Ok(Json::from(s))
        }
        _ => Err(Error::ExpectedArr),
    }
}

pub fn json_reverse(val: &mut Json) {
    if let Json::Array(ref mut arr) = val {
        arr.reverse();
    }
}

pub fn json_sortby(_val: &mut Json, _key: &str) {
    todo!("work out how to handle non existing entries")
}

pub fn json_ord(x: &Json, y: &Json) -> Ordering {
    if x == y {
        Ordering::Equal
    } else if gt(x, y) {
        Ordering::Greater
    } else {
        Ordering::Less
    }
}

pub fn json_desc_ord(x: &Json, y: &Json) -> Ordering {
    if x == y {
        Ordering::Equal
    } else if gt(x, y) {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

pub fn sortby_key(key: &str, x: &Json, y: &Json) -> Ordering {
    match (x.get(key), y.get(key)) {
        (Some(x), Some(y)) => json_ord(x, y),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

pub fn sortby_desc_key(key: &str, x: &Json, y: &Json) -> Ordering {
    match (x.get(key), y.get(key)) {
        (Some(x), Some(y)) => json_desc_ord(x, y),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn num_cmp(x: &Number, y: &Number) -> Ordering {
    match (x.is_i64(), y.is_i64()) {
        (true, true) => x.as_i64().unwrap().cmp(&y.as_i64().unwrap()),
        _ => {
            let x = x.as_f64().unwrap();
            let y = y.as_f64().unwrap();
            if (x - y).abs() < f64::EPSILON {
                Ordering::Equal
            } else if x < y {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
    }
}

pub fn json_len(val: &Json) -> usize {
    match val {
        Json::Array(arr) => arr.len(),
        _ => 1,
    }
}

pub fn json_innerjoin(lhs: &[Json], lhs_key: &str, rhs: &[Json], rhs_key: &str, key: &str) -> Json {
    let mut out: Vec<Json> = Vec::new();
    for lhs_val in lhs {
        if !lhs_val.is_object() {
            continue;
        }
        if let Some(lv) = lhs_val.get(lhs_key) {
            for rhs_val in rhs {
                if !rhs_val.is_object() {
                    continue;
                }
                if let Some(rv) = rhs_val.get(rhs_key) {
                    if lv == rv {
                        let mut lhs_obj = lhs_val.as_object().unwrap().clone();
                        lhs_obj.insert(key.to_string(), rhs_val.clone());
                        out.push(Json::from(lhs_obj));
                        break;
                    }
                }
            }
        }
    }
    Json::from(out)
}

pub fn json_outerjoin(lhs: &[Json], lhs_key: &str, rhs: &[Json], rhs_key: &str, key: &str) -> Json {
    let mut out = Vec::new();
    for l in lhs {
        if !l.is_object() {
            continue;
        }
        let l_val = match l.get(lhs_key) {
            Some(v) => v.clone(),
            None => continue,
        };
        let mut l_obj = l.as_object().unwrap().clone();
        for r in rhs {
            let r_obj = match r {
                Json::Object(o) => o,
                _ => continue,
            };
            let r_val = match r_obj.get(rhs_key) {
                Some(v) => v,
                None => continue,
            };
            if l_val == *r_val {
                l_obj.insert(key.to_string(), r.clone());
                break;
            }
        }
        out.push(Json::from(l_obj));
    }
    Json::Array(out)
}

#[cfg(test)]
mod tests {

    use super::*;

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

    #[test]
    fn json_get_arr_obj() {
        let obj = json!({"name":"anna", "age": 28});
        assert_eq!(Some(Json::from("anna")), json_get("name", obj.clone()));
        assert_eq!(Some(Json::from(28)), json_get("age", obj));
    }

    #[test]
    fn json_insert_arr() {
        let f = |x: Json| x.as_object().cloned().unwrap();
        let mut val = json!([{"name":"anna", "age": 28}]);
        json_insert(
            &mut val,
            vec![
                f(json!({"name":"james", "age": 32})),
                f(json!({"name":"misha", "age": 9})),
            ],
        );
        assert_eq!(
            json!([
                {"name":"anna", "age": 28},
                {"name":"james", "age": 32},
                {"name":"misha", "age": 9},
            ]),
            val
        );
    }

    #[test]
    fn json_bar_ok() {
        let lhs = json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let rhs = Json::from(2);
        assert_eq!(
            Ok(json!([0, 0, 2, 2, 4, 4, 6, 6, 8, 8])),
            json_bar(&lhs, &rhs)
        );
    }

    #[test]
    fn json_sort_ok() {
        let mut val = json!([1, 9, 4, 8, 3, 6, 10, 5, 7, 2]);
        json_sort(&mut val, false);
        assert_eq!(json!([1, 10, 2, 3, 4, 5, 6, 7, 8, 9]), val);

        let mut val = json!(["1", 9, 4, 8, 3, 6, 10, 5, 7, 1]);
        json_sort(&mut val, false);
        assert_eq!(json!(['1', 1, 10, 3, 4, 5, 6, 7, 8, 9]), val);
    }

    #[test]
    fn test_json_innerjoin() {
        let lhs = json!([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]);
        let rhs = json!([{"b": 1}, {"b": 2}, {"b": 3}, {"b": 4}]);
        assert_eq!(
            json!([
                {"a": 1, "c": {"b": 1}},
                {"a": 2, "c": {"b": 2}},
                {"a": 3, "c": {"b": 3}},
                {"a": 4, "c": {"b": 4}}
            ]),
            json_innerjoin(
                lhs.as_array().unwrap(),
                "a",
                rhs.as_array().unwrap(),
                "b",
                "c"
            )
        );

        let lhs = json!([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]);
        let rhs = json!([{"b": 6}, {"b": 7}, {"b": 8}, {"b": 9}]);
        assert_eq!(
            json!([]),
            json_innerjoin(
                lhs.as_array().unwrap(),
                "a",
                rhs.as_array().unwrap(),
                "b",
                "c"
            )
        );
        let lhs = json!([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]);
        let rhs = json!([]);
        assert_eq!(
            json!([]),
            json_innerjoin(
                lhs.as_array().unwrap(),
                "a",
                rhs.as_array().unwrap(),
                "b",
                "c"
            )
        );
    }
    #[test]
    fn test_json_outerjoin() {
        let lhs = json!([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]);
        let rhs = json!([{"b": 1}, {"b": 2}, {"b": 3}, {"b": 4}]);
        assert_eq!(
            json!([
                {"a": 1, "c": {"b": 1}},
                {"a": 2, "c": {"b": 2}},
                {"a": 3, "c": {"b": 3}},
                {"a": 4, "c": {"b": 4}},
                {"a": 5}
            ]),
            json_outerjoin(
                lhs.as_array().unwrap(),
                "a",
                rhs.as_array().unwrap(),
                "b",
                "c"
            )
        );

        let lhs = json!([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]);
        let rhs = json!([{"b": 6}, {"b": 7}, {"b": 8}, {"b": 9}]);
        assert_eq!(
            json!([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]),
            json_outerjoin(
                lhs.as_array().unwrap(),
                "a",
                rhs.as_array().unwrap(),
                "b",
                "c"
            )
        );

        let lhs = json!([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]);
        let rhs = json!([]);
        assert_eq!(
            json!([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}]),
            json_outerjoin(
                lhs.as_array().unwrap(),
                "a",
                rhs.as_array().unwrap(),
                "b",
                "c"
            )
        );
    }
}
