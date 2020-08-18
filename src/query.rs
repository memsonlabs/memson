use crate::cmd::{Cmd, QueryCmd};
use crate::err::Error;
use crate::rdb::InMemDb;

use crate::json::*;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Filter {
    #[serde(rename = "==")]
    Eq(String, Json),
    #[serde(rename = "!=")]
    NotEq(String, Json),
    #[serde(rename = ">")]
    Gt(String, Json),
    #[serde(rename = "<")]
    Lt(String, Json),
    #[serde(rename = ">=")]
    Gte(String, Json),
    #[serde(rename = "<=")]
    Lte(String, Json),
    #[serde(rename = "&&")]
    And(Box<Filter>, Box<Filter>),
    #[serde(rename = "||")]
    Or(Box<Filter>, Box<Filter>),
}

impl Filter {
    pub fn apply(&self, row: &Json) -> Result<bool, Error> {
        match self {
            Filter::Eq(key, val) => Filter::eval(row, key, val, &json_eq),
            Filter::NotEq(key, val) => Filter::eval(row, key, val, &json_neq),
            Filter::Lt(key, val) => Filter::eval(row, key, val, &json_lt),
            Filter::Gt(key, val) => Filter::eval(row, key, val, &json_gt),
            Filter::Lte(key, val) => Filter::eval(row, key, val, &json_lte),
            Filter::Gte(key, val) => Filter::eval(row, key, val, &json_gte),
            Filter::And(lhs, rhs) => Filter::eval_gate(row, lhs, rhs, &|x, y| x && y),
            Filter::Or(lhs, rhs) => Filter::eval_gate(row, lhs, rhs, &|x, y| x || y),
        }
    }

    fn eval_gate(
        row: &Json,
        lhs: &Filter,
        rhs: &Filter,
        p: &dyn Fn(bool, bool) -> bool,
    ) -> Result<bool, Error> {
        let lhs = lhs.apply(row)?;
        let rhs = rhs.apply(row)?;
        Ok(p(lhs, rhs))
    }

    fn eval(
        row: &Json,
        key: &str,
        val: &Json,
        predicate: &dyn Fn(&Json, &Json) -> Result<bool, Error>,
    ) -> Result<bool, Error> {
        match row {
            Json::Object(obj) => match obj.get(key) {
                Some(v) => predicate(v, val),
                None => Ok(false),
            },
            v => predicate(v, val),
        }
    }
}

#[derive(Debug, Default)]
struct KeyedMax {
    col_name: String,
    key: String,
}

impl KeyedMax {
    fn new<S: Into<String>>(col_name: S, key: String) -> Self {
        KeyedMax {
            col_name: col_name.into(),
            key,
        }
    }
}

fn key_obj_max(key: &str, val: &Json) -> Result<Option<Json>, Error> {
    match val {
        Json::Array(arr) => {
            if arr.is_empty() {
                return Ok(None);
            }
            let mut max = None;
            for val in arr {
                match val {
                    Json::Object(obj) => {
                        max = if let Some(val) = obj.get(key) {
                            if let Some(max) = max {
                                let is_max = json_gt(val, max).map_err(|e| Error::BadType)?;
                                if is_max {
                                    Some(val)
                                } else {
                                    Some(max)
                                }
                            } else {
                                Some(val)
                            }
                        } else {
                            max
                        };
                    }
                    _ => return Err(Error::BadObject),
                }
            }
            Ok(max.map(|x| x.clone()))
        }
        val => Ok(Some(val.clone())),
    }
}

fn key_obj_min(key: &str, val: &Json) -> Result<Option<Json>, Error> {
    match val {
        Json::Array(arr) => {
            if arr.is_empty() {
                return Ok(None);
            }
            let mut min = None;
            for val in arr {
                match val {
                    Json::Object(obj) => {
                        min = if let Some(val) = obj.get(key) {
                            if let Some(max) = min {
                                if json_lt(val, max).map_err(|_| Error::BadType)? {
                                    Some(val)
                                } else {
                                    Some(max)
                                }
                            } else {
                                Some(val)
                            }
                        } else {
                            min
                        };
                    }
                    _ => return Err(Error::BadObject),
                }
            }
            println!("min = {:?}", min);
            Ok(min.map(|x| x.clone()))
        }
        val => Ok(Some(val.clone())),
    }
}

impl KeyedAggregate for KeyedMax {
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error> {
        for (k, v) in input {
            println!("finding max of {:?}", v);
            let val = key_obj_max(&self.key, v).map_err(|err| {
                eprintln!("{:?}", err);
                Error::BadType
            })?;
            println!("{:?}", val);
            if let Some(val) = val {
                let entry = output
                    .entry(k.clone())
                    .or_insert_with(|| Json::from(JsonObj::new()));
                match entry {
                    Json::Object(ref mut obj) => {
                        obj.insert(self.col_name.clone(), val);
                    }
                    _ => return Err(Error::ExpectedObj),
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct KeyedMin {
    col_name: String,
    key: String,
}

impl KeyedMin {
    fn new<S: Into<String>>(col_name: S, key: String) -> Self {
        KeyedMin {
            col_name: col_name.into(),
            key,
        }
    }
}

impl KeyedAggregate for KeyedMin {
    //TODO refactor to make generic between this and KeyedMax as the logic is too similar
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error> {
        for (k, v) in input {
            println!("finding max of {:?}", v);
            let val = key_obj_min(&self.key, v).map_err(|err| {
                eprintln!("{:?}", err);
                Error::BadType
            })?;
            println!("{:?}", val);
            if let Some(val) = val {
                let entry = output
                    .entry(k.clone())
                    .or_insert_with(|| Json::from(JsonObj::new()));
                match entry {
                    Json::Object(ref mut obj) => {
                        obj.insert(self.col_name.clone(), val);
                    }
                    _ => unimplemented!(),
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct KeyedLast {
    col_name: String,
    key: String,
}

impl KeyedLast {
    fn new<S: Into<String>>(name: S, key: String) -> Self {
        KeyedLast {
            col_name: name.into(),
            key,
        }
    }
}

impl KeyedAggregate for KeyedLast {
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error> {
        for (k, v) in input {
            let val = match v {
                Json::Array(s) => {
                    if s.is_empty() {
                        return Ok(());
                    }
                    s[s.len() - 1].clone()
                }
                val => val.clone(),
            };
            output.insert(k.clone(), val);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct KeyedFirst {
    col_name: String,
    key: String,
}

impl KeyedFirst {
    fn new<S: Into<String>>(col_name: S, key: String) -> Self {
        Self {
            col_name: col_name.into(),
            key,
        }
    }
}

impl KeyedAggregate for KeyedFirst {
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error> {
        for (key, val) in input {
            let val = match val {
                Json::Array(s) => {
                    if s.is_empty() {
                        return Ok(());
                    }
                    s[0].clone()
                }
                val => val.clone(),
            };
            output.insert(key.clone(), val);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Count {
    col_name: String,
    key: String,
}

impl Count {
    fn new<S: Into<String>>(col_name: S, key: String) -> Count {
        Self {
            col_name: col_name.into(),
            key,
        }
    }
}

impl KeyedAggregate for Count {
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error> {
        for (key, val) in input {
            let entry = output
                .entry(key.to_string())
                .or_insert_with(|| Json::Object(JsonObj::new()));
            match entry {
                Json::Object(ref mut obj) => {
                    obj.insert(self.col_name.clone(), Json::from(json_len(val)));
                }
                _ => return Err(Error::ExpectedObj),
            }
        }
        Ok(())
    }
}

trait KeyedAggregate {
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error>;
}

trait Aggregate<'a> {
    fn aggregate(&mut self, val: &'a Json) -> Result<(), Error>;
    fn apply(self) -> Option<Json>;
}

#[derive(Debug, Default)]
struct Get {
    col_name: String,
    key: String,
    map: Option<JsonObj>,
}

#[derive(Debug, Default)]
struct Last<'a> {
    val: Option<&'a Json>,
}

impl<'a> Last<'a> {
    fn new() -> Self {
        Self::default()
    }
}

impl<'a> Aggregate<'a> for Last<'a> {
    fn aggregate(&mut self, val: &'a Json) -> Result<(), Error> {
        self.val = Some(val);
        Ok(())
    }

    fn apply(self) -> Option<Json> {
        self.val.map(|x| x.clone())
    }
}

#[derive(Debug, Default)]
struct First<'a> {
    val: Option<&'a Json>,
}

impl<'a> First<'a> {
    fn new() -> Self {
        Self::default()
    }
}

impl<'a> Aggregate<'a> for First<'a> {
    fn aggregate(&mut self, val: &'a Json) -> Result<(), Error> {
        if self.val.is_none() {
            self.val = Some(val);
        }
        Ok(())
    }

    fn apply(self) -> Option<Json> {
        self.val.map(|x| x.clone())
    }
}

#[derive(Debug, Default)]
struct Sum {
    total: Option<Json>,
}

impl Sum {
    fn new() -> Self {
        Sum::default()
    }
}

impl<'a> Aggregate<'a> for Sum {
    fn aggregate(&mut self, val: &'a Json) -> Result<(), Error> {
        self.total = match (&self.total, val) {
            (None, Json::Number(val)) => Some(Json::Number(val.clone())),
            (None, _) => return Err(Error::BadType),
            (Some(val), Json::Number(y)) => {
                let val = json_add_num(val, y).ok_or(Error::BadType)?;
                Some(val)
            }
            _ => return Err(Error::BadType),
        };
        Ok(())
    }

    fn apply(self) -> Option<Json> {
        self.total
    }
}

#[derive(Debug, Default)]
struct Min<'a> {
    min: Option<&'a Json>,
}

impl<'a> Min<'a> {
    fn new() -> Self {
        Self::default()
    }
}

impl<'a> Aggregate<'a> for Min<'a> {
    fn aggregate(&mut self, val: &'a Json) -> Result<(), Error> {
        self.min = match (&self.min, val) {
            (None, val) => Some(val),
            (Some(Json::String(y)), Json::String(x)) => {
                if x < y {
                    Some(val)
                } else {
                    self.min
                }
            }
            (Some(Json::String(_)), _) => return Err(Error::BadType),
            (Some(Json::Number(x)), Json::Number(y)) => {
                let r = json_num_lt(x, y).ok_or(Error::BadType)?;
                if r {
                    self.min
                } else {
                    Some(val)
                }
            }
            _ => unimplemented!(),
        };
        Ok(())
    }

    fn apply(self) -> Option<Json> {
        self.min.map(|x| x.clone())
    }
}

#[derive(Debug, Default)]
struct Max<'a> {
    max: Option<&'a Json>,
}

impl<'a> Max<'a> {
    fn new() -> Self {
        Max::default()
    }

    fn update(&mut self, val: &'a Json) {
        self.max = Some(val);
    }
}

impl<'a> Aggregate<'a> for Max<'a> {
    fn aggregate(&mut self, val: &'a Json) -> Result<(), Error> {
        let max = match self.max {
            Some(max) => match (max, val) {
                (Json::String(x), Json::String(y)) => {
                    if y > x {
                        val
                    } else {
                        max
                    }
                }
                (Json::String(_), _) => return Err(Error::BadType),
                (Json::Number(x), Json::Number(y)) => {
                    let r = json_num_gt(y, x).ok_or(Error::BadType)?;
                    if r {
                        val
                    } else {
                        max
                    }
                }
                (Json::Bool(x), Json::Bool(y)) => {
                    if y > x {
                        val
                    } else {
                        max
                    }
                }
                _ => unimplemented!(),
            },
            None => val,
        };
        self.update(max);
        Ok(())
    }

    fn apply(self) -> Option<Json> {
        self.max.map(|x| x.clone())
    }
}

#[derive(Debug, Default)]
struct Var {
    total: f64,
    mean: f64,
    count: usize,
}

impl Var {
    fn from(mean: f64, count: usize) -> Self {
        Var {
            total: 0.0,
            mean,
            count,
        }
    }
}

impl<'a> Aggregate<'a> for Var {
    fn aggregate(&mut self, val: &'a Json) -> Result<(), Error> {
        let mut val = val.as_f64().ok_or(Error::BadType)?;
        val = val - self.mean;
        val = val * val; //square the diff
        self.total += val;
        Ok(())
    }

    fn apply(self) -> Option<Json> {
        let var = self.total / ((self.count - 1) as f64);
        Some(Json::from(var))
    }
}

#[derive(Debug, Default)]
struct Avg {
    total: f64,
    count: usize,
}

impl Avg {
    fn new() -> Self {
        Self::default()
    }

    fn avg(&self) -> f64 {
        self.total / self.count as f64
    }
}

impl<'a> Aggregate<'a> for Avg {
    fn aggregate(&mut self, val: &'a Json) -> Result<(), Error> {
        self.total += json_f64(val).ok_or(Error::BadType)?;
        self.count += 1;
        Ok(())
    }

    fn apply(self) -> Option<Json> {
        if self.count == 0 {
            None
        } else {
            let val = self.avg();
            Some(Json::from(val))
        }
    }
}

type JsonObj = Map<String, Json>;

fn eq(x: &Json, y: &Json) -> Result<bool, Error> {
    json_eq(x, y)
}

fn neq(x: &Json, y: &Json) -> Result<bool, Error> {
    json_neq(x, y)
}

fn lt(x: &Json, y: &Json) -> Result<bool, Error> {
    json_lt(x, y)
}

fn lte(x: &Json, y: &Json) -> Result<bool, Error> {
    json_lte(x, y)
}

fn gt(x: &Json, y: &Json) -> Result<bool, Error> {
    json_gt(x, y)
}

fn gte(x: &Json, y: &Json) -> Result<bool, Error> {
    json_gte(x, y)
}

//TODO pass vec to remove clone
fn parse_selects_to_cmd(selects: &[Json]) -> Result<Vec<Cmd>, Error> {
    let mut cmds = Vec::new();
    for select in selects {
        cmds.extend(Cmd::parse(select.clone()).map_err(|_| Error::BadSelect)?);
    }
    Ok(cmds)
}

fn eval_aggregate(cmd: Cmd, rows: &[Json]) -> Result<Option<Json>, Error> {
    match cmd {
        Cmd::Sum(key) => eval_agg(&key, rows, Sum::new()),
        Cmd::Max(key) => eval_agg(&key, rows, Max::new()),
        Cmd::Min(key) => eval_agg(&key, rows, Min::new()),
        Cmd::Avg(key) => eval_agg(&key, rows, Avg::new()),
        Cmd::First(key) => eval_agg(&key, rows, First::new()),
        Cmd::Last(key) => eval_agg(&key, rows, Last::new()),
        Cmd::Var(key) => eval_var(&key, rows),
        Cmd::Dev(key) => eval_dev(&key, rows),
        _ => return Err(Error::NotAggregate),
    }
}

fn eval_nonaggregate(cmds: &[(String, Cmd)], rows: &[Json]) -> Result<Json, Error> {
    let mut out = Vec::new();
    for row in rows {
        let mut obj = None;
        for cmd in cmds {
            eval_row(&mut obj, cmd, row)?;
        }
        if let Some(obj) = obj {
            out.push(Json::from(obj));
        }
    }
    Ok(Json::Array(out))
}

fn eval_row(out: &mut Option<JsonObj>, cmd: &(String, Cmd), row: &Json) -> Result<(), Error> {
    match (&cmd.1, row) {
        (Cmd::Get(key), Json::Object(obj)) => {
            if let Some(val) = obj.get(key) {
                let key = cmd.0.to_string();
                let val = val.clone();
                if let Some(ref mut obj) = out {
                    obj.insert(key, val);
                } else {
                    let mut o = JsonObj::new();
                    o.insert("_id".to_string(), obj.get("_id").unwrap().clone());
                    o.insert(key, val);
                    *out = Some(o);
                }
            }
            Ok(())
        }
        _ => unimplemented!(),
    }
}

fn eval_var(key: &str, rows: &[Json]) -> Result<Option<Json>, Error> {
    let mut avg = Avg::new();
    for row in rows {
        if let Some(val) = row.get(key) {
            avg.aggregate(val)?;
        }
    }
    if avg.count == 0 {
        return Ok(None);
    }
    let mut var = Var::from(avg.avg(), avg.count);
    for row in rows {
        if let Some(val) = row.get(key) {
            var.aggregate(val)?;
        }
    }
    Ok(var.apply())
}

fn eval_dev(key: &str, rows: &[Json]) -> Result<Option<Json>, Error> {
    let r = match eval_var(key, rows)? {
        Some(val) => val.as_f64().map(|x| Json::from(x.sqrt())),
        None => None,
    };
    Ok(r)
}

fn eval_agg<'a, A: 'a>(key: &str, rows: &'a [Json], mut agg: A) -> Result<Option<Json>, Error>
where
    A: Aggregate<'a>,
{
    for row in rows {
        if let Some(val) = row.get(key) {
            agg.aggregate(val)?;
        }
    }
    Ok(agg.apply())
}

pub struct Query<'a> {
    db: &'a InMemDb,
    cmd: QueryCmd,
}

fn is_aggregation(cmds: &[(String, Cmd)]) -> bool {
    for (_, cmd) in cmds {
        if !cmd.is_aggregate() {
            return false;
        }
    }
    true
}

impl<'a> Query<'a> {
    pub fn from(db: &'a InMemDb, cmd: QueryCmd) -> Self {
        Self { db, cmd }
    }

    pub fn eval(&self) -> Result<Json, Error> {
        let rows = self.eval_from()?;
        if let Some(ref filter) = self.cmd.filter {
            let filtered_rows = self.eval_where(rows, filter.clone())?;
            self.eval_select(&filtered_rows)
        } else {
            self.eval_select(rows)
        }
    }

    fn eval_where(&self, rows: &[Json], filter: Filter) -> Result<Vec<Json>, Error> {
        let mut filtered_rows = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            if filter.apply(row)? {
                let obj_row = add_row_id(row, i)?;
                filtered_rows.push(obj_row);
            }
        }
        Ok(filtered_rows)
    }

    fn eval_from(&self) -> Result<&'a [Json], Error> {
        //TODO remove cloning
        match self.db.get(self.cmd.from.to_string()) {
            Ok(Json::Array(rows)) => Ok(rows.as_ref()),
            Ok(_) => return Err(Error::NotArray),
            Err(_) => return Err(Error::BadFrom),
        }
    }

    // TODO remove cloning
    fn eval_select(&self, rows: &[Json]) -> Result<Json, Error> {
        if let Some(by) = &self.cmd.by {
            self.eval_keyed_select(by, rows)
        } else {
            match &self.cmd.selects {
                Some(Json::Array(ref selects)) => self.eval_selects(selects, rows),
                Some(Json::String(s)) => self.eval_selects(&vec![Json::from(s.to_string())], rows),
                Some(Json::Object(obj)) => self.eval_obj_selects(obj.clone(), rows),
                Some(_) => Err(Error::BadSelect),
                None => self.eval_select_all(rows),
            }
        }
    }

    fn parse_ids(&self) -> Result<Option<Vec<String>>, Error> {
        if let Some(selects) = &self.cmd.selects {
            match selects {
                Json::Object(obj) => {
                    let mut ids = Vec::new();
                    for (_, v) in obj {
                        match v {
                            Json::Object(obj) => {
                                for (_, v) in obj {
                                    ids.push(json_to_string(v).ok_or(Error::BadSelect)?);
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }
                    Ok(Some(ids))
                }
                _ => unimplemented!(),
            }
        } else {
            Ok(None)
        }
    }

    fn eval_keyed_select(&self, by: &Json, rows: &[Json]) -> Result<Json, Error> {
        let by_key: &str = match by {
            Json::String(s) => s.as_ref(),
            _ => unimplemented!(),
        };
        let ids = self.parse_ids()?; // TODO remove unwrap
        let mut keyed_data: Map<String, Json> = Map::new();
        for row in rows {
            match row.get(by_key) {
                Some(by_val) => {
                    let obj: JsonObj = if let Some(ids) = &ids {
                        let mut obj = Map::new();
                        for id in ids {
                            if let Some(val) = row.get(id) {
                                obj.insert(id.clone(), val.clone());
                            }
                        }
                        obj
                    } else {
                        match row {
                            Json::Object(obj) => {
                                let mut obj = obj.clone();
                                obj.remove(by_key);
                                obj
                            }
                            _ => unimplemented!(),
                        }
                    };
                    let by_val_str = json_to_string(by_val).ok_or(Error::BadKey)?;
                    let entry = keyed_data
                        .entry(by_val_str)
                        .or_insert_with(|| Json::Array(Vec::new()));
                    json_push(entry, Json::from(obj));
                }
                None => continue,
            }
        }

        if let Some(key_aggs) = self.parse_key_aggregations()? {
            println!("key_aggs len = {:?}", key_aggs.len());
            if key_aggs.is_empty() {
                return Ok(Json::from(keyed_data));
            }
            let mut output = JsonObj::new();
            for key_agg in key_aggs {
                key_agg.aggregate(&keyed_data, &mut output)?;
            }
            Ok(Json::from(output))
        } else {
            Ok(Json::Object(keyed_data))
        }
    }

    fn parse_key_aggregations(&self) -> Result<Option<Vec<Box<dyn KeyedAggregate>>>, Error> {
        if let Some(selects) = &self.cmd.selects {
            let mut a: Vec<Box<dyn KeyedAggregate>> = Vec::new();
            match selects {
                Json::Object(obj) => {
                    for (col_name, cmd) in obj {
                        match cmd {
                            Json::Object(obj) => {
                                for (k, v) in obj {
                                    let key = json_string(v).ok_or(Error::BadSelect)?.to_string();
                                    let agg: Box<dyn KeyedAggregate> = match k.as_ref() {
                                        "count" => Box::new(Count::new(col_name, key)),
                                        "get" => continue,
                                        "first" => Box::new(KeyedFirst::new(col_name, key)),
                                        "last" => Box::new(KeyedLast::new(col_name, key)),
                                        "max" => Box::new(KeyedMax::new(col_name, key)),
                                        "min" => Box::new(KeyedMin::new(col_name, key)),
                                        _ => unimplemented!(),
                                    };
                                    a.push(agg);
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }
                }
                _ => unimplemented!(),
            }
            Ok(Some(a))
        } else {
            Ok(None)
        }
    }

    fn eval_obj_selects(&self, obj: JsonObj, rows: &[Json]) -> Result<Json, Error> {
        if obj.is_empty() {
            return self.eval_select_all(rows);
        }
        let mut cmds = Vec::new();
        for (key, val) in obj {
            let cmd = Cmd::parse_cmd(val).map_err(|_| Error::BadSelect)?;
            cmds.push((key, cmd));
        }
        if is_aggregation(&cmds) {
            let mut out = JsonObj::new();
            for (key, cmd) in cmds {
                if let Some(val) = eval_aggregate(cmd, rows)? {
                    out.insert(key, val);
                }
            }
            Ok(Json::Object(out))
        } else {
            eval_nonaggregate(&cmds, rows)
        }
    }

    //TODO remove clones
    fn eval_selects(&self, selects: &[Json], rows: &[Json]) -> Result<Json, Error> {
        if selects.is_empty() {
            return self.eval_select_all(rows);
        }
        let mut output = Vec::new();
        let cmds = parse_selects_to_cmd(selects)?;
        for (_, row) in rows.iter().enumerate() {
            let mut map = JsonObj::new();
            for cmd in &cmds {
                eval_row_cmd(cmd, row, &mut map)?;
            }
            if !map.is_empty() {
                output.push(Json::from(map));
            }
        }
        Ok(Json::from(output))
    }

    fn eval_select_all(&self, rows: &[Json]) -> Result<Json, Error> {
        let mut output = Vec::new();
        for (i, row) in rows.iter().take(50).enumerate() {
            let row = add_row_id(row, i)?;
            output.push(row);
        }
        Ok(Json::from(output))
    }
}

fn eval_row_cmd(cmd: &Cmd, row: &Json, obj: &mut JsonObj) -> Result<(), Error> {
    match cmd {
        Cmd::Get(key) => {
            if let Some(val) = row.get(&key) {
                obj.insert(key.to_string(), val.clone());
            }
            Ok(())
        }
        _ => unimplemented!(),
    }
}

fn add_row_id(row: &Json, i: usize) -> Result<Json, Error> {
    match row {
        Json::Object(obj) => {
            let mut obj = obj.clone();
            if let None = obj.get("_id") {
                obj.insert("_id".to_string(), Json::from(i));
            }
            Ok(Json::from(obj))
        }
        _ => Err(Error::BadObject),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    fn insert<K: Into<String>>(db: &mut InMemDb, key: K, val: Json) {
        db.eval_write_cmd(Cmd::Insert(key.into(), val)).unwrap();
    }

    fn insert_data(db: &mut InMemDb) {
        insert(db, "t", all_data());
    }

    fn test_db() -> InMemDb {
        let mut db = InMemDb::new();
        insert_data(&mut db);
        db
    }

    fn query(json: Json) -> Result<Json, Error> {
        let db = test_db();
        let cmd = serde_json::from_value(json).unwrap();
        let qry = Query::from(&db, cmd);
        qry.eval()
    }

    #[test]
    fn select_all_query() {
        let qry = query(json!({"from": "t"}));
        assert_eq!(Ok(all_data()), qry);
    }

    fn all_data() -> Json {
        json!([
            {"_id": 0, "name": "james", "age": 35},
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
            {"_id": 2, "name": "misha", "age": 10},
            {"_id": 3, "name": "ania", "age": 20},
        ])
    }

    #[test]
    fn select_1_prop_query() {
        let qry = query(json!({"select": "name", "from": "t"}));
        let val = json!([
            {"name": "james"},
            {"name": "ania"},
            {"name": "misha"},
            {"name": "ania"},
        ]);
        assert_eq!(Ok(val), qry);
    }
    #[test]
    fn select_3_prop_query() {
        let qry = query(json!({"select": ["name", "age", "job"], "from": "t"}));
        let val = json!([
            {"name": "james", "age": 35},
            {"name": "ania", "age": 28, "job": "english teacher"},
            {"name": "misha", "age": 10},
            {"name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_eq_query() {
        let qry = query(json!({"from": "t", "where": {"==": ["name", "james"]}}));
        let val = json!([{"_id": 0, "name": "james", "age": 35}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_neq_query_ok() {
        let qry = query(json!({"from": "t", "where": {"!=": ["name", "james"]}}));
        let val = json!([
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
            {"_id": 2, "name": "misha", "age": 10},
            {"_id": 3, "name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_gt_query_ok() {
        let qry = query(json!({"from": "t", "where": {">": ["age", 20]}}));
        let val = json!([
            {"_id": 0, "name": "james", "age": 35},
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_lt_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<": ["age", 20]}}));
        let val = json!([{"_id": 2, "name": "misha", "age": 10}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_lte_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<=": ["age", 28]}}));
        let val = json!([
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
            {"_id": 2, "name": "misha", "age": 10},
            {"_id": 3, "name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_gte_query_ok() {
        let qry = query(json!({"from": "t", "where": {">=": ["age", 28]}}));
        let val = json!([
            {"_id": 0, "name": "james", "age": 35},
            {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"}
        ]);

        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_and_gate_ok() {
        let qry = query(json!({
            "from": "t",
            "where": {"&&": [
                {">": ["age", 20]},
                {"==": ["name", "ania"]}
            ]}
        }));
        let val = json!([{"_id": 1, "name": "ania", "age": 28, "job": "english teacher"}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_sum_ok() {
        let qry = query(json!({
            "select": {"totalAge": {"sum": "age"}},
            "from": "t"
        }));
        let val = json!({"totalAge": 93});
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_max_num_ok() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": "age"}
            },
            "from": "t"
        }));
        let val = json!({"maxAge": 35});
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_max_str_ok() {
        let qry = query(json!({
            "select": {
                "maxName": {"max": "name"}
            },
            "from": "t"
        }));
        let val = json!({"maxName": "misha"});
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_min_num_ok() {
        let qry = query(json!({
            "select": {
                "minAge": {"min": "age"}
            },
            "from": "t"
        }));
        let val = json!({"minAge": 10});
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_avg_num_ok() {
        let qry = query(json!({
            "select": {
                "avgAge": {"avg": "age"}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"avgAge": 23.25})), qry);
    }

    #[test]
    fn select_first_ok() {
        let qry = query(json!({
            "select": {
                "firstAge": {"first": "age"}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"firstAge": 35})), qry);
    }

    #[test]
    fn select_last_ok() {
        let qry = query(json!({
            "select": {
                "lastAge": {"last": "age"}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"lastAge": 20})), qry);
    }

    #[test]
    fn select_var_num_ok() {
        let qry = query(json!({
            "select": {
                "varAge": {"var": "age"}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"varAge": 115.58333333333333})), qry);
    }

    #[test]
    fn select_dev_num_ok() {
        let qry = query(json!({
            "select": {
                "devAge": {"dev": "age"}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"devAge": 10.750968948580091})), qry);
    }

    #[test]
    fn select_max_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": "age"}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!({"maxAge": 35})), qry);
    }

    #[test]
    fn select_get_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"get": "age"}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!([{"_id": 0,"age":35}, {"_id": 1, "age": 28}])), qry);
    }

    #[test]
    fn select_min_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "minAge": {"min": "age"}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!({"minAge": 28})), qry);
    }

    #[test]
    fn select_min_max_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "youngestAge": {"min": "age"},
                "oldestAge": {"max": "age"}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!({"youngestAge": 28, "oldestAge": 35})), qry);
    }

    #[test]
    fn select_empty_obj() {
        let qry = query(json!({
            "select": {},
            "from": "t",
        }));
        assert_eq!(
            Ok(json!([
                {"_id": 0, "name": "james", "age": 35},
                {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
                {"_id": 2, "name": "misha", "age": 10},
                {"_id": 3, "name": "ania", "age": 20},
            ])),
            qry
        );
    }

    #[test]
    fn select_empty_obj_where_age_gt_20() {
        let qry = query(json!({
            "select": {},
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!([
                {"_id": 0, "name": "james", "age": 35},
                {"_id": 1, "name": "ania", "age": 28, "job": "english teacher"},
            ])),
            qry
        );
    }

    #[test]
    fn select_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"get": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": [{"age": 10}],
                "ania": [{"age": 28}, {"age": 20}],
                "james": [{"age": 35}],
            })),
            qry
        );
    }

    #[test]
    fn select_first_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"first": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 10},
                "ania": {"age": 28},
                "james": {"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_last_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"last": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 10},
                "ania": {"age": 20},
                "james": {"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_count_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"count": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 1},
                "ania": {"age": 2},
                "james": {"age": 1},
            })),
            qry
        );
    }

    #[test]
    fn select_min_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"min": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 10},
                "ania": {"age": 20},
                "james": {"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_max_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"max": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": 10},
                "ania": {"age": 28},
                "james": {"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_name_by_age() {
        let qry = query(json!({
            "select": {
                "name": {"get": "name"},
            },
            "from": "t",
            "by": "age",
        }));
        assert_eq!(
            Ok(json!({
                "10": [{"name": "misha"}],
                "20": [{"name": "ania"}],
                "28": [{"name": "ania"}],
                "35": [{"name": "james"}],
            })),
            qry
        );
    }

    #[test]
    fn select_age_by_name_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"get": "age"},
            },
            "from": "t",
            "by": "name",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({"ania": [{"age":28}], "james": [{"age": 35}]})),
            qry
        );
    }

    #[test]
    fn select_age_job_by_name_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"get": "age"},
                "job": {"get": "job"},
            },
            "from": "t",
            "by": "name",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({
                "ania": [{"age": 28, "job": "english teacher"}],
                "james": [{"age": 35}],
            })),
            qry
        );
    }

    #[test]
    fn select_all_by_name_where_age_gt_20() {
        let qry = query(json!({
            "from": "t",
            "by": "name",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({
                "ania": [{"_id": 1, "age": 28, "job": "english teacher"}],
                "james": [{"_id": 0, "age": 35}],
            })),
            qry
        );
    }

    #[test]
    fn select_count_max_min_age_by_name() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": "age"},
                "minAge": {"min": "age"},
                "countAge": {"count": "age"},
            },
            "from": "t",
            "by": "name",
        }));
        assert_eq!(
            Ok(json!({
                "ania": {"maxAge": 28, "countAge": 2, "minAge": 20},
                "james": {"maxAge": 35, "countAge": 1, "minAge": 35},
                "misha": {"maxAge": 10, "countAge": 1, "minAge": 10}
            })),
            qry
        );
    }
}
