use crate::aggregate::*;
use crate::cmd::{Cmd};
use crate::err::Error;

use crate::json::*;

use serde_json::{Value as Json};

pub fn parse_aggregator(cmd: &(String, Cmd)) -> Result<Box<dyn Aggregate>, Error> {
    let name = cmd.0.to_string();
    match cmd.1.clone() {
        Cmd::Sum(arg) => {
            match *arg {
                
                Cmd::Key(key) => {
                    println!("returning sum agg");
                    Ok(Box::new(Sum::new(name, key.to_string())))
                }
                _ => unimplemented!(),
            }
        }
        Cmd::Max(arg) => {
            match *arg {
                Cmd::Key(key) => {
                    println!("returning max agg");
                    Ok(Box::new(Max::new(name, key.to_string())))
                }
                _ => unimplemented!(),
            }
        }
        Cmd::Min(arg) => {
            match *arg {
                Cmd::Key(key) => {
                    println!("returning min agg");
                    Ok(Box::new(Min::new(name, key.to_string())))
                }
                _ => unimplemented!(),
            }
        }
        _ => unimplemented!(),
    }
}

pub fn parse_aggregators(cmds: &[(String,Cmd)]) -> Result<Vec<Box<dyn Aggregate>>, Error> {
    let mut aggs = Vec::new();
    for cmd in cmds.iter() {
        let cmd = parse_aggregator(cmd)?;
        aggs.push(cmd);
    }
    Ok(aggs)
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
                                let is_max = json_gt(val, max).map_err(|_| Error::BadType)?;
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
            Ok(max.cloned())
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
            Ok(min.cloned())
        }
        val => Ok(Some(val.clone())),
    }
}

impl KeyedAggregate for KeyedMax {
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error> {
        for (k, v) in input {
            let val = key_obj_max(&self.key, v).map_err(|err| {
                eprintln!("{:?}", err);
                Error::BadType
            })?;
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
            let val = key_obj_min(&self.key, v).map_err(|err| {
                eprintln!("{:?}", err);
                Error::BadType
            })?;
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



/*
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
        self.val.cloned()
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
        self.val.cloned()
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
        self.max.cloned()
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
        val -= self.mean;
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
*/

/*
fn eval_agg<'a, A: 'a>(arg: Box<Cmd>, rows: &'a [Json], mut agg: A) -> Result<Option<Json>, Error>
where
    A: Aggregate<'a>,
{
    let key = match *arg {
        Cmd::Key(key) => key,
        cmd => {
            eprintln!("{:?}", cmd);
            unimplemented!()
        }
    };
    for row in rows {
        if let Some(val) = row.get(&key) {
            agg.aggregate(val)?;
        }
    }
    Ok(agg.apply())
}
*/

