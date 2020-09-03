use crate::aggregate::*;
use crate::cmd::Cmd;
use crate::err::Error;

use crate::json::*;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Select {
    pub name: String,
    pub key: String,
}

impl Select {
    pub fn from<S: Into<String>>(name: S, key: S) -> Self {
        Self {
            name: name.into(),
            key: key.into(),
        }
    }
}

pub fn parse_aggregator(select: (&String, &Cmd)) -> Result<Box<dyn Aggregate>, Error> {
    let name = select.0.to_string();
    match select.1.clone() {
        Cmd::Sum(arg) => parse_arg(name, arg, |x, y| Box::new(Sum::new(x, y))),
        Cmd::Max(arg) => parse_arg(name, arg, |x, y| Box::new(Max::new(x, y))),
        Cmd::Min(arg) => parse_arg(name, arg, |x, y| Box::new(Min::new(x, y))),
        Cmd::Avg(arg) => parse_arg(name, arg, |x, y| Box::new(Avg::new(x, y))),
        Cmd::Last(arg) => parse_arg(name, arg, |x, y| Box::new(Last::new(x, y))),
        Cmd::First(arg) => parse_arg(name, arg, |x, y| Box::new(First::new(x, y))),
        Cmd::Count(arg) => parse_arg(name, arg, |x, y| Box::new(Count::new(x, y))),
        _ => unimplemented!(),
    }
}

fn parse_arg<F>(name: String, arg: Box<Cmd>, f: F) -> Result<Box<dyn Aggregate>, Error>
where
    F: FnOnce(String, String) -> Box<dyn Aggregate>,
{
    match *arg {
        Cmd::Key(key) => {
            println!("returning max agg");
            Ok(f(name, key.to_string()))
        }
        _ => unimplemented!(),
    }
}

pub fn parse_aggregators(selects: &HashMap<String, Cmd>) -> Result<Vec<Box<dyn Aggregate>>, Error> {
    let mut aggs = Vec::new();
    for select in selects.iter() {
        let cmd = parse_aggregator(select)?;
        aggs.push(cmd);
    }
    Ok(aggs)
}

fn eval_keyed_cmd(data: &JsonObj, cmd: &Cmd, out: &mut JsonObj) -> Result<(), Error> {
    match cmd {
        Cmd::Max(arg) => match &**arg {
            Cmd::Key(ref key) => {
                for (by_key, val) in data.iter() {
                    match val {
                        Json::Object(obj) => {
                            if let Some(y) = obj.get(key) {
                                let mut max_val: Option<Json> = None;
                                let v = if let Some(x) = max_val {
                                    if json_gt(&x, y) {
                                        x
                                    } else {
                                        y.clone()
                                    }
                                } else {
                                    y.clone()
                                };
                                max_val = Some(v);
                                let entry =
                                    out.entry(by_key.to_string()).or_insert_with(|| json!({}));
                                match entry {
                                    Json::Object(obj) => {
                                        obj.insert(
                                            key.to_string(),
                                            if let Some(val) = max_val {
                                                val
                                            } else {
                                                Json::Null
                                            },
                                        );
                                    }
                                    _ => todo!(),
                                }
                            }
                        }
                        _ => continue,
                    }
                }
                Ok(())
            }
            arg => unimplemented!(),
        },
        Cmd::Min(arg) => match **arg {
            Cmd::Key(_) => unimplemented!(),
            _ => unimplemented!(),
        },
        _ => todo!(),
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
