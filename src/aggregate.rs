use crate::json::json_num_lt;
use crate::json::json_num_gt;
use crate::err::Error;
use crate::json::{json_add, Json,JsonObj};

pub trait Aggregate {
    fn push(&mut self, val: &JsonObj) -> Result<(), Error>;
    fn aggregate(&self) -> Json;
    fn name(&self) -> &str;
}

#[derive(Debug, Default)]
pub struct Sum {
    name: String,
    key: String,
    total: Option<Json>,
}

impl Sum {
    pub fn new<K:Into<String>>(name: K, key: K) -> Self {
        Self { name: name.into(), key: key.into(), total: None }
    }
}

pub fn get<'a>(key: &str, row: &'a JsonObj) -> Option<&'a Json> {
    unimplemented!()
}

impl Aggregate for Sum {
    fn push(&mut self, val: &JsonObj) -> Result<(), Error> { 
        let val = if let Some(v) = val.get(&self.key) {
            v
        } else {
            return Err(Error::BadSelect);
        };
        self.total = Some(if let Some(ref total) = self.total {
            json_add(total, val)?
        } else {
            json_add(&Json::from(0), val)?
        });
        Ok(())
    }

    fn aggregate(&self) -> Json { 
        match &self.total {
            Some(val) => val.clone(),
            None => Json::Null,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Default)]
pub struct Max {
    name: String,
    key: String,    
    max: Option<Json>,
}

impl Max {
    pub fn new(name: String, key: String) -> Self {
        Self {
            name,
            key,
            max: None,
        }
    }

    fn update(&mut self, val: Json) {
        self.max = Some(val);
    }
}

impl Aggregate for Max {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error> {
        let val = row.get(&self.key).ok_or(Error::BadSelect)?;
        let max = match self.max {
            Some(ref max) => match (max, val) {
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
        self.update(max.clone());
        Ok(())
    }

    fn aggregate(&self) -> Json {
        if let Some(val) = &self.max {
            val.clone()
        } else {
            Json::Null
        }
    }

    fn name(&self) -> &str {
        self.name.as_ref()
    }
}


#[derive(Debug)]
pub struct Min {
    key: String, 
    name: String,
    min: Option<Json>,
}

impl Min {
    pub fn new<K:Into<String>>(name: K, key: K) -> Self {
        Self {
            key: key.into(),
            name: name.into(),
            min: None,
        }
    }
}

impl Aggregate for Min {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error> {
        let val = if let Some(val) = row.get(&self.key) {
            val
        } else {
            return Ok(());
        };
        self.min = match (&self.min, val) {
            (None, val) => Some(val.clone()),
            (Some(Json::String(y)), Json::String(x)) => {
                if x < y {
                    Some(val.clone())
                } else {
                    self.min.clone()
                }
            }
            (Some(Json::String(_)), _) => return Err(Error::BadType),
            (Some(Json::Number(x)), Json::Number(y)) => {
                let r = json_num_lt(x, y).ok_or(Error::BadType)?;
                if r {
                    self.min.clone()
                } else {
                    Some(val.clone())
                }
            }
            _ => unimplemented!(),
        };
        Ok(())
    }

    fn aggregate(&self) -> Json {
        match &self.min {
            Some(val) => val.clone(),
            None => Json::Null,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub trait KeyedAggregate {
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error>;
}

pub struct KeyedGet {
    keys: Vec<String>,
}

impl KeyedGet {
    pub fn from(keys: Vec<String>) -> KeyedGet {
        KeyedGet { keys }
    }
}

impl KeyedAggregate for KeyedGet {
    fn aggregate(&self, input: &JsonObj, output: &mut JsonObj) -> Result<(), Error> {
        for key in self.keys.iter() {
            if let Some(val) = input.get(key) {
                let entry = output.entry(key.to_string()).or_insert_with(|| Json::Array(Vec::new()));
                match entry {
                    Json::Array(ref mut arr) => arr.push(val.clone()),
                    _ => unimplemented!(),
                }
            }
        }
        Ok(())
    }
}