use crate::err::Error;
use crate::json::json_num_gt;
use crate::json::{json_add, Json, JsonNum, JsonObj};
use crate::json::{json_add_nums, json_num_lt};
use crate::query::Select;

pub trait Aggregate {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error>;
    fn aggregate(&self) -> Json;
    fn name(&self) -> &str;
}

#[derive(Debug, Default)]
pub struct Sum {
    meta: Select,
    total: Option<Json>,
}

impl Sum {
    pub fn new<K: Into<String>>(name: K, key: K) -> Self {
        Self {
            meta: Select::from(name, key),
            total: None,
        }
    }
}

impl Aggregate for Sum {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error> {
        let val = if let Some(v) = row.get(&self.meta.key) {
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
        &self.meta.name
    }
}

#[derive(Debug, Default)]
pub struct Max {
    key: Select,
    max: Option<Json>,
}

impl Max {
    pub fn new(name: String, key: String) -> Self {
        Self {
            key: Select::from(name, key),
            max: None,
        }
    }
}

impl Aggregate for Max {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error> {
        let val = row.get(&self.key.key).ok_or(Error::BadSelect)?;
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
                    if json_num_gt(y, x) {
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
        self.max = Some(max.clone());
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
        self.key.name.as_ref()
    }
}

#[derive(Debug)]
pub struct Min {
    key: String,
    name: String,
    min: Option<Json>,
}

impl Min {
    pub fn new<K: Into<String>>(name: K, key: K) -> Self {
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

#[derive(Debug)]
pub struct Avg {
    key: String,
    name: String,
    count: usize,
    total: JsonNum,
}

impl Avg {
    pub fn new<K: Into<String>, N: Into<String>>(name: N, key: K) -> Self {
        Self {
            key: key.into(),
            name: name.into(),
            total: JsonNum::from(0),
            count: 0,
        }
    }
}

impl Aggregate for Avg {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error> {
        if let Some(val) = row.get(&self.key) {
            self.count += 1;
            let num = match val {
                Json::Number(num) => num,
                _ => return Ok(()),
            };
            self.total = json_add_nums(&self.total, num);
        }
        Ok(())
    }

    fn aggregate(&self) -> Json {
        Json::from(self.total.as_f64().unwrap() / (self.count as f64))
    }

    fn name(&self) -> &str {
        self.name.as_ref()
    }
}

#[derive(Debug, Default)]
pub struct Last {
    meta: Select,
    val: Option<Json>,
}

impl Last {
    pub fn new<S: Into<String>>(name: S, key: S) -> Self {
        Self {
            meta: Select::from(name, key),
            val: None,
        }
    }
}

impl Aggregate for Last {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error> {
        if let Some(val) = row.get(&self.meta.key) {
            self.val = Some(val.clone());
        }
        Ok(())
    }

    fn aggregate(&self) -> Json {
        self.val.clone().unwrap_or(Json::Null)
    }

    fn name(&self) -> &str {
        self.meta.name.as_str()
    }
}

pub struct Count {
    meta: Select,
    count: usize,
}

impl Count {
    pub fn new<S: Into<String>>(name: S, key: S) -> Self {
        Count {
            meta: Select::from(name, key),
            count: 0,
        }
    }
}

impl Aggregate for Count {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error> {
        if let Some(_) = row.get(self.meta.key.as_str()) {
            self.count += 1;
        }
        Ok(())
    }

    fn aggregate(&self) -> Json {
        Json::from(self.count)
    }

    fn name(&self) -> &str {
        self.meta.name.as_str()
    }
}

#[derive(Debug, Default)]
pub struct First {
    meta: Select,
    val: Option<Json>,
}

impl First {
    pub fn new<S: Into<String>>(name: S, key: S) -> Self {
        Self {
            meta: Select::from(name, key),
            val: None,
        }
    }
}

impl Aggregate for First {
    fn push(&mut self, row: &JsonObj) -> Result<(), Error> {
        if self.val.is_none() {
            if let Some(val) = row.get(&self.meta.key) {
                self.val = Some(val.clone());
            }
        }
        Ok(())
    }

    fn aggregate(&self) -> Json {
        self.val.clone().unwrap_or(Json::Null)
    }

    fn name(&self) -> &str {
        self.meta.name.as_str()
    }
}
