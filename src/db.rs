use crate::cmd::Cmd;
use crate::cmd::Filter;
use crate::cmd::QueryCmd;
use crate::err::Error;
use crate::json::*;
use rayon::prelude::*;
use serde_json::{Value as Json, Value};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct InMemDb {
    cache: Map<String, Json>,
}

impl InMemDb {
    fn eval_read_bin_cmd<F>(&self, lhs: Cmd, rhs: Cmd, f: F) -> Result<Json, Error>
    where
        F: FnOnce(&Json, &Json) -> Result<Json, Error>,
    {
        let x = self.eval_read(lhs)?;
        let y = self.eval_read(rhs)?;
        f(&x, &y)
    }

    fn eval_read_unr_cmd<F>(&self, arg: Cmd, f: F) -> Result<Json, Error>
    where
        F: FnOnce(&Json) -> Result<Json, Error>,
    {
        let val = self.eval_read(arg)?;
        f(&val)
    }

    fn eval_sort_cmd(&self, arg: Cmd) -> Result<Json, Error> {
        let mut val = self.eval_read(arg)?;
        json_sort_ascend(&mut val);
        Ok(val)
    }

    pub fn eval_read(&self, cmd: Cmd) -> Result<Json, Error> {
        match cmd {
            Cmd::Append(_, _)
            | Cmd::Delete(_)
            | Cmd::Set(_, _)
            | Cmd::Insert(_, _)
            | Cmd::Pop(_)
            | Cmd::Push(_, _) => Err(Error::BadCmd),
            Cmd::Add(lhs, rhs) => self.eval_read_bin_cmd(*lhs, *rhs, json_add),
            Cmd::Avg(arg) => self.eval_read_unr_cmd(*arg, json_avg),
            Cmd::Bar(lhs, rhs) => self.eval_read_bin_cmd(*lhs, *rhs, json_bar),
            Cmd::Len(arg) => self.eval_read_unr_cmd(*arg, |x| Ok(json_count(x))),
            Cmd::Div(lhs, rhs) => self.eval_read_bin_cmd(*lhs, *rhs, json_div),
            Cmd::First(arg) => self.eval_read_unr_cmd(*arg, |x| Ok(json_first(x))),
            Cmd::Get(key, arg) => {
                let val = self.eval_read(*arg)?;
                json_get(&key, &val).ok_or(Error::BadKey)
            }
            Cmd::Json(val) => Ok(val),
            Cmd::Key(key) => self.eval_key(&key),
            //Cmd::Keys(_) => Ok(self.keys()),
            Cmd::Last(arg) => self.eval_read_unr_cmd(*arg, |x| Ok(json_last(x))),
            Cmd::Max(arg) => self.eval_read_unr_cmd(*arg, |x| Ok(json_max(x).clone())),
            Cmd::Min(arg) => self.eval_read_unr_cmd(*arg, |x| Ok(json_min(x).clone())),
            Cmd::Mul(lhs, rhs) => self.eval_read_bin_cmd(*lhs, *rhs, json_mul),
            Cmd::StdDev(arg) => self.eval_read_unr_cmd(*arg, json_dev),
            Cmd::Sub(lhs, rhs) => self.eval_read_bin_cmd(*lhs, *rhs, json_sub),
            Cmd::Sum(arg) => self.eval_read_unr_cmd(*arg, |x| Ok(json_sum(x))),
            //Cmd::Summary => Ok(self.summary()),
            Cmd::Sort(arg, _) => self.eval_sort_cmd(*arg),
            Cmd::ToString(arg) => {
                self.eval_read_unr_cmd(*arg, |x| Ok(Json::from(json_tostring(x))))
            }
            Cmd::Unique(arg) => self.eval_read_unr_cmd(*arg, |x| Ok(json_unique(x))),
            Cmd::Var(arg) => self.eval_read_unr_cmd(*arg, json_var),
            Cmd::Query(cmd) => {
                let query = Query::from(self, cmd);
                query.exec()
            }
            Cmd::Reverse(arg) => {
                let mut val = self.eval_read(*arg)?;
                json_reverse(&mut val);
                Ok(val)
            }
            Cmd::SortBy(arg, key) => {
                let mut val = self.eval_read(*arg)?;
                json_sortby(&mut val, &key);
                Ok(val)
            }
            Cmd::Median(arg) => {
                let mut val = self.eval_read(*arg)?;
                json_median(&mut val)
            }
            Cmd::Keys(page_no) => Ok(self.eval_keys(page_no)),
            Cmd::Summary => Ok(self.summary()),
            Cmd::Eval(cmds) => {
                let out: Result<Vec<Json>, Error> =
                    cmds.into_iter().map(|cmd| self.eval_read(cmd)).collect();
                out.map(Json::Array)
            }
        }
    }

    fn eval_keys(&self, page_no: Option<usize>) -> Json {
        let page_no = page_no.unwrap_or(0);
        let page_size = 50;
        let start = page_no * page_size;
        let vec = self
            .cache
            .keys()
            .skip(start)
            .take(50)
            .map(|x| Json::from(x.clone()))
            .collect();
        Json::Array(vec)
    }

    fn key(&self, key: &str) -> Result<Json, Error> {
        self.cache.get(key).cloned().ok_or(Error::BadKey)
    }

    pub fn eval(&mut self, cmd: Cmd) -> Result<Json, Error> {
        match cmd {
            Cmd::Add(lhs, rhs) => self.eval_bin_fn(*lhs, *rhs, json_add),
            Cmd::Append(key, arg) => {
                let val = self.eval(*arg)?;
                self.append(key, val)
            }
            Cmd::Avg(arg) => self.eval_unr_fn(*arg, json_avg),
            Cmd::Bar(lhs, rhs) => self.eval_bin_fn(*lhs, *rhs, json_bar),
            Cmd::Len(arg) => self.eval_unr_fn(*arg, &count),
            Cmd::Delete(key) => self.delete(&key),
            Cmd::Div(lhs, rhs) => self.eval_bin_fn(*lhs, *rhs, json_div),
            Cmd::First(arg) => self.eval_unr_fn(*arg, |x| Ok(json_first(x))),
            Cmd::Get(key, arg) => {
                let val = self.eval(*arg)?;
                Ok(json_get(&key, &val).unwrap_or(Json::Null))
            }
            Cmd::Insert(key, arg) => {
                let val = self.get_mut(&key)?;
                let n = arg.len();
                json_insert(val, arg);
                Ok(Json::from(n))
            }
            Cmd::Json(val) => Ok(val),
            Cmd::Keys(_page) => Ok(self.keys()),
            Cmd::Last(arg) => self.eval_unr_fn(*arg, |x| Ok(json_last(x))),
            Cmd::Max(arg) => self.eval_unr_fn_ref(*arg, json_max),
            Cmd::Min(arg) => self.eval_unr_fn_ref(*arg, json_min),
            Cmd::Mul(lhs, rhs) => self.eval_bin_fn(*lhs, *rhs, json_mul),
            Cmd::Push(key, arg) => {
                let val = self.eval(*arg)?;
                let kv = self.cache.get_mut(&key).ok_or(Error::BadKey)?;
                json_push(kv, val);
                Ok(Json::Null)
            }
            Cmd::Pop(key) => self.pop(&key).map(|x| x.unwrap_or(Json::Null)),
            Cmd::Query(cmd) => self.query(cmd),
            Cmd::Set(key, arg) => {
                let val = self.eval(*arg)?;
                Ok(self.set(key, val).unwrap_or(Json::Null))
            }
            Cmd::Sort(arg, _) => self.eval_sort_cmd(*arg),
            Cmd::StdDev(arg) => self.eval_unr_fn(*arg, json_dev),
            Cmd::Sub(lhs, rhs) => self.eval_bin_fn(*lhs, *rhs, json_sub),
            Cmd::Sum(arg) => self.eval_unr_fn(*arg, |x| Ok(json_sum(x))),
            Cmd::Summary => Ok(self.summary()),
            Cmd::Unique(arg) => self.eval_unr_fn(*arg, unique),
            Cmd::Var(arg) => self.eval_unr_fn(*arg, json_var),
            Cmd::ToString(arg) => {
                let val = self.eval(*arg)?;
                Ok(json_string(&val))
            }
            Cmd::Key(key) => self.eval_key(&key),
            Cmd::Reverse(arg) => {
                let mut val = self.eval(*arg)?;
                json_reverse(&mut val);
                Ok(val)
            }
            Cmd::Median(arg) => {
                let mut val = self.eval(*arg)?;
                json_median(&mut val)
            }
            Cmd::SortBy(_, _) => unimplemented!(),
            Cmd::Eval(_) => unimplemented!(),
        }
    }

    pub fn eval_key(&self, key: &str) -> Result<Json, Error> {
        let mut it = key.split('.');
        let key = it.next().ok_or(Error::BadKey)?;
        let mut val = self.key(key)?;
        for key in it {
            if let Some(v) = json_get(key, &val) {
                val = v;
            } else {
                return Err(Error::BadKey);
            }
        }
        Ok(val)
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn entry<K: Into<String>>(&mut self, key: K) -> &mut Value {
        self.cache.entry(key.into()).or_insert_with(|| Json::Null)
    }

    pub fn set<K: Into<String>>(&mut self, key: K, val: Json) -> Option<Json> {
        self.cache.insert(key.into(), val)
    }

    fn keys(&self) -> Json {
        let arr = self
            .cache
            .keys()
            .map(|x| Json::from(x.to_string()))
            .collect();
        Json::Array(arr)
    }

    pub fn summary(&self) -> Json {
        let no_entries = Json::from(self.cache.len());
        let keys: Vec<Json> = self
            .cache
            .keys()
            .map(|x| Json::String(x.to_string()))
            .collect();
        json!({"no_entries": no_entries, "keys": keys})
    }

    pub fn pop(&mut self, key: &str) -> Result<Option<Json>, Error> {
        let val = self.get_mut(key)?;
        json_pop(val)
    }

    pub fn query(&self, cmd: QueryCmd) -> Result<Json, Error> {
        let qry = Query::from(self, cmd);
        qry.exec()
    }

    pub fn get_mut(&mut self, key: &str) -> Result<&mut Json, Error> {
        self.cache.get_mut(key).ok_or(Error::BadKey)
    }

    pub fn delete(&mut self, key: &str) -> Result<Json, Error> {
        Ok(if let Some(val) = self.cache.remove(key) {
            val
        } else {
            Json::Null
        })
    }

    fn append(&mut self, key: String, val: Json) -> Result<Json, Error> {
        let entry = self.entry(key);
        json_append(entry, val);
        Ok(Json::Null)
    }

    fn eval_bin_fn<F>(&mut self, lhs: Cmd, rhs: Cmd, f: F) -> Result<Json, Error>
    where
        F: Fn(&Json, &Json) -> Result<Json, Error>,
    {
        let lhs = self.eval(lhs)?;
        let rhs = self.eval(rhs)?;
        f(&lhs, &rhs)
    }

    fn eval_unr_fn<F>(&mut self, arg: Cmd, f: F) -> Result<Json, Error>
    where
        F: Fn(&Json) -> Result<Json, Error>,
    {
        let val = self.eval(arg)?;
        f(&val)
    }

    fn eval_unr_fn_ref<F>(&mut self, arg: Cmd, f: F) -> Result<Json, Error>
    where
        F: Fn(&Json) -> &Json,
    {
        let val = self.eval(arg)?;
        Ok(f(&val).clone())
    }
}

pub struct Query<'a> {
    db: &'a InMemDb,
    cmd: QueryCmd,
}

fn has_aggregation(selects: &HashMap<String, Cmd>) -> bool {
    for (_, cmd) in selects.iter() {
        if !cmd.is_aggregate() {
            return false;
        }
    }
    true
}

fn merge_grouping(
    mut x: HashMap<String, Vec<Json>>,
    y: HashMap<String, Vec<Json>>,
) -> HashMap<String, Vec<Json>> {
    for (key, val) in y {
        let vals = x.entry(key).or_insert_with(|| Vec::new());
        vals.extend(val);
    }
    x
}

impl<'a> Query<'a> {
    pub fn from(db: &'a InMemDb, cmd: QueryCmd) -> Self {
        Self { db, cmd }
    }

    pub fn exec(&self) -> Result<Json, Error> {
        let data = self.eval_from()?;
        let rows = data.as_array().ok_or(Error::ExpectedArr)?;
        if let Some(by) = &self.cmd.by {
            let rows = if let Some(filter) = &self.cmd.filter {
                self.eval_where(rows, filter.clone())?
            } else {
                rows.clone()
            };
            let grouping = self.eval_grouping(by, &rows);
            if let Some(selects) = &self.cmd.selects {
                self.eval_grouped_select(grouping, selects)
            } else {
                let mut obj = JsonObj::new();
                for (k, v) in grouping.into_iter() {
                    obj.insert(k, v.into_iter().map(Json::from).collect());
                }
                Ok(Json::from(obj))
            }
        } else if let Some(filter) = &self.cmd.filter {
            let rows = self.eval_where(rows, filter.clone())?;
            self.eval_select(&rows)
        } else {
            self.eval_select(rows)
        }
    }

    fn eval_grouped_select(
        &self,
        grouping: HashMap<String, Vec<Json>>,
        selects: &HashMap<String, Cmd>,
    ) -> Result<Json, Error> {
        let mut keyed_obj = JsonObj::new();
        for (key, keyed_rows) in grouping.into_iter() {
            let mut obj = JsonObj::new();
            for (col, cmd) in selects {
                if let Some(v) = eval_reduce_cmd(cmd, &keyed_rows) {
                    obj.insert(col.to_string(), v);
                }
            }
            keyed_obj.insert(key, Json::Object(obj));
        }
        Ok(Json::from(keyed_obj))
    }

    fn eval_grouping(&self, by: &Cmd, rows: &[Json]) -> HashMap<String, Vec<Json>> {
        match by {
            Cmd::Key(key) => {
                let g: HashMap<String, Vec<Value>> = rows
                    .par_iter()
                    .map(|row| (row, row.get(key)))
                    .filter(|(_, x)| x.is_some())
                    .map(|(row, x)| (row, x.unwrap()))
                    .fold(
                        || HashMap::new(),
                        |mut g, (row, val)| {
                            let entry: &mut Vec<Json> =
                                g.entry(json_str(val)).or_insert_with(Vec::new);
                            entry.push(row.clone());
                            g
                        },
                    )
                    .reduce(|| HashMap::new(), merge_grouping);
                g
            }
            Cmd::Append(_, _) => unimplemented!(),
            Cmd::Bar(_, _) => unimplemented!(),
            Cmd::Set(_, _) => unimplemented!(),
            Cmd::Max(_) => unimplemented!(),
            Cmd::Min(_) => unimplemented!(),
            Cmd::Avg(_) => unimplemented!(),
            Cmd::Delete(_) => unimplemented!(),
            Cmd::StdDev(_) => unimplemented!(),
            Cmd::Sum(_) => unimplemented!(),
            Cmd::Add(_, _) => unimplemented!(),
            Cmd::Sub(_, _) => unimplemented!(),
            Cmd::Mul(_, _) => unimplemented!(),
            Cmd::Div(_, _) => unimplemented!(),
            Cmd::First(_) => unimplemented!(),
            Cmd::Last(_) => unimplemented!(),
            Cmd::Var(_) => unimplemented!(),
            Cmd::Push(_, _) => unimplemented!(),
            Cmd::Pop(_) => unimplemented!(),
            Cmd::Query(_) => unimplemented!(),
            Cmd::Insert(_, _) => unimplemented!(),
            Cmd::Keys(_) => unimplemented!(),
            Cmd::Len(_) => unimplemented!(),
            Cmd::Unique(_) => unimplemented!(),
            Cmd::Json(_) => unimplemented!(),
            Cmd::Summary => unimplemented!(),
            Cmd::Get(_, _) => unimplemented!(),
            Cmd::ToString(_) => unimplemented!(),
            Cmd::Sort(_, _) => unimplemented!(),
            Cmd::Reverse(_) => unimplemented!(),
            Cmd::SortBy(_, _) => unimplemented!(),
            Cmd::Median(_) => unimplemented!(),
            Cmd::Eval(_) => unimplemented!(),
        }
    }

    fn eval_from(&self) -> Result<&Json, Error> {
        self.db.cache.get(&self.cmd.from).ok_or(Error::BadFrom)
    }

    fn eval_where(&self, rows: &[Json], filter: Filter) -> Result<Vec<Json>, Error> {
        let mut filtered_rows = Vec::new();
        for row in rows {
            let obj = json_obj_ref(row)?;
            if filter.apply(obj) {
                filtered_rows.push(Json::from(obj.clone()));
            }
        }
        Ok(filtered_rows)
    }

    // TODO remove cloning
    fn eval_select(&self, rows: &[Json]) -> Result<Json, Error> {
        match &self.cmd.selects {
            Some(selects) => self.eval_obj_selects(selects, rows),
            None => self.eval_select_all(rows),
        }
    }

    fn eval_obj_selects(
        &self,
        selects: &HashMap<String, Cmd>,
        rows: &[Json],
    ) -> Result<Json, Error> {
        if selects.is_empty() {
            return self.eval_select_all(rows);
        }
        //todo the cmds vec is not neccessary

        if has_aggregation(&selects) {
            let out = reduce(selects, rows);
            Ok(Json::Array(vec![Json::Object(out)]))
        } else {
            eval_nonaggregate(&selects, rows)
        }
    }

    fn eval_select_all(&self, rows: &[Json]) -> Result<Json, Error> {
        let mut output = Vec::new();
        for row in rows.iter().take(50).cloned() {
            output.push(row);
        }
        Ok(Json::from(output))
    }
}

fn eval_reduce_cmd(cmd: &Cmd, rows: &[Json]) -> Option<Json> {
    match cmd {
        Cmd::Append(_, _) => None,
        Cmd::Get(key, arg) => {
            if let Some(val) = eval_reduce_cmd(arg.as_ref(), rows) {
                json_get(key, &val)
            } else {
                None
            }
        }
        Cmd::Key(key) => {
            let mut output = Vec::new();
            for row in rows {
                if let Some(val) = json_get(key, row) {
                    output.push(val);
                }
            }
            if output.is_empty() {
                None
            } else {
                Some(Json::from(output))
            }
        }
        Cmd::First(arg) => eval_reduce_cmd(arg.as_ref(), rows).map(|x| json_first(&x)),
        Cmd::Last(arg) => eval_reduce_cmd(arg.as_ref(), rows).map(|x| json_last(&x)),
        Cmd::Sum(arg) => eval_reduce_cmd(arg.as_ref(), rows).map(|x| json_sum(&x)),
        Cmd::Len(arg) => eval_reduce_cmd(arg.as_ref(), rows).map(|x| json_count(&x)),
        Cmd::Min(arg) => eval_reduce_cmd(arg.as_ref(), rows).map(|x| json_min(&x).clone()),
        Cmd::Max(arg) => eval_reduce_cmd(arg.as_ref(), rows).map(|x| json_max(&x).clone()),
        Cmd::Avg(arg) => {
            let val = eval_reduce_cmd(arg.as_ref(), rows);
            if let Some(ref val) = val {
                json_avg(val).ok()
            } else {
                None
            }
        }
        Cmd::Var(arg) => {
            if let Some(ref arg) = eval_reduce_cmd(arg.as_ref(), rows) {
                json_var(arg).ok()
            } else {
                None
            }
        }
        Cmd::StdDev(arg) => {
            if let Some(ref arg) = eval_reduce_cmd(arg.as_ref(), rows) {
                json_dev(arg).ok()
            } else {
                None
            }
        }
        Cmd::Json(val) => Some(val.clone()),
        Cmd::Unique(arg) => eval_reduce_cmd(arg.as_ref(), rows).map(|x| json_unique(&x)),
        Cmd::Bar(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_bar),
        Cmd::Add(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_add),
        Cmd::Sub(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_sub),
        Cmd::Mul(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_mul),
        Cmd::Div(lhs, rhs) => eval_reduce_bin_cmd(lhs, rhs, rows, json_div),
        Cmd::Set(_, _) => None,
        Cmd::Delete(_) => None,
        Cmd::Pop(_) => None,
        Cmd::Query(_) => None,
        Cmd::Insert(_, _) => None,
        Cmd::Push(_, _) => None,
        Cmd::Keys(_) => None,
        Cmd::Summary => None,
        Cmd::ToString(arg) => {
            eval_reduce_cmd(arg.as_ref(), rows).map(|x| Json::String(x.to_string()))
        }
        Cmd::Sort(_, _) => unimplemented!(),
        Cmd::Median(_) => unimplemented!(),
        Cmd::SortBy(_, _) => unimplemented!(),
        Cmd::Reverse(_) => unimplemented!(),
        Cmd::Eval(_) => unimplemented!(),
    }
}

fn eval_reduce_bin_cmd<F>(lhs: &Cmd, rhs: &Cmd, rows: &[Json], f: F) -> Option<Json>
where
    F: FnOnce(&Json, &Json) -> Result<Json, Error>,
{
    let x = eval_reduce_cmd(lhs, rows)?;
    let y = eval_reduce_cmd(rhs, rows)?;
    Some(f(&x, &y).unwrap_or(Json::Null))
}

fn reduce_select(select: (&String, &Cmd), rows: &[Json], output: &mut JsonObj) {
    if let Some(val) = eval_reduce_cmd(select.1, rows) {
        output.insert(select.0.to_string(), val);
    }
}

fn reduce(selects: &HashMap<String, Cmd>, rows: &[Json]) -> JsonObj {
    let mut obj = JsonObj::new();
    for select in selects {
        reduce_select(select, rows, &mut obj);
    }
    obj
}

fn eval_row_bin_cmd<F>(lhs: &Cmd, rhs: &Cmd, row: &Json, f: F) -> Option<Json>
where
    F: Fn(&Json, &Json) -> Option<Json>,
{
    let x = eval_row_cmd(lhs, row)?;
    let y = eval_row_cmd(rhs, row)?;
    f(&x, &y)
}

fn eval_row_cmd(cmd: &Cmd, row: &Json) -> Option<Json> {
    match cmd {
        Cmd::Key(key) => row.get(key).cloned(),
        Cmd::Mul(lhs, rhs) => {
            eval_row_bin_cmd(lhs.as_ref(), rhs.as_ref(), row, |x, y| json_mul(x, y).ok())
        }
        Cmd::Append(_, _) => todo!("throw error instead of None"),
        Cmd::Bar(arg, interval) => {
            let val = eval_row_cmd(arg.as_ref(), row)?;
            let interval = eval_row_cmd(interval.as_ref(), row)?;
            json_bar(&val, &interval).ok()
        }
        Cmd::Set(_, _) => todo!("throw error instead of None"),
        Cmd::Max(_) => None,
        Cmd::Min(_) => None,
        Cmd::Avg(_) => None,
        Cmd::Delete(_) => todo!("throw error instead of None"),
        Cmd::StdDev(_) => None,
        Cmd::Sum(_) => None,
        Cmd::Add(lhs, rhs) => match (lhs.as_ref(), rhs.as_ref()) {
            (Cmd::Key(lhs), Cmd::Key(rhs)) => {
                if let (Some(x), Some(y)) = (row.get(lhs), row.get(rhs)) {
                    json_add(x, y).ok()
                } else {
                    None
                }
            }
            (lhs, rhs) => {
                let x = eval_row_cmd(lhs, row)?;
                let y = eval_row_cmd(rhs, row)?;
                json_add(&x, &y).ok()
            }
        },
        Cmd::Sub(_, _) => unimplemented!(),
        Cmd::Div(_, _) => unimplemented!(),
        Cmd::First(_) => None,
        Cmd::Last(_) => None,
        Cmd::Var(_) => None,
        Cmd::Push(_, _) => todo!("throw error instead of None"),
        Cmd::Pop(_) => todo!("throw error instead of None"),
        Cmd::Query(_) => todo!("throw error instead of None"),
        Cmd::Insert(_, _) => todo!("throw error instead of None"),
        Cmd::Keys(_) => todo!("throw error instead of None"),
        Cmd::Len(_) => None,
        Cmd::Unique(_) => None,
        Cmd::Json(val) => Some(val.clone()),
        Cmd::Summary => todo!("throw error instead of None"),
        Cmd::Get(key, arg) => {
            let val = eval_row_cmd(arg.as_ref(), row)?;
            val.get(key).cloned()
        }
        Cmd::ToString(arg) => eval_row_cmd(arg.as_ref(), row).map(|x| Json::String(json_str(&x))),
        Cmd::Sort(_, _) => None,
        Cmd::Reverse(_) => None,
        Cmd::SortBy(_, _) => None,
        Cmd::Median(_) => None,
        Cmd::Eval(_) => todo!("throw an error here"),
    }
}

fn eval_nonaggregate(selects: &HashMap<String, Cmd>, rows: &[Json]) -> Result<Json, Error> {
    let mut out = Vec::new();
    for row in rows {
        let mut obj = JsonObj::new();
        for (col_name, cmd) in selects {
            //eval_row(&mut obj, select, row)?;
            if let Some(val) = eval_row_cmd(cmd, row) {
                obj.insert(col_name.to_string(), val);
            }
        }
        if !obj.is_empty() {
            out.push(Json::from(obj));
        }
    }
    Ok(Json::Array(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_approx_eq::assert_approx_eq;

    use serde_json::json;

    fn set<K: Into<String>>(key: K, arg: Cmd) -> Cmd {
        Cmd::Set(key.into(), b(arg))
    }

    fn key<K: Into<String>>(k: K) -> Cmd {
        Cmd::Key(k.into())
    }

    fn b<T>(val: T) -> Box<T> {
        Box::new(val)
    }

    fn get<K: Into<String>>(k: K, arg: Cmd) -> Cmd {
        Cmd::Get(k.into(), b(arg))
    }

    fn avg(arg: Cmd) -> Cmd {
        Cmd::Avg(b(arg))
    }

    fn first(arg: Cmd) -> Cmd {
        Cmd::First(b(arg))
    }

    fn last(arg: Cmd) -> Cmd {
        Cmd::Last(b(arg))
    }

    fn max(arg: Cmd) -> Cmd {
        Cmd::Max(b(arg))
    }

    fn min(arg: Cmd) -> Cmd {
        Cmd::Min(b(arg))
    }

    fn dev(arg: Cmd) -> Cmd {
        Cmd::StdDev(b(arg))
    }

    fn var(arg: Cmd) -> Cmd {
        Cmd::Var(b(arg))
    }

    fn mul(x: Cmd, y: Cmd) -> Cmd {
        Cmd::Mul(b(x), b(y))
    }

    fn div(x: Cmd, y: Cmd) -> Cmd {
        Cmd::Div(b(x), b(y))
    }

    fn add(x: Cmd, y: Cmd) -> Cmd {
        Cmd::Add(b(x), b(y))
    }

    fn sub(x: Cmd, y: Cmd) -> Cmd {
        Cmd::Sub(b(x), b(y))
    }

    fn bad_type() -> Result<Json, Error> {
        Err(Error::BadType)
    }

    fn eval(cmd: Cmd) -> Result<Json, Error> {
        let mut db = InMemDb::new();
        insert_data(&mut db);
        db.eval(cmd)
    }

    fn insert_data(db: &mut InMemDb) {
        assert_eq!(None, db.set("a", json!([1, 2, 3, 4, 5])));
        assert_eq!(None, db.set("b", json!(true)));
        assert_eq!(None, db.set("i", json!(2)));
        assert_eq!(None, db.set("f", json!(3.3)));
        assert_eq!(None, db.set("ia", json!([1, 2, 3, 4, 5])));
        assert_eq!(None, db.set("fa", json!([1.1, 2.2, 3.3, 4.4, 5.5])));
        assert_eq!(None, db.set("x", json!(4)));
        assert_eq!(None, db.set("y", json!(5)));
        assert_eq!(None, db.set("s", json!("hello")));
        assert_eq!(None, db.set("sa", json!(["a", "b", "c", "d"])));
        assert_eq!(None, db.set("t", table_data()));
        assert_eq!(
            None,
            db.set(
                "orders",
                json!([
                    { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                    { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                    { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                    { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                    { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
                ])
            )
        );
    }

    #[test]
    fn select_all_from_orders() {
        let exp = json!([
            json!({ "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 }),
            json!({ "time": 1, "customer": "ania", "qty": 2, "price": 2.0 }),
            json!({ "time": 2, "customer": "misha", "qty": 4, "price": 1.0 }),
            json!({ "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 }),
            json!({ "time": 4, "customer": "james", "qty": 1, "price": 16.0 }),
        ]);
        assert_eq!(Ok(exp), query(json!({"from": "orders"})));
    }

    #[test]
    fn select_customer_from_orders() {
        let exp = json!([
            json!({ "name": "james" }),
            json!({ "name": "ania" }),
            json!({ "name": "misha" }),
            json!({ "name": "james" }),
            json!({ "name": "james" }),
        ]);
        assert_eq!(
            Ok(exp),
            query(json!({
                "select": {"name":{"key":"customer"}},
                "from": "orders"
            }))
        );
    }

    #[test]
    fn select_customer_qty_from_orders() {
        let exp = json!([
            json!({ "name": "james", "quantity":2 }),
            json!({ "name": "ania", "quantity":2 }),
            json!({ "name": "misha", "quantity":4 }),
            json!({ "name": "james","quantity":10 }),
            json!({ "name": "james", "quantity":1 }),
        ]);
        assert_eq!(
            Ok(exp),
            query(json!({
                "select": {
                    "name":{"key":"customer"},
                    "quantity":{"key": "qty"},
                },
                "from": "orders"
            }))
        );
    }

    #[test]
    fn select_customer_qty_discount_from_orders() {
        let exp = json!([
            json!({ "name": "james", "quantity":2, "discount":10}),
            json!({ "name": "ania", "quantity":2 }),
            json!({ "name": "misha", "quantity":4 }),
            json!({ "name": "james","quantity":10, "discount": 20}),
            json!({ "name": "james", "quantity":1 }),
        ]);
        assert_eq!(
            Ok(exp),
            query(json!({
                "select": {
                    "name":{"key":"customer"},
                    "quantity":{"key": "qty"},
                    "discount":{"key": "discount"}
                },
                "from": "orders"
            }))
        );
    }

    #[test]
    fn select_unique_customer_from_orders() {
        let exp = json!([
            { "uniqueNames": ["james", "ania", "misha"]},
        ]);
        assert_eq!(
            Ok(exp),
            query(json!({
                "select": {
                    "uniqueNames":{"unique": {"key":"customer"}},
                },
                "from": "orders"
            }))
        );
    }

    #[test]
    fn select_count_discount_count_quantity_from_orders() {
        let exp = json!([
            { "countDiscount": 2, "countCustomer": 5}
        ]);
        assert_eq!(
            Ok(exp),
            query(json!({
                "select": {
                    "countDiscount":{ "len": {"key": "discount"} },
                    "countCustomer":{ "len": {"key": "customer"} },
                },
                "from": "orders"
            }))
        );
    }

    #[test]
    fn select_mul_price_quantity_from_orders() {
        let exp = json!([
            { "value": 18.0 },
            { "value": 4.0 },
            { "value": 4.0 },
            { "value": 160.0 },
            { "value": 16.0 },
        ]);
        assert_eq!(
            Ok(exp),
            query(json!({
                "select": {
                    "value":{ "*": [{"key": "qty"}, {"key":"price"}] },
                },
                "from": "orders"
            }))
        );
    }

    #[test]
    fn select_sum_mul_price_quantity_from_orders() {
        let exp = json!([
            { "totalValue": 202.0 }
        ]);
        assert_eq!(
            Ok(exp),
            query(json!({
                "select": {
                    "totalValue": {"sum": { "*": [{"key": "qty"}, {"key":"price"}]} },
                },
                "from": "orders"
            }))
        );
    }

    #[test]
    fn select_by_customer_from_orders() {
        let exp = json!({
                "james": [{ "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },{ "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },{ "time": 4, "customer": "james", "qty": 1, "price": 16.0 }],
                "ania": [{ "time": 1, "customer": "ania", "qty": 2, "price": 2.0 }],
                "misha": [{ "time": 2, "customer": "misha", "qty": 4, "price": 1.0 }],
        });
        assert_eq!(
            Ok(exp),
            query(json!({
                "by": {"key":"customer"},
                "from": "orders"
            }))
        );
    }

    #[test]
    fn select_max_qty_by_customer_from_orders() {
        let exp = json!({
                "james": { "maxQty": 10 },
                "ania": { "maxQty": 2 },
                "misha": { "maxQty": 4 },
        });
        assert_eq!(
            query(json!({
                "select": {"maxQty":{"max":{"key":"qty"}}},
                "by": {"key":"customer"},
                "from": "orders"
            })),
            Ok(exp)
        );
    }

    #[test]
    fn select_max_unique_qty_by_customer_from_orders() {
        let exp = json!({
                "james": { "maxUniqueQty": 10 },
                "ania": { "maxUniqueQty": 2 },
                "misha": { "maxUniqueQty": 4 },
        });
        assert_eq!(
            query(json!({
                "select": {"maxUniqueQty":{"max":{"unique":{"key":"qty"}}}},
                "by": {"key":"customer"},
                "from": "orders"
            })),
            Ok(exp)
        );
    }

    #[test]
    fn select_first_time_last_time_from_orders() {
        let exp = json!([{
                "start": 0,
                "end": 4,
        }]);
        assert_eq!(
            query(json!({
                "select": {
                    "start": {"first":{"key":"time"}},
                    "end": {"last":{"key":"time"}},
                },
                "from": "orders"
            })),
            Ok(exp)
        );
    }

    #[test]
    fn select_first_time_last_time_by_customer_from_orders() {
        let exp = json!({
                "james": { "startQty":2, "endQty":1 },
                "ania": { "startQty":2, "endQty":2 },
                "misha": { "startQty": 4, "endQty": 4},
        });
        assert_eq!(
            query(json!({
                "select": {
                    "startQty":{"first":{"key":"qty"}},
                    "endQty":{"last":{"key":"qty"}},
                },
                "by": {"key":"customer"},
                "from": "orders"
            })),
            Ok(exp)
        );
    }

    #[test]
    fn select_order_volume_by_customer_from_orders() {
        let exp = json!({
                "james": { "orderVolume": 194.0 },
                "ania": { "orderVolume": 4.0 },
                "misha": { "orderVolume": 4.0 },
        });
        assert_eq!(
            query(json!({
                "select": {
                    "orderVolume":{"sum":{"*": [{"key":"qty"},{"key":"price"}]}},
                },
                "by": {"key":"customer"},
                "from": "orders"
            })),
            Ok(exp)
        );
    }

    #[test]
    fn select_order_value_by_customer_from_orders() {
        let exp = json!({
                "james":{ "orderVal": [18.0, 160.0, 16.0] },
                "ania": { "orderVal": [4.0] },
                "misha": { "orderVal": [4.0] },
        });
        assert_eq!(
            query(json!({
                "select": {
                    "orderVal":{"*": [{"key":"qty"},{"key":"price"}]},
                },
                "by": {"key":"customer"},
                "from": "orders"
            })),
            Ok(exp)
        );
    }

    #[test]
    fn test_first() {
        assert_eq!(Ok(Json::Bool(true)), eval(first(key("b"))));
        assert_eq!(Ok(Json::Bool(true)), eval(first(key("b"))));
        assert_eq!(Ok(Json::from(3.3)), eval(first(key("f"))));
        assert_eq!(Ok(Json::from(2)), eval(first(key("i"))));
        assert_eq!(Ok(Json::from(1.1)), eval(first(key("fa"))));
        assert_eq!(Ok(Json::from(1)), eval(first(key("ia"))));
    }

    #[test]
    fn test_last() {
        assert_eq!(Ok(Json::from(true)), eval(last(key("b"))));
        assert_eq!(Ok(Json::from(3.3)), eval(last(key("f"))));
        assert_eq!(Ok(Json::from(2)), eval(last(key("i"))));
        assert_eq!(Ok(Json::from(5.5)), eval(last(key("fa"))));
        assert_eq!(Ok(Json::from(5)), eval(last(key("ia"))));
    }

    #[test]
    fn test_max() {
        assert_eq!(Ok(Json::Bool(true)), eval(max(key("b"))));
        assert_eq!(Ok(Json::from(2)), eval(max(key("i"))));
        assert_eq!(Ok(Json::from(3.3)), eval(max(key("f"))));
        assert_eq!(Ok(Json::from(5)), eval(max(key("ia"))));
        assert_eq!(Ok(Json::from(5.5)), eval(max(key("fa"))));
    }

    #[test]
    fn test_min() {
        assert_eq!(Ok(Json::Bool(true)), eval(min(key("b"))));
        assert_eq!(Ok(Json::from(2)), eval(min(key("i"))));
        assert_eq!(Ok(Json::from(3.3)), eval(min(key("f"))));
        assert_eq!(Ok(Json::from(1.1)), eval(min(key("fa"))));
        assert_eq!(Ok(Json::from(1)), eval(min(key("ia"))));
    }

    #[test]
    fn test_avg() {
        assert_eq!(Ok(Json::from(3.3)), eval(avg(key("f"))));
        assert_eq!(Ok(Json::from(2)), eval(avg(key("i"))));
        assert_eq!(Ok(Json::from(3.3)), eval(avg(key("fa"))));
        assert_eq!(Ok(Json::from(3.0)), eval(avg(key("ia"))));
    }

    #[test]
    fn test_var() {
        assert_eq!(Ok(Json::from(0)), eval(var(key("f"))));
        assert_eq!(Ok(Json::from(0)), eval(var(key("i"))));
        let val = eval(var(key("fa"))).unwrap().as_f64().unwrap();
        assert_approx_eq!(3.10, val, 0.0249f64);
        let val = eval(var(key("ia"))).unwrap().as_f64().unwrap();
        assert_approx_eq!(2.56, val, 0.0249f64);
    }

    #[test]
    fn test_dev() {
        assert_eq!(Ok(Json::from(0)), eval(dev(key("f"))));
        assert_eq!(Ok(Json::from(0)), eval(dev(key("i"))));
        let val = eval(dev(key("fa"))).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.55, val, 0.03f64);
        let val = eval(dev(key("ia"))).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.414, val, 0.03f64);
    }

    #[test]
    fn test_add() {
        assert_eq!(Ok(Json::from(9)), eval(add(key("x"), key("y"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(5),
                Json::from(6),
                Json::from(7),
                Json::from(8),
                Json::from(9),
            ])),
            eval(add(key("x"), key("ia")))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3),
                Json::from(4),
                Json::from(5),
                Json::from(6),
                Json::from(7),
            ])),
            eval(add(key("ia"), key("i")))
        );

        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("ahello"),
                Json::from("bhello"),
                Json::from("chello"),
                Json::from("dhello"),
            ])),
            eval(add(key("sa"), key("s")))
        );
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from("helloa"),
                Json::from("hellob"),
                Json::from("helloc"),
                Json::from("hellod"),
            ])),
            eval(add(key("s"), key("sa")))
        );

        assert_eq!(Ok(Json::from("hellohello")), eval(add(key("s"), key("s"))));
        assert_eq!(Ok(json!("hello3.3")), eval(add(key("s"), key("f"))));
        assert_eq!(Ok(json!("3.3hello")), eval(add(key("f"), key("s"))));
        assert_eq!(Ok(json!("2hello")), eval(add(key("i"), key("s"))));
        assert_eq!(Ok(json!("hello2")), eval(add(key("s"), key("i"))));
    }

    #[test]
    fn test_sub() {
        assert_eq!(Ok(Json::from(-1)), eval(sub(key("x"), key("y"))));
        assert_eq!(Ok(Json::from(1)), eval(sub(key("y"), key("x"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(3),
                Json::from(2),
                Json::from(1),
                Json::from(0),
                Json::from(-1),
            ])),
            eval(sub(key("x"), key("ia")))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(-4),
                Json::from(-3),
                Json::from(-2),
                Json::from(-1),
                Json::from(0),
            ])),
            eval(sub(key("ia"), key("y")))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0),
                Json::from(0),
                Json::from(0),
                Json::from(0),
                Json::from(0),
            ])),
            eval(sub(key("ia"), key("ia")))
        );

        assert_eq!(bad_type(), eval(sub(key("s"), key("s"))));
        assert_eq!(bad_type(), eval(sub(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval(sub(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval(sub(key("i"), key("s"))));
        assert_eq!(bad_type(), eval(sub(key("s"), key("i"))));
    }

    #[test]
    fn json_mul() {
        assert_eq!(Ok(Json::from(20)), eval(mul(key("x"), key("y"))));
        assert_eq!(Ok(Json::from(16)), eval(mul(key("x"), key("x"))));
        let arr = vec![
            Json::from(5),
            Json::from(10),
            Json::from(15),
            Json::from(20),
            Json::from(25),
        ];
        assert_eq!(Ok(Json::from(arr.clone())), eval(mul(key("ia"), key("y"))));
        assert_eq!(Ok(Json::from(arr)), eval(mul(key("y"), key("ia"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1),
                Json::from(4),
                Json::from(9),
                Json::from(16),
                Json::from(25),
            ])),
            eval(mul(key("ia"), key("ia")))
        );
        assert_eq!(bad_type(), eval(mul(key("s"), key("s"))));
        assert_eq!(bad_type(), eval(mul(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval(mul(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval(mul(key("i"), key("s"))));
        assert_eq!(bad_type(), eval(mul(key("s"), key("i"))));
    }

    #[test]
    fn json_div() {
        assert_eq!(Ok(Json::from(1.0)), eval(div(key("x"), key("x"))));
        assert_eq!(Ok(Json::from(1.0)), eval(div(key("y"), key("y"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
            ])),
            eval(div(key("ia"), key("ia")))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.5),
                Json::from(1.0),
                Json::from(1.5),
                Json::from(2.0),
                Json::from(2.5),
            ])),
            eval(div(key("ia"), key("i")))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.6666666666666666),
                Json::from(0.5),
                Json::from(0.4),
            ])),
            eval(div(key("i"), key("ia")))
        );

        assert_eq!(bad_type(), eval(div(key("s"), key("s"))));
        assert_eq!(bad_type(), eval(div(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval(div(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval(div(key("i"), key("s"))));
        assert_eq!(bad_type(), eval(div(key("s"), key("i"))));
    }

    #[test]
    fn open_db() {
        assert_eq!(Ok(Json::Bool(true)), eval(key("b")));
        assert_eq!(
            eval(key("ia")),
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5),
            ]))
        );
        assert_eq!(eval(key("i")), Ok(Json::from(2)));
        assert_eq!(eval(key("f")), Ok(Json::from(3.3)));
        assert_eq!(
            eval(key("fa")),
            Ok(Json::Array(vec![
                Json::from(1.1),
                Json::from(2.2),
                Json::from(3.3),
                Json::from(4.4),
                Json::from(5.5),
            ]))
        );
        assert_eq!(eval(key("f")), Ok(Json::from(3.3)));
        assert_eq!(eval(key("s")), Ok(Json::from("hello")));
        assert_eq!(
            eval(key("sa")),
            Ok(Json::Array(vec![
                Json::from("a"),
                Json::from("b"),
                Json::from("c"),
                Json::from("d"),
            ]))
        );
    }

    #[test]
    fn test_get() {
        assert_eq!(Ok(Json::Bool(true)), eval(key("b")));
        assert_eq!(Ok(Json::Bool(true)), eval(key("b")));
        assert_eq!(
            Ok(Json::Array(vec![
                Json::from(1),
                Json::from(2),
                Json::from(3),
                Json::from(4),
                Json::from(5)
            ])),
            eval(key("ia"))
        );
        assert_eq!(Ok(Json::from(2)), eval(key("i")));
    }

    #[test]
    fn eval_set_get_ok() {
        let vec = vec![
            Json::from(1),
            Json::from(2),
            Json::from(3),
            Json::from(4),
            Json::from(5),
        ];
        let val = Json::from(vec.clone());
        let mut db = test_db();
        assert_eq!(Ok(Json::Null), db.eval(set("nums", Cmd::Json(val.clone()))));
        assert_eq!(Ok(Json::from(val)), db.eval(key("nums")));
    }

    #[test]
    fn eval_get_string_err_not_found() {
        assert_eq!(Err(Error::BadKey), eval(key("ania")));
    }

    #[test]
    fn nested_get() {
        let act = eval(get("name", key("t"))).unwrap();
        assert_eq!(json!(["james", "ania", "misha", "ania",]), act);
    }

    fn test_db() -> InMemDb {
        let mut db = InMemDb::new();
        insert_data(&mut db);
        db
    }

    fn query(json: Json) -> Result<Json, Error> {
        let cmd = serde_json::from_value(json).unwrap();
        let db = test_db();
        let qry = Query::from(&db, cmd);
        qry.exec()
    }

    #[test]
    fn select_all_query() {
        let qry = query(json!({"from": "t"}));
        assert_eq!(Ok(table_data()), qry);
    }

    fn table_data() -> Json {
        json!([
            {"name": "james", "age": 35},
            {"name": "ania", "age": 28, "job": "english teacher"},
            {"name": "misha", "age": 10},
            {"name": "ania", "age": 20},
        ])
    }

    #[test]
    fn select_1_prop_query() {
        let qry = query(json!({"select": {"name": {"key": "name"}}, "from": "t"}));
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
        let qry = query(json!({
            "select": {
                "name": {"key": "name"},
                "age": {"key": "age"},
                "job": {"key": "job"}
            },
            "from": "t"
        }));
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
        let val = json!([{"name": "james", "age": 35}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_neq_query_ok() {
        let qry = query(json!({"from": "t", "where": {"!=": ["name", "james"]}}));
        let val = json!([
            {"name": "ania", "age": 28, "job": "english teacher"},
            {"name": "misha", "age": 10},
            {"name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_gt_query_ok() {
        let qry = query(json!({"from": "t", "where": {">": ["age", 20]}}));
        let val = json!([
            {"name": "james", "age": 35},
            {"name": "ania", "age": 28, "job": "english teacher"},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_lt_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<": ["age", 20]}}));
        let val = json!([{"name": "misha", "age": 10}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_lte_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<=": ["age", 28]}}));
        let val = json!([
            {"name": "ania", "age": 28, "job": "english teacher"},
            {"name": "misha", "age": 10},
            {"name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_gte_query_ok() {
        let qry = query(json!({"from": "t", "where": {">=": ["age", 28]}}));
        let val = json!([
            {"name": "james", "age": 35},
            {"name": "ania", "age": 28, "job": "english teacher"}
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
        let val = json!([{"name": "ania", "age": 28, "job": "english teacher"}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_sum_ok() {
        let qry = query(json!({
            "select": {"totalAge": {"sum": {"key": "age"}}},
            "from": "t"
        }));
        let val = json!([{"totalAge": 93}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_max_num_ok() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": {"key": "age"}}
            },
            "from": "t"
        }));
        let val = json!([{"maxAge": 35}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_max_str_ok() {
        let qry = query(json!({
            "select": {
                "maxName": {"max":  {"key": "name"}}
            },
            "from": "t"
        }));
        let val = json!([{"maxName": "misha"}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_min_num_ok() {
        let qry = query(json!({
            "select": {
                "minAge": {"min": {"key": "age"}}
            },
            "from": "t"
        }));
        let val = json!([{"minAge": 10}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_avg_num_ok() {
        let qry = query(json!({
            "select": {
                "avgAge": {"avg": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!([{"avgAge": 23.25}])), qry);
    }

    #[test]
    fn select_first_ok() {
        let qry = query(json!({
            "select": {
                "firstAge": {"first": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!([{"firstAge": 35}])), qry);
    }

    #[test]
    fn select_last_ok() {
        let qry = query(json!({
            "select": {
                "lastAge": {"last": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!([{"lastAge": 20}])), qry);
    }

    #[test]
    fn select_var_num_ok() {
        let qry = query(json!({
            "select": {
                "varAge": {"var": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!([{"varAge": 146.75}])), qry);
    }

    #[test]
    fn select_dev_num_ok() {
        let qry = query(json!({
            "select": {
                "devAge": {"dev": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!([{"devAge": 9.310612224768036}])), qry);
    }

    #[test]
    fn select_max_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": {"key": "age"}}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!([{"maxAge": 35}])), qry);
    }

    #[test]
    fn select_get_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"key": "age"}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!([{"age":35}, {"age": 28}])), qry);
    }

    #[test]
    fn select_min_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "minAge": {"min":  {"key": "age"}}
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!([{"minAge": 28}])), qry);
    }

    #[test]
    fn select_min_max_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "youngestAge": {"min": {"key": "age"}},
                "oldestAge": {"max": {"key": "age"}},
            },
            "from": "t",
            "where": {">": ["age", 20]}
        }));
        assert_eq!(Ok(json!([{"youngestAge": 28, "oldestAge": 35}])), qry);
    }

    #[test]
    fn select_empty_obj() {
        let qry = query(json!({
            "select": {},
            "from": "t",
        }));
        assert_eq!(
            Ok(json!([
                {"name": "james", "age": 35},
                {"name": "ania", "age": 28, "job": "english teacher"},
                {"name": "misha", "age": 10},
                {"name": "ania", "age": 20},
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
                {"name": "james", "age": 35},
                {"name": "ania", "age": 28, "job": "english teacher"},
            ])),
            qry
        );
    }

    #[test]
    fn select_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"key": "age"},
            },
            "from": "t",
            "by": {"key": "name"},
        }));
        assert_eq!(
            Ok(json!({
                "misha": {"age": [10]},
                "ania": {"age": [28, 20]},
                "james": {"age":[35]},
            })),
            qry
        );
    }

    #[test]
    fn select_first_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"first": {"key": "age"}},
            },
            "from": "t",
            "by": {"key": "name"},
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
                "age": {"last": {"key":"age"}},
            },
            "from": "t",
            "by": {"key": "name"},
        }));
        assert_eq!(
            Ok(json!({
                "misha":{"age": 10},
                "ania":{"age": 20},
                "james":{"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_count_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"len": {"key": "age"}},
            },
            "from": "t",
            "by": {"key": "name"},
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
                "age": {"min": {"key": "age"}},
            },
            "from": "t",
            "by": {"key":"name"},
        }));
        assert_eq!(
            Ok(json!({
                "misha":{"age": 10},
                "ania":{"age": 20},
                "james":{"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_max_age_by_name() {
        let qry = query(json!({
            "select": {
                "age": {"max": {"key": "age"}},
            },
            "from": "t",
            "by": {"key": "name"},
        }));
        assert_eq!(
            Ok(json!({
                "misha":{"age": 10},
                "ania":{"age": 28},
                "james":{"age": 35},
            })),
            qry
        );
    }

    #[test]
    fn select_name_by_age() {
        let qry = query(json!({
            "select": {
                "name": {"key": "name"},
            },
            "from": "t",
            "by": {"key": "age"},
        }));
        assert_eq!(
            Ok(json!({
                "10": {"name": ["misha"]},
                "20": {"name": ["ania"]},
                "28": {"name": ["ania"]},
                "35": {"name": ["james"]},
            })),
            qry
        );
    }

    #[test]
    fn select_age_by_name_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"key": "age"},
            },
            "from": "t",
            "by": {"key": "name"},
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({
                "ania": {"age": [28]},
                "james": {"age": [35]}
            })),
            qry
        );
    }

    #[test]
    fn select_empty_keys_by_name() {
        let qry = query(json!({
            "select": {
                "qty": {"key": "qty"},
                "price": {"key": "price"},
            },
            "from": "t",
            "by": {"key":"name"}
        }));
        assert_eq!(
            Ok(json!({
                "ania": {},
                "james": {},
                "misha": {},
            })),
            qry
        );
    }

    #[test]
    fn select_age_job_by_name_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"key": "age"},
                "job": {"key": "job"},
            },
            "from": "t",
            "by": {"key": "name"},
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({
                "ania": {"age": [28], "job": ["english teacher"]},
                "james": {"age": [35]},
            })),
            qry
        );
    }

    #[test]
    fn select_all_by_name_where_age_gt_20() {
        let qry = query(json!({
            "from": "t",
            "by": {"key": "name"},
            "where": {">": ["age", 20]}
        }));
        assert_eq!(
            Ok(json!({
                "ania": [{"age": 28, "name": "ania", "job": "english teacher"}],
                "james": [{"age": 35, "name": "james"}],
            })),
            qry
        );
    }

    #[test]
    fn select_count_max_min_age_by_name() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": {"key": "age"}},
                "minAge": {"min": {"key": "age"}},
                "countAge": {"len": {"key": "age"}},
            },
            "from": "t",
            "by": {"key": "name"},
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

    #[test]
    fn eval_mul() {
        assert_eq!(Ok(Json::from(20)), eval(mul(key("x"), key("y"))));
        assert_eq!(Ok(Json::from(16)), eval(mul(key("x"), key("x"))));
        let arr = vec![
            Json::from(5),
            Json::from(10),
            Json::from(15),
            Json::from(20),
            Json::from(25),
        ];
        assert_eq!(Ok(Json::from(arr.clone())), eval(mul(key("ia"), key("y"))));
        assert_eq!(Ok(Json::from(arr)), eval(mul(key("y"), key("ia"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1),
                Json::from(4),
                Json::from(9),
                Json::from(16),
                Json::from(25),
            ])),
            eval(mul(key("ia"), key("ia")))
        );
        assert_eq!(bad_type(), eval(mul(key("s"), key("s"))));
        assert_eq!(bad_type(), eval(mul(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval(mul(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval(mul(key("i"), key("s"))));
        assert_eq!(bad_type(), eval(mul(key("s"), key("i"))));
    }

    #[test]
    fn eval_div() {
        assert_eq!(Ok(Json::from(1.0)), eval(div(key("x"), key("x"))));
        assert_eq!(Ok(Json::from(1.0)), eval(div(key("y"), key("y"))));
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
                Json::from(1.0),
            ])),
            eval(div(key("ia"), key("ia")))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(0.5),
                Json::from(1.0),
                Json::from(1.5),
                Json::from(2.0),
                Json::from(2.5),
            ])),
            eval(div(key("ia"), key("i")))
        );
        assert_eq!(
            Ok(Json::from(vec![
                Json::from(2.0),
                Json::from(1.0),
                Json::from(0.6666666666666666),
                Json::from(0.5),
                Json::from(0.4),
            ])),
            eval(div(key("i"), key("ia")))
        );

        assert_eq!(bad_type(), eval(div(key("s"), key("s"))));
        assert_eq!(bad_type(), eval(div(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval(div(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval(div(key("i"), key("s"))));
        assert_eq!(bad_type(), eval(div(key("s"), key("i"))));
    }
}
