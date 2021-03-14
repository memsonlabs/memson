use crate::apply::apply_rows;
use crate::cmd::{Cmd, QueryCmd};
use crate::err::Error;
use crate::eval::*;
use crate::inmem::InMemDb;
use crate::json::*;
use crate::ondisk::OnDiskDb;
use rayon::prelude::*;
use serde_json::{Value as Json, Value};
use std::collections::HashMap;
use std::path::Path;

pub(crate) const PAGE_SIZE: usize = 50;

pub struct Memson {
    mem_db: InMemDb,
    disk_db: OnDiskDb,
}

impl Memson {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let disk_db = OnDiskDb::open(path)?;
        let mem_db = InMemDb::load(&disk_db)?;
        Ok(Self { mem_db, disk_db })
    }

    pub(crate) fn eval(&mut self, cmd: Cmd) -> Result<Json, Error> {
        match cmd {
            Cmd::Set(key, arg) => {
                let val = self.eval(*arg)?;
                match self.disk_db.set(&key, &val)? {
                    Some(val) => Ok(val),
                    None => Ok(Json::Null),
                }
            }
            cmd => self.mem_db.eval(cmd),
        }
    }

    pub(crate) fn query(&mut self, cmd: QueryCmd) -> Result<Json, Error> {
        self.mem_db.query(cmd)
    }
}

pub struct Query<'a> {
    pub(crate) db: &'a InMemDb,
    pub(crate) cmd: QueryCmd,
}

/// Represents rows from a table from memson
/// Avoids doing a deep copy
enum Rows<'a> {
    Val(Vec<Json>),
    Ref(&'a [Json]),
}

impl<'a> Rows<'a> {
    /// slice of rows
    fn as_slice(&self) -> &[Json] {
        match self {
            Rows::Val(vec) => vec.as_slice(),
            Rows::Ref(slice) => slice,
        }
    }
}

/// merge two groupby maps together. if key exists in both then the array are merged together.
fn merge_grouping(
    mut x: HashMap<String, Vec<Json>>,
    y: HashMap<String, Vec<Json>>,
) -> HashMap<String, Vec<Json>> {
    for (key, val) in y {
        let vals = x.entry(key).or_insert_with(Vec::new);
        vals.extend(val);
    }
    x
}

/// evaluation of sort by
fn eval_sortby(rows: &[Json], key: &str, descend: bool) -> Vec<Json> {
    let mut r = rows.to_vec();
    if descend {
        r.par_sort_by(|x, y| sortby_desc_key(key, x, y));
    } else {
        r.par_sort_by(|x, y| sortby_key(key, x, y));
    }
    r
}

impl<'a> Query<'a> {
    /// Create query from a reference to the key/value cache and query command
    pub fn from(db: &'a InMemDb, cmd: QueryCmd) -> Self {
        Self { db, cmd }
    }

    /// executes the query
    pub fn exec(&self) -> Result<Json, Error> {
        let rows = self.eval_rows()?;
        if let Some(by) = &self.cmd.by {
            self.eval_grouped_selects(by.as_ref(), rows)
        } else {
            self.eval_select(rows)
        }
    }

    /// evaluate the rows to query against
    fn eval_rows(&self) -> Result<Rows, Error> {
        let rows = self.eval_db_rows()?;
        let descend = self.descend();
        let rows = match (&self.cmd.filter, &self.cmd.sort) {
            (Some(filter), Some(key)) => {
                let cmd = Cmd::parse(filter.clone())?;
                let rows = self.eval_where(rows, &cmd)?;
                Rows::Val(eval_sortby(&rows, key, descend))
            }
            (Some(filter), None) => {
                let cmd = Cmd::parse(filter.clone())?;
                Rows::Val(self.eval_where(rows, &cmd)?)
            }
            (None, Some(key)) => Rows::Val(eval_sortby(rows, key, descend)),
            (None, None) => Rows::Ref(rows),
        };
        Ok(rows)
    }

    /// check if the sort order is descending
    fn descend(&self) -> bool {
        self.cmd.descend.unwrap_or(false)
    }

    /// evaulate the grouped selects
    fn eval_grouped_selects(&self, by: &Cmd, rows: Rows) -> Result<Json, Error> {
        let grouping = self.eval_grouping(by, rows.as_slice())?;
        if let Some(selects) = &self.cmd.selects {
            self.eval_grouped_select(grouping, selects)
        } else {
            let mut obj = JsonObj::new();
            for (k, v) in grouping {
                obj.insert(k, v.into_iter().map(Json::from).collect());
            }
            Ok(Json::from(obj))
        }
    }

    /// evaulate the commands by grouped json values
    fn eval_grouped_select(
        &self,
        grouping: HashMap<String, Vec<Json>>,
        selects: &HashMap<String, Cmd>,
    ) -> Result<Json, Error> {
        let mut keyed_obj = JsonObj::new();
        for (key, keyed_rows) in grouping {
            let mut obj = JsonObj::new();
            let keyed_val = Json::Array(keyed_rows);
            for (col, cmd) in selects {
                if let Some(v) = eval_rows_cmd(cmd.clone(), &keyed_val) {
                    obj.insert(col.to_string(), v);
                }
            }
            keyed_obj.insert(key, Json::Object(obj));
        }
        Ok(Json::from(keyed_obj))
    }

    /// evaulate the group by statements
    // TODO(jaupe) refactor to change val type to Json from Vec<Json>
    fn eval_grouping(&self, by: &Cmd, rows: &[Json]) -> Result<HashMap<String, Vec<Json>>, Error> {
        match by {
            Cmd::Key(key) => {
                let g: HashMap<String, Vec<Value>> = rows
                    .par_iter()
                    .map(|row| (row, row.get(key)))
                    .filter(|(_, x)| x.is_some())
                    .map(|(row, x)| (row, x.unwrap()))
                    .fold(HashMap::new, |mut g, (row, val)| {
                        let entry: &mut Vec<Json> = g.entry(json_str(val)).or_insert_with(Vec::new);
                        entry.push(row.clone());
                        g
                    })
                    .reduce(HashMap::new, merge_grouping);
                Ok(g)
            }
            _ => Err(Error::BadGroupBy),
        }
    }

    /// evaulate the rows from the memson cache
    fn eval_db_rows(&self) -> Result<&[Value], Error> {
        let val = self.db.get(&self.cmd.from)?;
        val.as_array()
            .map(|x| x.as_slice())
            .ok_or(Error::ExpectedArr)
    }

    /// evaulate the where statement
    fn eval_where(&self, rows: &[Json], filter: &Cmd) -> Result<Vec<Json>, Error> {
        let mut filtered_rows = Vec::new();
        for row in rows {
            if let Some(obj) = row.as_object() {
                if let Some(true) = eval_filter(filter.clone(), row) {
                    filtered_rows.push(Json::from(obj.clone()));
                }
            }
        }
        Ok(filtered_rows)
    }

    // TODO remove cloning
    /// evaluate the select statements
    fn eval_select(&self, rows: Rows) -> Result<Json, Error> {
        match &self.cmd.selects {
            Some(selects) => self.eval_obj_selects(selects, rows),
            None => self.eval_select_all(rows.as_slice()),
        }
    }

    /// evaluate select statements when structured as a json object
    fn eval_obj_selects(&self, selects: &HashMap<String, Cmd>, rows: Rows) -> Result<Json, Error> {
        if selects.is_empty() {
            return self.eval_select_all(rows.as_slice());
        }
        //todo the cmds vec is not neccessary
        let mut projections = Map::new();
        for (name, select) in selects {
            let val = apply_rows(select.clone(), rows.as_slice())?;
            projections.insert(name.to_string(), val);
        }
        Ok(Json::Object(projections))
    }

    //TODO paginate
    /// evaluate select query without any select statements
    fn eval_select_all(&self, rows: &[Json]) -> Result<Json, Error> {
        let mut output = Vec::new();
        for row in rows.iter().take(50).cloned() {
            output.push(row);
        }
        Ok(Json::from(output))
    }
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
        Cmd::Dev(b(arg))
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

    fn orders_val() -> Json {
        json!([
                { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ])
    }

    fn orders2_val() -> Json {
        json!([
                { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10, "address": {"line1": "23 garden close", "city": "london", "country": "gb"} },
                { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ])
    }

    fn insert_data(db: &mut InMemDb) {
        db.set("a", json!([1, 2, 3, 4, 5]));
        db.set("b", json!(true));
        db.set("i", json!(2));
        db.set("f", json!(3.3));
        db.set("ia", json!([1, 2, 3, 4, 5]));
        db.set("nia", json!([Json::Null, 2, Json::Null, 4, 5]));
        db.set("nfa", json!([1, Json::Null, 3, Json::Null, 5]));
        db.set("fa", json!([1.1, 2.2, 3.3, 4.4, 5.5]));
        db.set("x", json!(4));
        db.set("y", json!(5));
        db.set("s", json!("hello"));
        db.set("sa", json!(["a", "b", "c", "d"]));
        db.set("t", table_data());
        db.set("n", Json::Null);
        db.set("orders", orders_val());
        db.set("orders2", orders2_val());
    }

    #[test]
    fn load_data() {
        let path = "testdata";
        let data = json!([
                { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]);
        {
            let ondisk_db = OnDiskDb::open(path).unwrap();
            ondisk_db.set("customers", &data).unwrap();
        }
        let mut memson = Memson::open(path).unwrap();
        assert_eq!(Ok(data), memson.eval(Cmd::Key("customers".to_string())));
    }

    #[test]
    fn select_all_from_orders() {
        let exp = json!([
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10},
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]);
        assert_eq!(Ok(exp), query(json!({"from": "orders"})));
    }

    #[test]
    fn select_customer_from_orders() {
        let exp = json!({"name":["james", "ania", "misha", "james", "james"]});
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
        let exp =
            json!({"name": ["james", "ania","misha","james","james"], "quantity": [2,2,4,10,1]});
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
        let exp = json!({
            "name": ["james", "ania", "misha", "james", "james"],
            "quantity": [2, 2, 4, 10, 1],
            "discount": [10,20]
        });
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
        let exp = json!({ "uniqueNames": ["james", "ania", "misha"] });
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
        let exp = json!(
            { "countDiscount": 2, "countCustomer": 5}
        );
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
        let exp: Json = json!({ "value": [18.0, 4.0, 4.0, 160.0, 16.0] });
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
        let exp = json!({ "totalValue": 202.0 });
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
        let exp = json!({
                "start": 0,
                "end": 4,
        });
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
            Ok(Json::from(vec![
                Json::Null,
                Json::from(4),
                Json::Null,
                Json::from(6),
                Json::from(7),
            ])),
            eval(add(key("nia"), key("i")))
        );

        assert_eq!(
            Ok(Json::from(vec![
                Json::Null,
                Json::Null,
                Json::Null,
                Json::Null,
                Json::from(10),
            ])),
            eval(add(key("nia"), key("nfa")))
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
            Ok(Json::Array(vec![
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
    fn test_eval_key() {
        assert_eq!(Ok(orders2_val()), eval(key("orders2")));
        assert_eq!(Ok(json!([0,1,2,3,4])), eval(key("orders2.time")));
        assert_eq!(Ok(json!(["james", "ania", "misha", "james", "james"])), eval(key("orders2.customer")));
        assert_eq!(Ok(json!([2,2,4,10,1])), eval(key("orders2.qty")));
        assert_eq!(Ok(json!([9.0, 2.0, 1.0, 16.0, 16.0])), eval(key("orders2.price")));
        assert_eq!(Ok(json!([10, Json::Null, Json::Null, 20, Json::Null])), eval(key("orders2.discount")));
        assert_eq!(Ok(json!(["23 garden close", Json::Null, Json::Null, Json::Null, Json::Null])), eval(key("orders2.address.line1")));
    }

    #[test]
    fn eval_cmds() {
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
        assert_eq!(Err(Error::BadKey("ania".to_string())), eval(key("ania")));
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
        let val = json!({"name": ["james", "ania", "misha", "ania"]});
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
        let val = json!({
            "name": ["james", "ania", "misha", "ania"],
            "age": [35, 28, 10, 20],
            "job": ["english teacher"],
        });
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_eq_query() {
        let qry = query(json!({"from": "t", "where": {"==": [{"key": "name"}, "james"]}}));
        let val = json!([{"name": "james", "age": 35}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_neq_query_ok() {
        let qry = query(json!({"from": "t", "where": {"!=": [{"key":"name"}, "james"]}}));
        let val = json!([
            {"name": "ania", "age": 28, "job": "english teacher"},
            {"name": "misha", "age": 10},
            {"name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_gt_query_ok() {
        let qry = query(json!({"from": "t", "where": {">": [{"key": "age"}, 20]}}));
        let val = json!([
            {"name": "james", "age": 35},
            {"name": "ania", "age": 28, "job": "english teacher"},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_lt_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<": [{"key": "age"}, 20]}}));
        let val = json!([{"name": "misha", "age": 10}]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_lte_query_ok() {
        let qry = query(json!({"from": "t", "where": {"<=": [{"key": "age"}, 28]}}));
        let val = json!([
            {"name": "ania", "age": 28, "job": "english teacher"},
            {"name": "misha", "age": 10},
            {"name": "ania", "age": 20},
        ]);
        assert_eq!(Ok(val), qry);
    }

    #[test]
    fn select_all_where_gte_query_ok() {
        let qry = query(json!({"from": "t", "where": {">=": [{"key":"age"}, 28]}}));
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
                {">": [{"key": "age"}, 20]},
                {"==": [{"key":"name"}, "ania"]}
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
        let val = json!({"totalAge": 93});
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
        let val = json!({"maxAge": 35});
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
        let val = json!({"maxName": "misha"});
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
        let val = json!({"minAge": 10});
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
        assert_eq!(Ok(json!({"avgAge": 23.25})), qry);
    }

    #[test]
    fn select_first_ok() {
        let qry = query(json!({
            "select": {
                "firstAge": {"first": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"firstAge": 35})), qry);
    }

    #[test]
    fn select_last_ok() {
        let qry = query(json!({
            "select": {
                "lastAge": {"last": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"lastAge": 20})), qry);
    }

    #[test]
    fn select_var_num_ok() {
        let qry = query(json!({
            "select": {
                "varAge": {"var": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"varAge": 146.75})), qry);
    }

    #[test]
    fn select_dev_num_ok() {
        let qry = query(json!({
            "select": {
                "devAge": {"dev": {"key": "age"}}
            },
            "from": "t"
        }));
        assert_eq!(Ok(json!({"devAge": 9.310612224768036})), qry);
    }

    #[test]
    fn select_max_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "maxAge": {"max": {"key": "age"}}
            },
            "from": "t",
            "where": {">": [{"key":"age"}, 20]}
        }));
        assert_eq!(Ok(json!({"maxAge": 35})), qry);
    }

    #[test]
    fn select_get_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "age": {"key": "age"}
            },
            "from": "t",
            "where": {">": [{"key":"age"}, 20]}
        }));
        assert_eq!(Ok(json!({"age":[35, 28]})), qry);
    }

    #[test]
    fn select_min_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "minAge": {"min":  {"key": "age"}}
            },
            "from": "t",
            "where": {">": [{"key":"age"}, 20]}
        }));
        assert_eq!(Ok(json!({"minAge": 28})), qry);
    }

    #[test]
    fn select_min_max_age_where_age_gt_20() {
        let qry = query(json!({
            "select": {
                "youngestAge": {"min": {"key": "age"}},
                "oldestAge": {"max": {"key": "age"}},
            },
            "from": "t",
            "where": {">": [{"key":"age"}, 20]}
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
            "where": {">": [{"key":"age"}, 20]}
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
            "where": {">": [{"key": "age"}, 20]}
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
                "ania": {"qty":[], "price":[]},
                "james": {"qty":[], "price":[]},
                "misha": {"qty":[], "price":[]},
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
            "where": {">": [{"key": "age"}, 20]}
        }));
        assert_eq!(
            Ok(json!({
                "ania": {"age": [28], "job": ["english teacher"]},
                "james": {"age": [35], "job": []},
            })),
            qry
        );
    }

    #[test]
    fn select_all_by_name_where_age_gt_20() {
        let qry = query(json!({
            "from": "t",
            "by": {"key": "name"},
            "where": {">": [{"key":"age"}, 20]}
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
    fn select_all_sort_by_name() {
        let qry = query(json!({
            "from": "orders",
            "sort": "customer",
        }));
        assert_eq!(
            Ok(json!([
                { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
                { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            ])),
            qry
        );
    }

    #[test]
    fn select_all_sort_by_qty() {
        let qry = query(json!({
            "from": "orders",
            "sort": "qty",
        }));
        assert_eq!(
            Ok(json!([
                    { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
                    { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                    { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                    { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                    { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            ])),
            qry
        );
    }

    #[test]
    fn select_all_sort_by_time() {
        let qry = query(json!({
            "from": "orders",
            "sortBy": "time",
        }));
        assert_eq!(
            Ok(json!([
                    { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                    { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                    { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                    { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                    { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
            ])),
            qry
        );
    }

    #[test]
    fn select_all_sort_by_time_desc() {
        let qry = query(json!({
            "from": "orders",
            "sort": "time",
            "descend": true,
        }));
        assert_eq!(
            Ok(json!([
                    { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
                    { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                    { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                    { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                    { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            ])),
            qry
        );
    }

    #[test]
    fn select_all_sort_by_price() {
        let qry = query(json!({
            "from": "orders",
            "sort": "price",
        }));
        assert_eq!(
            Ok(json!([
                    { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                    { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                    { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                    { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                    { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
            ])),
            qry
        );
    }

    #[test]
    fn select_all_sort_by_discount() {
        let qry = query(json!({
            "from": "orders",
            "sort": "discount",
        }));
        assert_eq!(
            Ok(json!([
                    { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                    { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
                    { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
                    { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                    { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
            ])),
            qry
        );
    }

    #[test]
    fn select_from_orders_where_key_eq_discount() {
        let qry = query(json!({
            "from": "orders",
            "where": {"has": "discount"},
        }));
        assert_eq!(
            Ok(json!([
                { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
                { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            ])),
            qry
        );
    }

    #[test]
    fn select_all_from_orders_where_qty_gt_2() {
        let qry = query(json!({
            "from": "orders",
            "where": {">": [{"key": "qty"}, 2]}
        }));
        assert_eq!(
            Ok(json!([
                { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
                { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            ])),
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
    fn eval_sortby_ok() {
        let rows = json!([
            {"name": "a", "age": 20},
            {"name": "d", "age": 19},
            {"name": "b", "age": 21},
            {"name": "c", "age": 10},
            {"name": "b", "age": 29},
        ]);
        let exp = json!([
            {"name": "a", "age": 20},
            {"name": "b", "age": 21},
            {"name": "b", "age": 29},
            {"name": "c", "age": 10},
            {"name": "d", "age": 19},
        ]);
        assert_eq!(
            Json::Array(eval_sortby(rows.as_array().unwrap(), "name", false)),
            exp
        );

        let val = json!([
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]);
        let exp = json!([
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
        ]);
        assert_eq!(
            exp,
            Json::Array(eval_sortby(val.as_array().unwrap(), "qty", false))
        );

        let exp = json!([
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]);
        assert_eq!(
            exp,
            Json::Array(eval_sortby(val.as_array().unwrap(), "price", false))
        );

        let exp = json!([
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]);
        assert_eq!(
            exp,
            Json::Array(eval_sortby(val.as_array().unwrap(), "time", false))
        );

        let exp = json!([
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]);
        assert_eq!(
            exp,
            Json::Array(eval_sortby(val.as_array().unwrap(), "discount", false))
        );
    }

    #[test]
    fn eval_filter_ok() {
        let cmd = Cmd::Gt(
            Box::new(Cmd::Key("age".to_string())),
            Box::new(Cmd::Json(Json::from(2))),
        );
        assert_eq!(Some(true), eval_filter(cmd, &json!({"age": 3})));
    }

    #[test]
    fn eval_map_ok() {
        let cmd = Cmd::Map(
            Box::new(Cmd::Json(json!([1, 2, 3, 4, 5]))),
            "len".to_string(),
        );
        assert_eq!(Ok(json!([1, 1, 1, 1, 1])), eval(cmd));
    }

    #[test]
    fn clone_rows_ok() {
        let exp = json!([
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]);
        assert_eq!(exp.clone(), exp);
        let arr = exp.as_array().unwrap();
        let act = arr.to_vec();
        assert_eq!(exp, Json::Array(act));
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
