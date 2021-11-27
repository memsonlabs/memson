use crate::cmd::{Cmd, Range};
use crate::err::Error;
use crate::json::*;
use crate::ondisk::{ivec_to_json, OnDiskDb};
use serde_json::json;
use std::collections::BTreeMap;

pub(crate) const PAGE_SIZE: usize = 50;

pub type Cache = BTreeMap<String, Json>;

pub fn load_cache(db: &sled::Db) -> Result<Cache, Error> {
    let mut cache = Cache::new();
    for kv in db.iter() {
        let (key, val) = kv.map_err(|_| Error::BadIO)?;
        let key: String = bincode::deserialize(key.as_ref()).map_err(|_| Error::BadIO)?;
        let val: Json = bincode::deserialize(val.as_ref()).map_err(|_| Error::BadIO)?;
        cache.insert(key, val);
    }
    Ok(cache)
}

/// The in-memory database of memson
#[derive(Debug)]
pub struct InMemDb {
    cache: Cache,
}

impl InMemDb {
    /// populate an in memory database from a on disk db
    ///
    pub fn load(on_disk_db: &OnDiskDb) -> Result<Self, Error> {
        let mut inmem_db = InMemDb::new();
        for kv in on_disk_db.sled.iter() {
            let (key, val) = kv.map_err(|_| Error::BadIO)?;
            let s = String::from_utf8(key.as_ref().to_vec()).unwrap();
            inmem_db.set(s, ivec_to_json(&val)?);
        }
        Ok(inmem_db)
    }

    /// the paginated keys of entried in memson
    pub fn keys(&self, range: Option<Range>) -> Vec<Json> {
        if let Some(range) = range {
            let start = range.start.unwrap_or(0);
            let size = range.size.unwrap_or(PAGE_SIZE);
            self.cache
                .keys()
                .skip(start)
                .take(size)
                .cloned()
                .map(Json::from)
                .collect()
        } else {
            self.cache
                .keys()
                .take(PAGE_SIZE)
                .cloned()
                .map(Json::from)
                .collect()
        }
    }

    /// delete an entry by key and return the previous value if exists
    pub fn delete(&mut self, key: &str) -> Option<Json> {
        self.cache.remove(key)
    }

    /// has checks if the key is contained in memson or not
    pub fn has(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    /// get a key/val entry; similar to key but takes a reference to a string
    pub fn get(&self, key: &str) -> Result<&Json, Error> {
        self.key(key).ok_or_else(|| Error::BadKey(key.to_string()))
    }

    pub fn get_mut(&mut self, key: &str) -> Result<&mut Json, Error> {
        self.cache
            .get_mut(key)
            .ok_or_else(|| Error::BadKey(key.to_string()))
    }

    pub fn key(&self, key: &str) -> Option<&Json> {
        self.cache.get(key)
    }

    /// inserts a new key/val entry
    pub fn set<K: Into<String>>(&mut self, key: K, val: Json) -> Option<Json> {
        self.cache.insert(key.into(), val)
    }

    /// evaluate a command
    pub fn eval(&mut self, cmd: Cmd) -> Result<Json, Error> {
        match cmd {
            Cmd::Add(lhs, rhs) => eval_bin_fn(self, *lhs, *rhs, &|x, y| json_add(x, y)),
            Cmd::Append(key, arg) => eval_append(self, &key, *arg),
            Cmd::Avg(arg) => self.eval_unr_fn(*arg, &|x| json_avg(x)),
            Cmd::Bar(lhs, rhs) => eval_bin_fn(self, *lhs, *rhs, &|x, y| json_bar(x, y)),
            Cmd::Len(arg) => self.eval_unr_fn(*arg, &|x| count(x)),
            Cmd::Delete(key) => {
                let val = self.delete(&key).unwrap_or(Json::Null);
                Ok(val)
            }
            Cmd::Div(lhs, rhs) => eval_bin_fn(self, *lhs, *rhs, &|x, y| json_div(x, y)),
            Cmd::First(arg) => self.eval_unr_fn(*arg, &|x| Ok(json_first(x).unwrap_or(Json::Null))),
            Cmd::Get(key, arg) => {
                let val = self.eval(*arg)?;
                Ok(json_get(&key, val).unwrap_or(Json::Null))
            }
            Cmd::Insert(key, arg) => eval_insert(self, &key, arg),
            Cmd::Json(val) => Ok(val),
            Cmd::Keys(page) => Ok(Json::Array(self.keys(page))),
            Cmd::Last(arg) => self.eval_unr_fn(*arg, &|x| {
                let val = json_last(x).cloned().unwrap_or(Json::Null);
                Ok(val)
            }),
            Cmd::Max(arg) => eval_max(self, *arg),
            Cmd::In(lhs, rhs) => eval_bin_fn(self, *lhs, *rhs, &|x, y| Ok(json_in(x, y))),
            Cmd::Min(arg) => eval_min(self, *arg),
            Cmd::Mul(lhs, rhs) => eval_bin_fn(self, *lhs, *rhs, &|x, y| json_mul(x, y)),
            Cmd::Push(key, arg) => eval_push(self, &key, *arg),
            Cmd::Pop(_key) => {
                //let val = pop(self, key)?;
                //Ok(val.unwrap_or(Json::Null))
                unimplemented!()
            }
            Cmd::Set(key, arg) => {
                let val = self.eval(*arg)?;
                if let Some(val) = self.set(key, val) {
                    Ok(val)
                } else {
                    Ok(Json::Null)
                }
            }
            Cmd::Slice(arg, range) => {
                let val: Json = self.eval(*arg)?;
                json_slice(val, range)
            }
            Cmd::Sort(arg, _) => eval_sort_cmd(self, *arg),
            Cmd::Dev(arg) => self.eval_unr_fn(*arg, &|x| json_dev(x)),
            Cmd::Sub(lhs, rhs) => eval_bin_fn(self, *lhs, *rhs, &|x, y| json_sub(x, y)),
            Cmd::Sum(arg) => self.eval_unr_fn(*arg, &|x| Ok(json_sum(x))),
            Cmd::Summary => Ok(self.summary()),
            Cmd::Unique(arg) => self.eval_unr_fn(*arg, &|x| unique(x)),
            Cmd::Var(arg) => self.eval_unr_fn(*arg, &|x| json_var(x)),
            Cmd::ToString(arg) => Ok(self.eval(*arg)?),
            Cmd::Key(key) => {
                let keys: Vec<&str> = key.split('.').collect();
                let val = self.get(keys[0]).map(|x| x.clone())?;
                Ok(eval_nested_key(val, &keys[1..]).unwrap_or(Json::Null))
            }
            Cmd::Reverse(arg) => eval_reverse(self, *arg),
            Cmd::Median(arg) => eval_median(self, *arg),
            Cmd::SortBy(arg, key) => eval_sortby(self, *arg, key),
            Cmd::Eval(cmd) => {
                let val = self.eval(*cmd)?;
                let cmd = Cmd::parse(val)?;
                self.eval(cmd)
            }
            Cmd::Eq(lhs, rhs) => eval_eq(self, *lhs, *rhs),
            Cmd::NotEq(lhs, rhs) => eval_not_eq(self, *lhs, *rhs),
            Cmd::Gt(lhs, rhs) => eval_gt(self, *lhs, *rhs),
            Cmd::Lt(lhs, rhs) => eval_lt(self, *lhs, *rhs),
            Cmd::Gte(lhs, rhs) => eval_gte(self, *lhs, *rhs),
            Cmd::Lte(lhs, rhs) => eval_lte(self, *lhs, *rhs),
            Cmd::And(lhs, rhs) => eval_and(self, *lhs, *rhs),
            Cmd::Or(lhs, rhs) => eval_or(self, *lhs, *rhs),
            Cmd::Map(arg, f) => eval_map(self, *arg, f),
            Cmd::Flat(arg) => eval_flat(self, *arg),
            Cmd::NumSort(arg, descend) => eval_numsort(self, *arg, descend),
            Cmd::Has(key) => Ok(Json::Bool(self.has(&key))),
            Cmd::InnerJoin(_lhs, _rhs, _left_key, _right_key, _prop_key) => unimplemented!(),
            Cmd::OuterJoin(_lhs, _rhs, _left_key, _right_key, _prop_key) => unimplemented!(),
            Cmd::Upsert(key, arg) => eval_upsert(self, key, *arg),
        }
    }

    pub fn eval_unr_fn(
        &mut self,
        arg: Cmd,
        f: &dyn Fn(&Json) -> Result<Json, Error>,
    ) -> Result<Json, Error> {
        let val: Json = self.eval(arg)?;
        //Ok(JsonVal::Ref(val_ref))
        f(&val)
    }

    /// create a new instance of the in-memory database with no entries
    pub fn new() -> Self {
        Self {
            cache: Cache::new(),
        }
    }

    /// retrieves a key/val entry and if not present, it inserts an entry
    pub fn entry<K: Into<String>>(&mut self, key: K) -> &Json {
        self.cache.entry(key.into()).or_insert_with(|| Json::Null)
    }

    /// summary of keys stored and no. of entries
    pub fn summary(&self) -> Json {
        let no_entries = Json::from(self.cache.len());
        let keys: Vec<Json> = self
            .cache
            .keys()
            .map(|x| Json::String(x.to_string()))
            .collect();
        json!({"no_entries": no_entries, "keys": keys})
    }
}

impl Default for InMemDb {
    fn default() -> Self {
        InMemDb::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_approx_eq::assert_approx_eq;

    use serde_json::{self, json, Value as Json};

    fn set<K: Into<String>>(key: K, arg: Cmd) -> Cmd {
        Cmd::Set(key.into(), b(arg))
    }

    fn key<K: Into<String>>(k: K) -> Cmd {
        Cmd::Key(k.into())
    }

    fn eval(arg: Cmd) -> Cmd {
        Cmd::Eval(b(arg))
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

    fn eval_cmd<'a>(db: &'a mut InMemDb, cmd: Cmd) -> Result<Json, Error> {
        insert_data(db);
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
        //db.set("t", table_data());
        db.set("n", Json::Null);
        db.set("orders", orders_val());
        db.set("cmd", json!({"+": [{"key": "x"}, {"key": "y"}]}));
    }

    /*
    #[test]
    fn select_all_from_orders() {
        let exp = json!([
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
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
    */

    /*
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
    */
    /*
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
    */
    #[test]
    fn test_first() {
        let mut db = test_db();
        assert_eq!(Ok(json!(true)), eval_cmd(&mut db, first(key("b"))));
        assert_eq!(Ok(json!(true)), eval_cmd(&mut db, first(key("b"))));
        assert_eq!(Ok(json!(3.3)), eval_cmd(&mut db, first(key("f"))));
        assert_eq!(Ok(json!(2)), eval_cmd(&mut db, first(key("i"))));
        assert_eq!(Ok(json!(1.1)), eval_cmd(&mut db, first(key("fa"))));
        assert_eq!(Ok(json!(1)), eval_cmd(&mut db, first(key("ia"))));
    }

    #[test]
    fn test_last() {
        let mut db = test_db();
        assert_eq!(Ok(json!(true)), eval_cmd(&mut db, last(key("b"))));
        assert_eq!(Ok(json!(3.3)), eval_cmd(&mut db, last(key("f"))));
        assert_eq!(Ok(json!(2)), eval_cmd(&mut db, last(key("i"))));
        assert_eq!(Ok(json!(5.5)), eval_cmd(&mut db, last(key("fa"))));
        assert_eq!(Ok(json!(5)), eval_cmd(&mut db, last(key("ia"))));
    }

    #[test]
    fn test_max() {
        let mut db = test_db();
        assert_eq!(Ok(json!(true)), eval_cmd(&mut db, max(key("b"))));
        assert_eq!(Ok(json!(2)), eval_cmd(&mut db, max(key("i"))));
        assert_eq!(Ok(json!(3.3)), eval_cmd(&mut db, max(key("f"))));
        assert_eq!(Ok(json!(5)), eval_cmd(&mut db, max(key("ia"))));
        assert_eq!(Ok(json!(5.5)), eval_cmd(&mut db, max(key("fa"))));
    }

    #[test]
    fn test_min() {
        let mut db = test_db();
        assert_eq!(Ok(json!(true)), eval_cmd(&mut db, min(key("b"))));
        assert_eq!(Ok(json!(2)), eval_cmd(&mut db, min(key("i"))));
        assert_eq!(Ok(json!(3.3)), eval_cmd(&mut db, min(key("f"))));
        assert_eq!(Ok(json!(1.1)), eval_cmd(&mut db, min(key("fa"))));
        assert_eq!(Ok(json!(1)), eval_cmd(&mut db, min(key("ia"))));
    }

    #[test]
    fn test_avg() {
        let mut db = test_db();
        assert_eq!(Ok(json!(3.3)), eval_cmd(&mut db, avg(key("f"))));
        assert_eq!(Ok(json!(2)), eval_cmd(&mut db, avg(key("i"))));
        assert_eq!(Ok(json!(3.3)), eval_cmd(&mut db, avg(key("fa"))));
        assert_eq!(Ok(json!(3.0)), eval_cmd(&mut db, avg(key("ia"))));
    }

    #[test]
    fn test_var() {
        let mut db = test_db();
        assert_eq!(Ok(json!(0)), eval_cmd(&mut db, var(key("f"))));
        assert_eq!(Ok(json!(0)), eval_cmd(&mut db, var(key("i"))));
        let val = eval_cmd(&mut db, var(key("fa"))).unwrap().as_f64().unwrap();
        assert_approx_eq!(3.10, val, 0.0249f64);
        let val = eval_cmd(&mut db, var(key("ia"))).unwrap().as_f64().unwrap();
        assert_approx_eq!(2.56, val, 0.0249f64);
    }

    #[test]
    fn test_dev() {
        let mut db = test_db();
        assert_eq!(Ok(json!(0)), eval_cmd(&mut db, dev(key("f"))));
        assert_eq!(Ok(json!(0)), eval_cmd(&mut db, dev(key("i"))));
        let val = eval_cmd(&mut db, dev(key("fa"))).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.55, val, 0.03f64);
        let val = eval_cmd(&mut db, dev(key("ia"))).unwrap().as_f64().unwrap();
        assert_approx_eq!(1.414, val, 0.03f64);
    }

    #[test]
    fn test_add() {
        let mut db = test_db();
        assert_eq!(Ok(json!(9)), eval_cmd(&mut db, add(key("x"), key("y"))));
        assert_eq!(
            Ok(json!([5, 6, 7, 8, 9,])),
            eval_cmd(&mut db, add(key("x"), key("ia")))
        );

        assert_eq!(
            Ok(json!([3, 4, 5, 6, 7,])),
            eval_cmd(&mut db, add(key("ia"), key("i")))
        );

        assert_eq!(
            Ok(json!([Json::Null, 4, Json::Null, 6, 7,])),
            eval_cmd(&mut db, add(key("nia"), key("i")))
        );

        assert_eq!(
            Ok(json!([Json::Null, Json::Null, Json::Null, Json::Null, 10,])),
            eval_cmd(&mut db, add(key("nia"), key("nfa")))
        );

        assert_eq!(
            Ok(json!(["ahello", "bhello", "chello", "dhello",])),
            eval_cmd(&mut db, add(key("sa"), key("s")))
        );
        assert_eq!(
            Ok(json!(["helloa", "hellob", "helloc", "hellod",])),
            eval_cmd(&mut db, add(key("s"), key("sa")))
        );

        assert_eq!(
            Ok(json!("hellohello".to_string())),
            eval_cmd(&mut db, add(key("s"), key("s")))
        );
        assert_eq!(
            Ok(json!("hello3.3")),
            eval_cmd(&mut db, add(key("s"), key("f")))
        );
        assert_eq!(
            Ok(json!("3.3hello")),
            eval_cmd(&mut db, add(key("f"), key("s")))
        );
        assert_eq!(
            Ok(json!("2hello")),
            eval_cmd(&mut db, add(key("i"), key("s")))
        );
        assert_eq!(
            Ok(json!("hello2")),
            eval_cmd(&mut db, add(key("s"), key("i")))
        );
    }

    #[test]
    fn test_sub() {
        let mut db = test_db();
        assert_eq!(Ok(json!(-1)), eval_cmd(&mut db, sub(key("x"), key("y"))));
        assert_eq!(Ok(json!(1)), eval_cmd(&mut db, sub(key("y"), key("x"))));
        assert_eq!(
            Ok(json!([3, 2, 1, 0, -1,])),
            eval_cmd(&mut db, sub(key("x"), key("ia")))
        );

        assert_eq!(
            Ok(json!([-4, -3, -2, -1, 0,])),
            eval_cmd(&mut db, sub(key("ia"), key("y")))
        );

        assert_eq!(
            Ok(json!([0, 0, 0, 0, 0,])),
            eval_cmd(&mut db, sub(key("ia"), key("ia")))
        );

        assert_eq!(bad_type(), eval_cmd(&mut db, sub(key("s"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, sub(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, sub(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, sub(key("i"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, sub(key("s"), key("i"))));
    }

    #[test]
    fn json_mul() {
        let mut db = test_db();
        assert_eq!(Ok(json!(20)), eval_cmd(&mut db, mul(key("x"), key("y"))));
        assert_eq!(Ok(json!(16)), eval_cmd(&mut db, mul(key("x"), key("x"))));
        let arr = json!([5, 10, 15, 20, 25,]);
        assert_eq!(
            Ok(json!(arr.clone())),
            eval_cmd(&mut db, mul(key("ia"), key("y")))
        );
        assert_eq!(Ok(json!(arr)), eval_cmd(&mut db, mul(key("y"), key("ia"))));
        assert_eq!(
            Ok(json!([1, 4, 9, 16, 25,])),
            eval_cmd(&mut db, mul(key("ia"), key("ia")))
        );
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("s"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("i"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("s"), key("i"))));
    }

    #[test]
    fn json_div() {
        let mut db = test_db();
        assert_eq!(Ok(json!(1.0)), eval_cmd(&mut db, div(key("x"), key("x"))));
        assert_eq!(Ok(json!(1.0)), eval_cmd(&mut db, div(key("y"), key("y"))));
        assert_eq!(
            Ok(json!([1.0, 1.0, 1.0, 1.0, 1.0,])),
            eval_cmd(&mut db, div(key("ia"), key("ia")))
        );
        assert_eq!(
            Ok(json!([0.5, 1.0, 1.5, 2.0, 2.5,])),
            eval_cmd(&mut db, div(key("ia"), key("i")))
        );
        assert_eq!(
            Ok(json!([2.0, 1.0, 0.6666666666666666, 0.5, 0.4,])),
            eval_cmd(&mut db, div(key("i"), key("ia")))
        );

        assert_eq!(bad_type(), eval_cmd(&mut db, div(key("s"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, div(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, div(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, div(key("i"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, div(key("s"), key("i"))));
    }

    #[test]
    fn eval_cmds() {
        let mut db = test_db();
        assert_eq!(Ok(json!(true)), eval_cmd(&mut db, key("b")));
        assert_eq!(eval_cmd(&mut db, key("ia")), Ok(json!([1, 2, 3, 4, 5,])));

        assert_eq!(eval_cmd(&mut db, key("i")), Ok(json!(2)));
        assert_eq!(eval_cmd(&mut db, key("f")), Ok(json!(3.3)));
        assert_eq!(
            eval_cmd(&mut db, key("fa")),
            Ok(json!([1.1, 2.2, 3.3, 4.4, 5.5,]))
        );
        assert_eq!(eval_cmd(&mut db, key("f")), Ok(json!(3.3)));
        assert_eq!(eval_cmd(&mut db, key("s")), Ok(json!("hello")));
        assert_eq!(
            eval_cmd(&mut db, key("sa")),
            Ok(json!(["a", "b", "c", "d",]))
        );
    }

    #[test]
    fn test_key() {
        let mut db = test_db();
        assert_eq!(Ok(json!(true)), eval_cmd(&mut db, key("b")));
        assert_eq!(Ok(json!(true)), eval_cmd(&mut db, key("b")));
        assert_eq!(Ok(json!([1, 2, 3, 4, 5,])), eval_cmd(&mut db, key("ia")));
        assert_eq!(Ok(json!(2)), eval_cmd(&mut db, key("i")));
    }

    #[test]
    fn eval_set_get_ok() {
        let v = json!([1, 2, 3, 4, 5]);
        let mut db = test_db();
        assert_eq!(Ok(Json::Null), db.eval(set("nums", Cmd::Json(v.clone()))));
        assert_eq!(Ok(v), db.eval(key("nums")));
    }

    #[test]
    fn eval_get_string_err_not_found() {
        let mut db = test_db();
        assert_eq!(
            Err(Error::BadKey("ania".to_string())),
            eval_cmd(&mut db, key("ania"))
        );
    }

    #[test]
    fn nested_get() {
        let mut db = test_db();
        db.set(
            "order",
            json!({"address": {"line1": "street 1", "line2": "street 2"}}),
        );
        let act = eval_cmd(&mut db, get("name", key("order.address.line1"))).unwrap();
        assert_eq!(json!("street 1"), act);
    }

    fn test_db() -> InMemDb {
        let mut db = InMemDb::new();
        insert_data(&mut db);
        db
    }
    /*
        #[test]
        fn select_all_query() {
            let qry = query(json!({"from":  "t"}));
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
    */
    /*
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
    */
    /*
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
    */
    /*
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
    */
    /*
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
    */
    /*
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

    */
    /*
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
    } */
    /*
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
    */
    /*
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
    } */

    /*
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
    } */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    /*
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
    */

    #[test]
    fn eval_mul() {
        let mut db = InMemDb::new();
        assert_eq!(Ok(json!(20)), eval_cmd(&mut db, mul(key("x"), key("y"))));
        assert_eq!(Ok(json!(16)), eval_cmd(&mut db, mul(key("x"), key("x"))));
        let arr = json!([5, 10, 15, 20, 25,]);
        assert_eq!(Ok(arr.clone()), eval_cmd(&mut db, mul(key("ia"), key("y"))));
        assert_eq!(Ok(arr), eval_cmd(&mut db, mul(key("y"), key("ia"))));
        assert_eq!(
            Ok(json!([1, 4, 9, 16, 25,])),
            eval_cmd(&mut db, mul(key("ia"), key("ia")))
        );
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("s"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("sa"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("s"), key("sa"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("i"), key("s"))));
        assert_eq!(bad_type(), eval_cmd(&mut db, mul(key("s"), key("i"))));
    }

    #[test]
    fn eval_map_ok() {
        let mut db = InMemDb::new();
        let cmd = Cmd::Map(
            Box::new(Cmd::Json(json!([1, 2, 3, 4, 5]))),
            "len".to_string(),
        );
        assert_eq!(Ok(json!([1, 1, 1, 1, 1])), eval_cmd(&mut db, cmd));
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
        let mut db = InMemDb::new();
        assert_eq!(Ok(json!(1.0)), eval_cmd(&mut db, div(key("x"), key("x"))));
        assert_eq!(Ok(json!(1.0)), eval_cmd(&mut db, div(key("y"), key("y"))));
        assert_eq!(
            Ok(json!([1.0, 1.0, 1.0, 1.0, 1.0,])),
            eval_cmd(&mut db, div(key("ia"), key("ia")))
        );
        assert_eq!(
            Ok(json!([0.5, 1.0, 1.5, 2.0, 2.5,])),
            eval_cmd(&mut db, div(key("ia"), key("i")))
        );
        assert_eq!(
            Ok(json!([2.0, 1.0, 0.6666666666666666, 0.5, 0.4,])),
            eval_cmd(&mut db, div(key("i"), key("ia")))
        );

        assert_eq!(
            Error::BadType,
            eval_cmd(&mut db, div(key("s"), key("s"))).unwrap_err()
        );
        assert_eq!(
            Error::BadType,
            eval_cmd(&mut db, div(key("sa"), key("s"))).unwrap_err()
        );
        assert_eq!(
            Error::BadType,
            eval_cmd(&mut db, div(key("s"), key("sa"))).unwrap_err()
        );
        assert_eq!(
            Error::BadType,
            eval_cmd(&mut db, div(key("i"), key("s"))).unwrap_err()
        );
        assert_eq!(
            Error::BadType,
            eval_cmd(&mut db, div(key("s"), key("i"))).unwrap_err()
        );
    }

    #[test]
    fn test_eval_key() {
        let cmd = eval(key("cmd"));
        let mut db = test_db();
        assert_eq!(Ok(json!(9)), eval_cmd(&mut db, cmd));
    }
}

pub fn eval_upsert(db: &mut InMemDb, key: String, cmd: Cmd) -> Result<Json, Error> {
    let val = db.eval(cmd)?;
    match db.get_mut(&key) {
        Ok(rval) => {
            json_upsert(rval, val);
            Ok(Json::Null)
        }
        Err(_) => match db.set(key, val) {
            Some(val) => Ok(val),
            None => Ok(Json::Null),
        },
    }
}

pub fn eval_oj(
    db: &mut InMemDb,
    lhs: Cmd,
    lhs_key: &str,
    rhs: Cmd,
    rhs_key: &str,
    key: &str,
) -> Result<Json, Error> {
    let lv = db.eval(lhs)?;
    let rv = db.eval(rhs)?;
    match (lv.as_array(), rv.as_array()) {
        (Some(l), Some(r)) => Ok(json_outerjoin(l, lhs_key, r, rhs_key, key)),
        _ => Err(Error::BadType),
    }
}

pub fn eval_ij(
    db: &mut InMemDb,
    lhs: Cmd,
    lhs_key: &str,
    rhs: Cmd,
    rhs_key: &str,
    key: &str,
) -> Result<Json, Error> {
    let lv = db.eval(lhs)?;
    let rv = db.eval(rhs)?;
    match (lv.as_array(), rv.as_array()) {
        (Some(l), Some(r)) => Ok(json_innerjoin(l, lhs_key, r, rhs_key, key)),
        _ => Err(Error::BadType),
    }
}

pub fn eval_max(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    let max_val = json_max(&val);
    let val: Json = if let Some(val) = max_val {
        val.clone()
    } else {
        Json::Null
    };
    Ok(val)
}

pub fn eval_min(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    let min_val = json_min(&val);
    let val = if let Some(val) = min_val {
        val.clone()
    } else {
        Json::Null
    };
    Ok(val)
}

// evaluate binary function (a fn with 2 args)
pub fn eval_bin_fn(
    db: &mut InMemDb,
    lhs: Cmd,
    rhs: Cmd,
    f: &dyn Fn(&Json, &Json) -> Result<Json, Error>,
) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    f(&x, &y)
}

/// evaluate unary function (a fn with 1 arg)

pub fn eval_nested_key(val: Json, keys: &[&str]) -> Option<Json> {
    let mut opt_val = Some(val);
    for key in keys {
        if let Some(v) = opt_val {
            println!("v={:?}\nkey={:?}", v, key);
            opt_val = eval_key(&v, key)
        } else {
            return None;
        }
    }
    opt_val
}

/// evaluate the key command
pub fn eval_keys(db: &InMemDb, keys: &[&str]) -> Option<Json> {
    let key = keys[0];
    let val = db.key(key)?;
    eval_nested_key(val.clone(), &keys[1..])
}

fn eval_key(val: &Json, key: &str) -> Option<Json> {
    match val {
        Json::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for val in arr {
                let v: Json = val.get(key).cloned().unwrap_or(Json::Null);
                out.push(v);
            }
            Some(Json::from(out))
        }
        val => val.get(key).cloned(),
    }
}

pub fn eval_append(db: &mut InMemDb, key: &str, arg: Cmd) -> Result<Json, Error> {
    let elem = db.eval(arg)?;
    let val = db.get_mut(key)?;
    json_append(val, elem);
    Ok(Json::Null)
}

/// evaluate the sort command
pub fn eval_sort_cmd(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let mut val: Json = db.eval(arg)?;
    json_sort(&mut val, false);
    Ok(val)
}

/// evaluate the insert command
pub fn eval_insert(db: &mut InMemDb, key: &str, arg: Vec<JsonObj>) -> Result<Json, Error> {
    let val = db.get_mut(key)?;
    let n = arg.len();
    json_insert(val, arg);
    Ok(Json::from(n))
}

pub fn eval_push(db: &mut InMemDb, key: &str, arg: Cmd) -> Result<Json, Error> {
    let val: Json = db.eval(arg)?;
    let kv = db.get_mut(key)?;
    json_push(kv, val);
    Ok(Json::Null)
}

pub fn eval_reverse(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let mut val: Json = db.eval(arg)?;
    json_reverse(&mut val);
    Ok(val)
}

pub fn eval_map(db: &mut InMemDb, arg: Cmd, key: String) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    json_map(&val, key)
}

pub fn eval_flat(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    let val: Json = json_flat(&val);
    Ok(val)
}

pub fn eval_numsort(db: &mut InMemDb, arg: Cmd, descend: bool) -> Result<Json, Error> {
    let val: Json = db.eval(arg)?;
    Ok(json_numsort(val, descend))
}

pub fn eval_median(db: &mut InMemDb, arg: Cmd) -> Result<Json, Error> {
    let val = db.eval(arg)?;
    json_median(&val)
}

pub fn eval_sortby(db: &mut InMemDb, arg: Cmd, key: String) -> Result<Json, Error> {
    let mut val: Json = db.eval(arg)?;
    json_sortby(&mut val, &key);
    Ok(val)
}

pub fn eval_eq(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let lhs = db.eval(lhs)?;
    let rhs = db.eval(rhs)?;
    Ok(json_eq(&lhs, &rhs))
}

pub fn eval_not_eq(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(json_not_eq(&x, &y))
}

pub fn eval_lt(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(json_lt(&x, &y))
}

pub fn eval_gt(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    Ok(json_gt(&x, &y))
}

pub fn eval_lte(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_lte(&x, &y);
    Ok(val)
}

pub fn eval_gte(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val: Json = json_gte(&x, &y);
    Ok(val)
}

pub fn eval_and(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_lte(&x, &y);
    Ok(val)
}

pub fn eval_or(db: &mut InMemDb, lhs: Cmd, rhs: Cmd) -> Result<Json, Error> {
    let x = db.eval(lhs)?;
    let y = db.eval(rhs)?;
    let val = json_gte(&x, &y);
    Ok(val)
}

#[cfg(test)]
mod evaltests {
    use super::*;

    use serde_json::json;

    #[test]
    pub fn eval_in() {
        let mut db = InMemDb::new();
        db.set("nums".to_string(), json!([1, 2, 3, 4, 5]));
        let cmd = Cmd::In(
            Box::new(Cmd::Key("nums".to_string())),
            Box::new(Cmd::Json(json!(3))),
        );
        assert_eq!(Ok(json!([false, false, true, false, false])), db.eval(cmd));
    }

    #[test]
    fn test_eval_nested_key() {
        let _it = "address.line1".split('.');
        let val = json!({"a": 1});
        assert_eq!(None, eval_nested_key(val, &["b"]));

        let val = json!({"orders": [1,2,3,4,5]});
        assert_eq!(
            Some(json!([1, 2, 3, 4, 5])),
            eval_nested_key(val, &["orders"])
        );

        let val = json!({"orders": [
            {"qty": 1},
            {"qty": 2},
            {"qty": 3},
            {"qty": 4},
            {"qty": 5},
        ]});
        assert_eq!(
            Some(json!([1, 2, 3, 4, 5])),
            eval_nested_key(val, &["orders", "qty"])
        );

        let val = json!({"orders": [
            {"customer": {"name": "alice"}},
            {"customer": {"name": "bob"}},
            {"customer": {"name": "charles"}},
            {"customer": {"name": "dave"}},
            {"customer": {"name": "ewa"}},
            {"customer": {"surname": "perry"}},
            {"foo": {"bar": "baz"}},
        ]});
        assert_eq!(
            Some(json!([
                "alice",
                "bob",
                "charles",
                "dave",
                "ewa",
                Json::Null,
                Json::Null
            ])),
            eval_nested_key(val, &["orders", "customer", "name"])
        );
    }

    #[test]
    fn test_eval_ij() {
        let mut db = InMemDb::new();
        db.set("a", json!([{"a": 1}, {"a": 2}, {"a": 3}]));
        db.set("b", json!([{"b": 1}, {"b": 2}]));
        let lhs = Cmd::Key("a".to_string());
        let rhs = Cmd::Key("b".to_string());
        assert_eq!(
            Ok(json!([{"a": 1, "c": {"b": 1}}, {"a": 2, "c": {"b": 2}}])),
            eval_ij(&mut db, lhs, "a", rhs, "b", "c")
        );
    }

    #[test]
    fn test_eval_oj() {
        let mut db = InMemDb::new();
        db.set("a", json!([{"a": 1}, {"a": 2}, {"a": 3}]));
        db.set("b", json!([{"b": 1}, {"b": 2}]));
        let lhs = Cmd::Key("a".to_string());
        let rhs = Cmd::Key("b".to_string());
        assert_eq!(
            Ok(json!([{"a": 1, "c": {"b": 1}}, {"a": 2, "c": {"b": 2}}, {"a": 3}])),
            eval_oj(&mut db, lhs, "a", rhs, "b", "c")
        );
    }
}
