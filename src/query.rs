use crate::cmd::{Cmd, Filter};
use crate::err::Error;
use crate::inmemdb::InMemDb;
use crate::json::JsonObj;
use crate::json::{json_first, json_gt, json_last, json_max, json_mul, json_str, json_sum, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Query {
    select: Option<HashMap<String, Cmd>>,
    from: String,
    by: Option<Cmd>,
    filter: Option<Filter>,
}

impl Query {
    pub fn exec(&self, db: &InMemDb) -> Result<Json, Error> {
        let val = db.get(&self.from)?;
        if let Some(rows) = val.as_array() {
            if let Some(by) = &self.by {
                self.eval_grouped_select(by, rows)
            } else {
                self.eval_select(rows)
            }
        } else {
            unimplemented!()
        }
    }

    fn eval_grouped_select(&self, by: &Cmd, rows: &[Json]) -> Result<Json, Error> {
        let grouping = match by {
            Cmd::Key(key) => {
                let mut g = HashMap::new();
                for row in rows {
                    if let Some(key) = row.get(key) {
                        let entry = g.entry(json_str(key)).or_insert_with(|| Vec::new());
                        entry.push(row.clone());
                    }
                }
                g
            }
            _ => unimplemented!(),
        };
        if let Some(selects) = &self.select {
            let mut keyed_obj = JsonObj::new();
            for (key, keyed_rows) in grouping.into_iter() {
                let mut obj = JsonObj::new();
                for (col, cmd) in selects {
                    if let Some(v) = eval_reduce_cmd(cmd, &keyed_rows) {
                        obj.insert(col.to_string(), v);
                    }
                }
                keyed_obj.insert(key, Json::from(obj));
            }
            Ok(Json::from(keyed_obj))
        } else {
            let mut obj = JsonObj::new();
            for (k, v) in grouping.into_iter() {
                obj.insert(k, v.into_iter().map(Json::from).collect());
            }
            Ok(Json::from(obj))
        }
    }

    fn eval_select(&self, rows: &[Json]) -> Result<Json, Error> {
        let mut out = Vec::new();
        if let Some(selects) = &self.select {
            let mut aggs = JsonObj::new();
            for (col, cmd) in selects {
                if cmd.is_aggregate() {
                    if let Some(val) = eval_reduce_cmd(cmd, rows) {
                        aggs.insert(col.to_string(), val);
                    }
                }
            }
            if !aggs.is_empty() {
                out.push(Json::from(aggs));
            }
            for row in rows {
                if let Some(val) = self.eval_selects_row(selects, row) {
                    out.push(val);
                }
            }
        } else {
            for row in rows {
                out.push(Json::from(row.clone()));
            }
        }
        Ok(Json::Array(out))
    }

    fn eval_selects_row(&self, selects: &HashMap<String, Cmd>, row: &Json) -> Option<Json> {
        let mut out = JsonObj::new();
        for (col, cmd) in selects {
            match cmd {
                Cmd::Key(key) => {
                    if let Some(val) = row.get(key) {
                        out.insert(col.to_string(), val.clone());
                    }
                }
                Cmd::Mul(lhs, rhs) => {
                    let lhs = match lhs.as_ref() {
                        Cmd::Key(key) => row.get(key),
                        _ => unimplemented!(),
                    };
                    let rhs = match rhs.as_ref() {
                        Cmd::Key(key) => row.get(key),
                        _ => unimplemented!(),
                    };
                    match (lhs, rhs) {
                        (Some(x), Some(y)) => {
                            if let Some(val) = json_mul(x, y).ok() {
                                out.insert(col.to_string(), val);
                            }
                        }
                        _ => (),
                    }
                }
                _ => return None,
            }
        }
        Some(Json::from(out))
    }
}

fn eval_reduce_cmd(cmd: &Cmd, rows: &[Json]) -> Option<Json> {
    match cmd {
        Cmd::Unique(arg) => match arg.as_ref() {
            Cmd::Key(key) => {
                let mut unique: Vec<Json> = Vec::new();
                for row in rows {
                    if let Some(val) = row.get(key) {
                        if !unique.contains(val) {
                            unique.push(val.clone());
                        }
                    }
                }
                Some(Json::from(unique))
            }
            _ => unimplemented!(),
        },
        Cmd::Len(arg) => match arg.as_ref() {
            Cmd::Key(key) => {
                let mut count = 0usize;
                for row in rows {
                    if let Some(_) = row.get(key) {
                        count += 1;
                    }
                }
                Some(Json::from(count))
            }
            _ => unimplemented!(),
        },
        Cmd::Mul(lhs, rhs) => match (lhs.as_ref(), rhs.as_ref()) {
            (Cmd::Key(lhs), Cmd::Key(rhs)) => {
                let mut out = Vec::new();
                for row in rows {
                    match (row.get(lhs), row.get(rhs)) {
                        (Some(x), Some(y)) => {
                            if let Some(val) = json_mul(x, y).ok() {
                                out.push(val);
                            }
                        }
                        _ => unimplemented!(),
                    }
                }
                Some(Json::from(out))
            }
            (lhs, rhs) => {
                let x = eval_reduce_cmd(lhs, rows);
                let y = eval_reduce_cmd(rhs, rows);
                match (x, y) {
                    (Some(x), Some(y)) => {
                        if let Some(val) = json_mul(&x, &y).ok() {
                            Some(val)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
        },
        Cmd::Sum(arg) => match arg.as_ref() {
            Cmd::Key(_) => unimplemented!(),
            arg => eval_reduce_cmd(arg, rows).map(|x| json_sum(&x)),
        },
        Cmd::Max(arg) => match arg.as_ref() {
            Cmd::Key(key) => {
                let mut max = None;
                for row in rows {
                    if let Some(y) = row.get(key) {
                        max = if let Some(x) = max {
                            Some(if json_gt(x, y) { x } else { y })
                        } else {
                            Some(y)
                        }
                    }
                }
                max.cloned()
            }
            arg => eval_reduce_cmd(arg, rows).map(|x| json_max(&x).clone()),
        },
        Cmd::First(arg) => match arg.as_ref() {
            Cmd::Key(key) => {
                if let Some(row) = rows.get(0) {
                    row.get(key).cloned()
                } else {
                    None
                }
            }
            arg => eval_reduce_cmd(arg, rows).map(|x| json_first(&x).clone()),
        },
        Cmd::Last(arg) => match arg.as_ref() {
            Cmd::Key(key) => {
                if rows.is_empty() {
                    return None;
                }
                if let Some(row) = rows.get(rows.len() - 1) {
                    row.get(key).cloned()
                } else {
                    None
                }
            }
            arg => eval_reduce_cmd(arg, rows).map(|x| json_last(&x).clone()),
        },
        _ => unimplemented!(),
    }
}

mod tests {

    use super::*;
    use crate::json::Json;
    use serde_json::json;

    fn test_db() -> InMemDb {
        let mut db = InMemDb::new();
        let orders = json!([
            { "time": 0, "customer": "james", "qty": 2, "price": 9.0, "discount": 10 },
            { "time": 1, "customer": "ania", "qty": 2, "price": 2.0 },
            { "time": 2, "customer": "misha", "qty": 4, "price": 1.0 },
            { "time": 3, "customer": "james", "qty": 10, "price": 16.0, "discount": 20 },
            { "time": 4, "customer": "james", "qty": 1, "price": 16.0 },
        ]);
        db.set("orders", orders);
        db
    }

    fn query(val: Json) -> Result<Json, Error> {
        let db = test_db();
        let qry: Query = serde_json::from_value(val).unwrap();
        qry.exec(&db)
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
            { "countDiscount": 2, "countCustomer": 5},
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
}
