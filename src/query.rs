pub struct Query<'a> {
    pub(crate) db: &'a InMemDb,
    pub(crate) cmd: QueryCmd,
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
    fn eval_db_rows(&'a self) -> Result<&'a [Value], Error> {
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
