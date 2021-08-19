
use crate::json::json_len;
use crate::Error;
use crate::json::Json;
use crate::cmd::Cmd;
use crate::inmem::InMemDb;

pub struct Query {
    selects: Vec<Cmd>,
    from: Cmd,
    filters: Option<Vec<Cmd>>,
}

impl Query {
    pub fn exec(&self, db: &mut InMemDb) -> Result<Json, Error> {
        let val = db.eval(self.from.clone())?;
        let val = self.filter(db, val)?;
        Ok(val)
    }

    fn filter(&self, db: &mut InMemDb, val: Json) -> Result<Json, Error> {
        let gate: Vec<bool> = Vec::with_capacity(json_len(&val));
        
        Ok(val)
    }
}