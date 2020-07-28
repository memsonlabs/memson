use crate::db::Cache;
use crate::Res;
use crate::json::Cmd;
use std::fs::{File,OpenOptions};
use std::path::Path;
use std::io::{self, BufRead, BufReader, Write};
use serde_json::Value as JsonVal;

/// The replay log that records all mututations
/// 
pub struct ReplayLog {
    file: File,
}

impl ReplayLog {
    pub fn open<P:AsRef<Path>>(path: P) -> io::Result<ReplayLog> {
        let file = OpenOptions::new()
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .create(true)                    
                    .open(path)?;
        Ok(ReplayLog{ file })
    }

    pub fn write(&mut self, key: &str, val: &JsonVal) -> io::Result<()> {
        let line = "{\"set\":[\"".to_string() + key + "\"," + &val.to_string() + "]}\n";
        self.file.write_all(line.as_bytes())?;
        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> io::Result<()> {
        let line = "{\"del\": \"".to_string() + key + "\"}\n";
        self.file.write_all(line.as_bytes())
    }

    pub fn replay(&mut self) -> Res<Cache> {
        let buf = Box::new(BufReader::new(&mut self.file));
        let mut cache = Cache::new();
        for line in buf.lines() {
            let line = line.map_err(|err| {eprintln!("{:?}", err); "bad line"})?;
            let val: Cmd = serde_json::from_str(&line).map_err(|err| {println!("{:?}", err); "bad json"})?;

            match val {
                Cmd::Set(key, val) => {
                    cache.insert(key, val);
                }
                _ => return Err("unexpected json"),
            }            
        }
        Ok(cache)
    }
}