use crate::cmd::WriteCmd;
use crate::rdb::InMemDb;
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, LineWriter, Write};
use std::path::Path;

#[derive(Debug)]
pub struct ReplayLog {
    writer: LineWriter<File>,
}

fn replay(file: &mut File) -> io::Result<InMemDb> {
    let buf = BufReader::new(file);
    let mut data = BTreeMap::new();
    for line in buf.lines() {
        let line = line?;
        let cmd: WriteCmd = serde_json::from_str(&line)?;
        match cmd {
            WriteCmd::Set(key, val) => {
                data.insert(key, val);
            }
            _ => panic!("only set commands"),
        }
    }
    Ok(InMemDb::from(data))
}

impl ReplayLog {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<(Self, InMemDb)> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let db = replay(&mut file)?;
        let writer = LineWriter::new(file);
        Ok((Self { writer }, db))
    }

    pub fn write(&mut self, cmd: &WriteCmd) -> io::Result<()> {
        let line = serde_json::to_string(cmd).unwrap();
        self.writer.write_all(line.as_bytes())
    }

    pub fn set(&mut self, key: &str, val: &Json) -> io::Result<()> {
        let line =
            "{\"set\":[\"".to_string() + key + "\", " + &serde_json::to_string(val).unwrap() + "]}";
        self.writer.write_all(line.as_bytes())
    }
}
