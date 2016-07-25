use std::path::{Path, PathBuf};
use std::error::Error;
use std::fmt;
use rusqlite::Connection;

use {Node, Index};

#[derive(Debug)]
pub enum SqlLightIndexError {
}

impl Error for SqlLightIndexError {
    fn description(&self) -> &str {
        "SqlLightIndexError"
    }
}

impl fmt::Display for SqlLightIndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "MyError");
        Ok(())
    }
}

#[derive(Debug)]
pub struct SqlLightIndex {
    conn: Connection,
}

impl SqlLightIndex {
    pub fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute("CREATE TABLE node(
                      id INTEGER PRIMARY KEY
                      name TEXT NOT NULL,
                      mtime TEXT NOT NULL,
                      type TEXT NOT NULL
                      )",
                     &[])
            .unwrap();

        SqlLightIndex { conn: conn }
    }
}

impl Index for SqlLightIndex {
    fn latest(&self, path: &Path) -> Result<Option<Node>, Box<Error>> {
        Ok(None)
    }

    fn insert(&self, node: Node) -> Result<Node, Box<Error>> {
        Ok(node)
    }

    fn path_exists(&self, _path: &Path) -> Result<bool, Box<Error>> {
        Ok(false)
    }

    fn contains(&self, n: &Node) -> Result<bool, Box<Error>> {
        Ok(false)
    }

    fn save(&self, n: &Node) -> Result<(), Box<Error>> {
        Ok(())
    }
}
