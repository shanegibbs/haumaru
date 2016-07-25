#![feature(question_mark, box_syntax, try_from)]
#[macro_use]
extern crate log;
extern crate notify;
extern crate time;
extern crate rusqlite;
extern crate crypto;
extern crate rustc_serialize;
extern crate walkdir;

pub mod filesystem;
pub mod engine;
pub mod index;
pub mod storage;

use std::error::Error;
use std::path::PathBuf;
use std::collections::HashSet;
use std::fs::create_dir_all;
use rusqlite::Error as SqliteError;
use time::Timespec;
use rusqlite::Connection;
use std::fmt;

use engine::DefaultEngine;
use filesystem::Change;
use index::SqlLightIndex;
use storage::LocalStorage;

pub trait Engine {
    fn run(&mut self) -> Result<u64, Box<Error>>;
    fn process_change(&mut self, change: Change) -> Result<(), Box<Error>>;
}

pub trait Index {
    fn latest<S: Into<String>>(&mut self, path: S) -> Result<Option<Node>, Box<Error>>;
    fn list<S: Into<String>>(&mut self, path: S) -> Result<Vec<Node>, Box<Error>>;
    fn insert(&mut self, Node) -> Result<Node, Box<Error>>;

    fn dump(&self) -> Vec<Record>;
}

pub trait Storage {
    fn send(&self, &String, Node) -> Result<Node, Box<Error>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    kind: NodeKind,
    path: String,
    size: u64,
    mode: u32,
    deleted: bool,
}

impl Record {
    pub fn new(kind: NodeKind, path: String, size: u64, mode: u32) -> Self {
        Record {
            kind: kind,
            path: path,
            size: size,
            mode: mode,
            deleted: false,
        }
    }
    pub fn deleted(mut self) -> Self {
        self.deleted = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeKind {
    File,
    Dir,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The backup key. Not the absolute path.
    path: String,
    kind: NodeKind,
    mtime: Timespec,
    size: u64,
    mode: u32,
    deleted: bool,
    hash: Option<Vec<u8>>,
}

impl Node {
    fn new<S>(path: S, kind: NodeKind, mtime: Timespec, size: u64, mode: u32) -> Self
        where S: Into<String>
    {
        Node {
            path: path.into(),
            kind: kind,
            mtime: mtime,
            size: size,
            mode: mode,
            deleted: false,
            hash: None,
        }
    }
    fn new_file<S>(path: S, mtime: Timespec, size: u64, mode: u32) -> Self
        where S: Into<String>
    {
        Self::new(path, NodeKind::File, mtime, size, mode)
    }
    fn new_dir<S>(path: S, mtime: Timespec, mode: u32) -> Self
        where S: Into<String>
    {
        Self::new(path, NodeKind::Dir, mtime, 0, mode)
    }
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }
    pub fn deleted(mut self) -> Self {
        self.deleted = true;
        self.size = 0;
        self.mode = 0;
        self.mtime = time::now().to_timespec();
        self
    }
    pub fn is_dir(&self) -> bool {
        self.kind == NodeKind::Dir
    }
    pub fn is_file(&self) -> bool {
        self.kind == NodeKind::File
    }
    pub fn with_hash_str(mut self, hash: &str) -> Self {
        self.hash = Some(hash.as_bytes().to_vec());
        self
    }
    pub fn with_hash(mut self, hash: Vec<u8>) -> Self {
        self.hash = Some(hash);
        self
    }
    pub fn has_hash(&self) -> bool {
        self.hash.is_some()
    }
}

#[derive(Debug)]
pub enum HaumaruError {
    SqlLite(String, SqliteError),
    Index(Box<Error>),
    Storage(Box<Error>),
    Engine(Box<Error>),
}

impl fmt::Display for HaumaruError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            HaumaruError::SqlLite(ref s, ref e) => write!(f, "{}: {}", s, e).unwrap(),
            HaumaruError::Index(ref e) => write!(f, "{}", e).unwrap(),
            HaumaruError::Storage(ref e) => write!(f, "{}", e).unwrap(),
            HaumaruError::Engine(ref e) => write!(f, "{}", e).unwrap(),
        }
        Ok(())
    }
}

pub fn run<T>(path: T) -> Result<(), HaumaruError>
    where T: Into<String>
{
    let path = path.into();

    let mut db_path = PathBuf::new();
    db_path.push("target");
    create_dir_all(&db_path).unwrap();
    db_path.push("haumaru.idx");

    let conn = Connection::open(&db_path)
        .map_err(|e| HaumaruError::SqlLite(format!("Failed to open database {:?}", db_path), e))?;
    let db_path_abs = db_path.canonicalize().unwrap().to_str().unwrap().to_string();

    let mut store_path = PathBuf::new();
    store_path.push("target");
    store_path.push("store");
    create_dir_all(&store_path).unwrap();
    let store_path_abs = store_path.canonicalize().unwrap().to_str().unwrap().to_string();

    {
        let mut index = SqlLightIndex::new(&conn)
            .map_err(|e| HaumaruError::Index(box e))?;

        let store = LocalStorage::new(store_path_abs.clone())
            .map_err(|e| HaumaruError::Storage(box e))?;

        let mut excludes = HashSet::new();
        excludes.insert(db_path_abs);
        excludes.insert(store_path_abs);

        let mut engine = DefaultEngine::new(path, excludes, &mut index, store).unwrap();
        engine.run().map_err(|e| HaumaruError::Engine(e))?;
    }

    Ok(())
}

pub fn dump() -> Result<(), HaumaruError> {

    let mut db_path = PathBuf::new();
    db_path.push("target");
    db_path.push("haumaru.idx");

    let conn = Connection::open_with_flags(&db_path, rusqlite::SQLITE_OPEN_READ_ONLY).unwrap();
    let index = SqlLightIndex::new(&conn)
        .map_err(|e| HaumaruError::Index(box e))?;

    index.dump_records();

    Ok(())
}

fn get_key(base_path: &str, abs_path: &str) -> String {
    assert!(abs_path.len() >= base_path.len(),
            format!("abs_path.len() >= base_path.len(), base_path={}, abs_path={}",
                    base_path,
                    abs_path));
    abs_path[base_path.len()..].trim_matches('/').to_string()
}
