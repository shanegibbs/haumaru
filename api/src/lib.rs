// #![deny(warnings)]
#![feature(question_mark, box_syntax, try_from, custom_derive, plugin)]
#![plugin(serde_macros)]
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
extern crate notify;
extern crate time;
extern crate chrono;
extern crate rusqlite;
extern crate crypto;
extern crate rustc_serialize;
extern crate regex;
extern crate serde;
extern crate serde_yaml;
extern crate hyper;
extern crate threadpool;

#[cfg(test)]
extern crate env_logger;

pub mod filesystem;
pub mod engine;
pub mod index;
pub mod storage;
pub mod config;

mod node;
mod hasher;
mod retry;
mod queue;

pub use config::{Config, AsConfig};
pub use engine::EngineConfig;
pub use node::{Node, NodeKind};

use std::convert::TryInto;
use std::error::Error;
use std::path::PathBuf;
use std::collections::HashSet;
use std::fs::create_dir_all;
use rusqlite::Error as SqliteError;
use rusqlite::Connection;
use std::fmt;
use std::borrow::Borrow;
use std::io::{Read, Write};
use time::Timespec;

use engine::DefaultEngine;
use filesystem::Change;
use index::IndexError;
use index::SqlLightIndex;
use index::SingleThreadIndex;
// use storage::LocalStorage;
use storage::SendRequest;

pub trait Engine {
    fn run(&mut self) -> Result<u64, Box<Error>>;
    fn process_change(&mut self, backup_set: u64, change: Change) -> Result<(), Box<Error>>;
    fn verify_store(&mut self) -> Result<(), Box<Error>>;
    fn restore(&mut self,
               key: &str,
               from: Option<Timespec>,
               target: &str)
               -> Result<(), Box<Error>>;
    fn list(&mut self,
            key: &str,
            from: Option<Timespec>,
            out: &mut Write)
            -> Result<(), Box<Error>>;
}

pub trait Index: Send + Clone {
    fn get(&mut self, path: String, from: Option<Timespec>) -> Result<Option<Node>, IndexError>;
    fn list(&mut self, path: String, from: Option<Timespec>) -> Result<Vec<Node>, IndexError>;
    fn visit_all_hashable(&mut self,
                          f: &mut FnMut(Node) -> Result<(), IndexError>)
                          -> Result<(), IndexError>;
    fn insert(&mut self, Node) -> Result<Node, IndexError>;
    fn create_backup_set(&mut self, timestamp: i64) -> Result<u64, IndexError>;

    fn dump(&self) -> Vec<Record>;
}

pub trait Storage: Send + Clone {
    fn send(&self, req: SendRequest) -> Result<Node, Box<Error>>;
    fn retrieve(&self, hash: &[u8]) -> Result<Option<Box<Read>>, Box<Error>>;
    fn verify(&self, Node) -> Result<Option<Node>, Box<Error>>;
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

#[derive(Debug)]
pub enum HaumaruError {
    SqlLite(String, SqliteError),
    Config(Box<Error>),
    Index(Box<Error>),
    Storage(Box<Error>),
    Engine(Box<Error>),
    Other(String),
}

impl Error for HaumaruError {
    fn description(&self) -> &str {
        match *self {
            HaumaruError::Config(ref _e) => "Failed to load config",
            HaumaruError::SqlLite(ref _s, ref _e) => "SqlLite error",
            HaumaruError::Index(ref _e) => "Index error",
            HaumaruError::Storage(ref _e) => "Storage error",
            HaumaruError::Engine(ref _e) => "Engine error",
            HaumaruError::Other(ref s) => s,
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            HaumaruError::Config(ref e) => Some(e.borrow()),
            HaumaruError::SqlLite(ref _s, ref e) => Some(e),
            HaumaruError::Index(ref e) => Some(e.borrow()),
            HaumaruError::Storage(ref e) => Some(e.borrow()),
            HaumaruError::Engine(ref e) => Some(e.borrow()),
            HaumaruError::Other(ref _s) => None,
        }
    }
}

impl fmt::Display for HaumaruError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            HaumaruError::Config(ref e) => write!(f, "Failed to load config file: {}", e)?,
            HaumaruError::SqlLite(ref s, ref e) => write!(f, "{}: {}", s, e)?,
            HaumaruError::Index(ref e) => write!(f, "{}", e)?,
            HaumaruError::Storage(ref e) => write!(f, "{}", e)?,
            HaumaruError::Engine(ref e) => write!(f, "{}", e)?,
            HaumaruError::Other(ref e) => write!(f, "{}", e)?,
        }
        Ok(())
    }
}

fn split_key(key: &str) -> (String, Option<Timespec>) {
    if !key.contains("@") {
        return (key.to_string(), None);
    }

    use regex::Regex;

    let split_re = Regex::new(r"^(.*)@(.*)$").unwrap();
    let cap = split_re.captures(key).unwrap();

    let key_str = cap.at(1).expect("group1");
    let unix_ts_str = cap.at(2).expect("group2");

    debug!("key_str={}", key_str);
    debug!("key_unix_ts={}", unix_ts_str);

    let unix_ts = unix_ts_str.parse::<i64>().expect("unix timestamp");

    (key_str.to_string(),
     Some(Timespec {
        sec: unix_ts,
        nsec: 0,
    }))
}

#[test]
fn test_split_key() {
    let _ = env_logger::init();

    let (key, ts) = split_key("abc");
    assert_eq!("abc", key);
    assert_eq!(ts, None);

    let (key, ts) = split_key("abc@123");
    assert_eq!("abc", key);
    assert_eq!(ts,
               Some(Timespec {
                   sec: 123,
                   nsec: 0,
               }));

    let (key, ts) = split_key("@123");
    assert_eq!("", key);
    assert_eq!(ts,
               Some(Timespec {
                   sec: 123,
                   nsec: 0,
               }));

}

fn build_storage() -> storage::S3Storage {
    // LocalStorage::new(&config)
    //     .map_err(|e| HaumaruError::Storage(box e))?;
    storage::S3Storage::new()
}

pub fn run(user_config: Config) -> Result<(), HaumaruError> {
    let config: EngineConfig = user_config.try_into()?;

    let mut pathb = PathBuf::new();
    pathb.push(config.path());
    let path = pathb.as_path();
    if !path.exists() {
        return Err(HaumaruError::Other(format!("Backup path does not exist: {}", config.path())));
    }

    let mut working_path = PathBuf::new();
    working_path.push(config.working());
    create_dir_all(&working_path).unwrap();
    let working_abs = working_path.canonicalize().unwrap().to_str().unwrap().to_string();

    let mut db_path = working_path.clone();
    db_path.push("haumaru.idx");

    let conn = Connection::open(&db_path)
        .map_err(|e| HaumaruError::SqlLite(format!("Failed to open database {:?}", db_path), e))?;

    let mut store_path = working_path.clone();
    store_path.push("store");
    create_dir_all(&store_path).unwrap();

    {
        let index = SqlLightIndex::new(conn)
            .map_err(|e| HaumaruError::Index(box e))?;
        // let index = SingleThreadIndex::new(index);

        let mut excludes = HashSet::new();
        excludes.insert(working_abs);

        let mut engine = DefaultEngine::new(config, excludes, index, build_storage())
            .map_err(|e| HaumaruError::Engine(e))?;
        engine.run().map_err(|e| HaumaruError::Engine(e))?;
    }

    Ok(())
}

fn setup_and_run<F>(config: EngineConfig, mut f: F) -> Result<(), HaumaruError>
    where F: FnMut(&mut Engine) -> Result<(), HaumaruError>
{
    let conn = SqlLightIndex::open_database(&config).map_err(|e| HaumaruError::Index(box e))?;
    let index = SqlLightIndex::new(conn)
        .map_err(|e| HaumaruError::Index(box e))?;
    // let index = SingleThreadIndex::new(index);

    let mut excludes = HashSet::new();
    excludes.insert(config.abs_working().to_str().unwrap().to_string());

    let mut engine = DefaultEngine::new(config, excludes, index, build_storage())
        .map_err(|e| HaumaruError::Engine(e))?;

    f(&mut engine)
}

pub fn verify(user_config: Config) -> Result<(), HaumaruError> {
    let config: EngineConfig = user_config.try_into()?;
    let config = config.detached();

    let conn = SqlLightIndex::open_database(&config).map_err(|e| HaumaruError::Index(box e))?;
    // let index = SingleThreadIndex::new({
    //     SqlLightIndex::new(&conn).map_err(|e| HaumaruError::Index(box e))?
    // });
    let index = SqlLightIndex::new(conn).map_err(|e| HaumaruError::Index(box e))?;

    let mut excludes = HashSet::new();
    excludes.insert(config.abs_working().to_str().unwrap().to_string());

    let mut engine = DefaultEngine::new(config, excludes, index, build_storage())
        .map_err(|e| HaumaruError::Engine(e))?;
    engine.verify_store().map_err(|e| HaumaruError::Engine(e))?;

    Ok(())
}

pub fn restore(user_config: Config, key: &str, target: &str) -> Result<(), HaumaruError> {
    let config: EngineConfig = user_config.try_into()?;
    let config = config.detached();
    let (key, from) = split_key(key);
    setup_and_run(config,
                  |eng| eng.restore(&key, from, target).map_err(|e| HaumaruError::Engine(e)))
}

pub fn list(user_config: Config, key: &str) -> Result<(), HaumaruError> {
    use std::io::Cursor;

    let config: EngineConfig = user_config.try_into()?;
    let config = config.detached();
    let (key, from) = split_key(key);

    let mut cur = Cursor::new(Vec::new());
    setup_and_run(config,
                  |eng| eng.list(&key, from, &mut cur).map_err(|e| HaumaruError::Engine(e)))
        ?;
    let content = String::from_utf8(cur.into_inner()).expect("from_utf8");
    println!("{}", content);
    Ok(())
}

pub fn dump() -> Result<(), HaumaruError> {

    let mut db_path = PathBuf::new();
    db_path.push("target");
    db_path.push("haumaru.idx");

    let conn = Connection::open_with_flags(&db_path, rusqlite::SQLITE_OPEN_READ_ONLY).unwrap();
    let index = SqlLightIndex::new(conn)
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
