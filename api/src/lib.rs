#![deny(warnings)]
#![feature(box_syntax, try_from, custom_derive, plugin, proc_macro)]
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
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
extern crate hyper;
extern crate threadpool;

#[cfg(test)]
extern crate env_logger;

#[macro_use]
mod expect;

pub mod filesystem;
pub mod engine;
pub mod index;
pub mod storage;
pub mod config;

mod node;
mod hasher;
mod retry;
mod queue;

pub use config::{AsConfig, Config};

use engine::DefaultEngine;
pub use engine::EngineConfig;
use filesystem::Change;

pub use index::Index;
use index::SqlLightIndex;
pub use node::{Node, NodeKind};
use rusqlite::Connection;
use rusqlite::Error as SqliteError;
use std::borrow::Borrow;
use std::collections::HashSet;

use std::convert::TryInto;
use std::error::Error;
use std::fmt;
use std::fs::create_dir_all;
use std::io::{Read, Write};
use std::path::PathBuf;
// use storage::LocalStorage;
use storage::SendRequest;
use time::Timespec;

pub trait Engine {
    fn run(&mut self) -> Result<(), Box<Error>>;
    fn process_changes(&mut self, for_time: i64, changes: Vec<Change>) -> Result<(), Box<Error>>;
    fn verify_store(&mut self, like: String) -> Result<(), Box<Error>>;
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

pub trait Storage: Send + Clone {
    fn send(&self, req: &mut SendRequest) -> Result<(), Box<Error>>;
    fn retrieve(&self, hash: &[u8]) -> Result<Option<Box<Read>>, Box<Error>>;
    fn verify(&self, Node) -> Result<(Node, bool), Box<Error>>;
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
    ParseConfig(Box<Error>),
    Index(Box<Error>),
    Storage(Box<Error>),
    Engine(Box<Error>),
    Other(String),
}

impl Error for HaumaruError {
    fn description(&self) -> &str {
        match *self {
            HaumaruError::Config(ref _e) => "Config error",
            HaumaruError::ParseConfig(ref _e) => "Config parse error",
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
            HaumaruError::ParseConfig(ref e) => Some(e.borrow()),
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
            HaumaruError::Config(ref e) => write!(f, "Config error: {}", e)?,
            HaumaruError::ParseConfig(ref e) => write!(f, "Parse error: {}", e)?,
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

fn build_storage(config: EngineConfig) -> storage::LocalStorage {
    storage::LocalStorage::new(&config).expect("build storage")
    // storage::S3Storage::new(config)
}

fn build_index(config: EngineConfig) -> Result<SqlLightIndex, HaumaruError> {
    let mut working_path = PathBuf::new();
    working_path.push(config.working());
    create_dir_all(&working_path).unwrap();

    let mut db_path = working_path.clone();
    db_path.push("haumaru.idx");

    let conn = Connection::open(&db_path)
        .map_err(|e| HaumaruError::SqlLite(format!("Failed to open database {:?}", db_path), e))?;
    Ok(SqlLightIndex::new(conn).map_err(|e| HaumaruError::Index(box e))?)
}

fn setup_and_run<F>(config: EngineConfig, mut f: F) -> Result<(), HaumaruError>
    where F: FnMut(&mut Engine) -> Result<(), HaumaruError>
{
    let mut excludes = HashSet::new();
    excludes.insert(config.abs_working().to_str().unwrap().to_string());

    let mut engine =
        DefaultEngine::new(config.clone(),
                           excludes,
                           build_index(config.clone())?,
                           build_storage(config)).map_err(|e| HaumaruError::Engine(e))?;

    f(&mut engine)
}

pub fn run(user_config: Config) -> Result<(), HaumaruError> {
    let config: EngineConfig = user_config.try_into()?;
    setup_and_run(config, |eng| eng.run().map_err(|e| HaumaruError::Engine(e)))
}

pub fn verify(user_config: Config, like: String) -> Result<(), HaumaruError> {
    let config: EngineConfig = user_config.try_into()?;
    setup_and_run(config,
                  |eng| eng.verify_store(like.clone()).map_err(|e| HaumaruError::Engine(e)))
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
                  |eng| eng.list(&key, from, &mut cur).map_err(|e| HaumaruError::Engine(e)))?;
    let content = String::from_utf8(cur.into_inner()).expect("from_utf8");
    println!("{}", content);
    Ok(())
}

pub fn dump() -> Result<(), HaumaruError> {

    let mut db_path = PathBuf::new();
    db_path.push("target");
    db_path.push("haumaru.idx");

    let conn = Connection::open_with_flags(&db_path, rusqlite::SQLITE_OPEN_READ_ONLY).unwrap();
    let index = SqlLightIndex::new(conn).map_err(|e| HaumaruError::Index(box e))?;

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
