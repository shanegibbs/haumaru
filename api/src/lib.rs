#[macro_use]
extern crate log;
extern crate notify;
extern crate time;
extern crate rusqlite;

mod filesystem;
mod engine;
mod index;
mod storage;

use time::Timespec;
use std::time::SystemTime;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::fmt::Debug;

use engine::DefaultEngine;
use engine::DefaultEngineError;

pub trait Engine {
    fn run(&self) -> Result<u64, Box<Error>>;
}

pub trait Index {
    fn latest(&self, &Path) -> Result<Option<Node>, Box<Error>>;
    fn insert(&self, Node) -> Result<Node, Box<Error>>;

    fn path_exists(&self, &Path) -> Result<bool, Box<Error>>;
    fn contains(&self, &Node) -> Result<bool, Box<Error>>;
    fn save(&self, &Node) -> Result<(), Box<Error>>;
}

pub trait Storage {
    fn send(&self, Node) -> Result<Node, Box<Error>>;
}

#[derive(Debug)]
pub struct Node {
    /// Full path. No trailing slash.
    path: PathBuf,
    sym_link: bool,
    mtime: Timespec,
    mode: u32,
    leaf: Leaf,
}

impl Node {
    fn new(path: &PathBuf, mtime: Timespec, mode: u32, leaf: Leaf) -> Self {
        Node {
            path: path.clone(),
            sym_link: false,
            mtime: mtime,
            mode: mode,
            leaf: leaf,
        }
    }
    fn new_file(path: &PathBuf, mtime: Timespec, mode: u32, size: u64) -> Self {
        Self::new(path, mtime, mode, Leaf::File(FileLeaf::new()))
    }
    fn new_dir(path: &PathBuf, mtime: Timespec, mode: u32) -> Self {
        Self::new(path, mtime, mode, Leaf::Dir(DirLeaf {}))
    }
}

#[derive(Debug)]
pub enum Leaf {
    File(FileLeaf),
    Dir(DirLeaf),
}

#[derive(Debug)]
pub struct FileLeaf {
    size: u64,
    hash: Option<String>,
}

impl FileLeaf {
    fn new() -> Self {
        FileLeaf {
            size: 0,
            hash: None,
        }
    }
}

#[derive(Debug)]
pub struct DirLeaf;

use index::SqlLightIndex;
use storage::LocalStorage;

pub fn new<T>(path: T) -> DefaultEngine<SqlLightIndex, LocalStorage>
    where T: Into<String>
{
    let index = SqlLightIndex::new();
    let store = LocalStorage::new();

    DefaultEngine::new(path, index, store)
}
