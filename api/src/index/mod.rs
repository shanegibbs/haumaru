use std::error::Error;
use std::fmt;
use time::Timespec;
use {Node, Record};

mod sql_light_index;
pub use index::sql_light_index::*;

pub trait Index {
    fn get(&mut self, path: String, from: Option<Timespec>) -> Result<Option<Node>, IndexError>;
    fn list(&mut self, path: String, from: Option<Timespec>) -> Result<Vec<Node>, IndexError>;
    fn visit_all_hashable(&mut self,
                          f: &mut FnMut(Node) -> Result<(), IndexError>)
                          -> Result<(), IndexError>;
    fn insert(&mut self, &Node) -> Result<(), IndexError>;
    fn create_backup_set(&mut self, timestamp: i64) -> Result<u64, IndexError>;
    // fn backup_set_records(&mut self, backup_set: u64);

    fn dump(&self) -> Vec<Record>;
}

#[derive(Debug)]
pub enum IndexError {
    Fatal(String, Option<Box<IndexError>>),
}

impl Error for IndexError {
    fn description(&self) -> &str {
        match *self {
            IndexError::Fatal(ref _s, ref _e) => "Unrecoverable fatal error",
        }
    }
    fn cause(&self) -> Option<&Error> {
        match *self {
            IndexError::Fatal(ref _s, None) => None,
            IndexError::Fatal(ref _s, Some(ref e)) => Some(e.as_ref()),
        }
    }
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            IndexError::Fatal(ref s, None) => write!(f, "Fatal error: {}", s)?,
            IndexError::Fatal(ref s, Some(ref e)) => write!(f, "{}, caused by: {}", s, e)?,
        }
        Ok(())
    }
}
