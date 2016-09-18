use std::result::Result as StdResult;
use std::fmt::{Formatter, Display};
use std::fmt::Error as FmtError;
use std::error::Error as StdError;

use super::Result;
use filesystem::BackupPathError;
use Node;

#[derive(Debug)]
pub enum DefaultEngineError {
    CreateBackupPath(BackupPathError),
    StartWatcher(BackupPathError),
    GetFile(BackupPathError),
    Scan(BackupPathError),
    Index(Box<StdError>),
    Storage(String, Box<StdError>),
    Other(String),
    GeneralWithNode(String, Node),
}

impl StdError for DefaultEngineError {
    fn description(&self) -> &str {
        "something"
    }
    fn cause(&self) -> Option<&StdError> {
        None
    }
}

impl Display for DefaultEngineError {
    fn fmt(&self, f: &mut Formatter) -> StdResult<(), FmtError> {
        match *self {
            DefaultEngineError::CreateBackupPath(ref e) => {
                write!(f, "Unable to create backup path: {}", e).unwrap()
            }
            DefaultEngineError::StartWatcher(ref e) => {
                write!(f, "Unable to start watcher: {}", e).unwrap()
            }
            DefaultEngineError::GetFile(ref e) => write!(f, "Unable to read file: {}", e).unwrap(),
            DefaultEngineError::Scan(ref e) => write!(f, "Scan failed: {}", e).unwrap(),
            DefaultEngineError::Index(ref e) => write!(f, "Index error: {}", e).unwrap(),
            DefaultEngineError::Storage(ref s, ref e) => {
                write!(f, "Storage error: {}: {}", s, e).unwrap()
            }
            DefaultEngineError::Other(ref s) => write!(f, "Engine error: {}", s).unwrap(),
            DefaultEngineError::GeneralWithNode(ref s, ref _n) => write!(f, "{}", s).unwrap(),
        }
        Ok(())
    }
}