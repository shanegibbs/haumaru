use filesystem::{BackupPath, BackupPathError};
use std::path::Path;
use std::result::Result as StdResult;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::ops::Deref;
use std::collections::HashSet;
use std::error::Error as StdError;
use std::fmt::{Formatter, Display};
use std::fmt::Error as FmtError;
use std::fmt::Debug;

use filesystem::Change;
use {Engine, Index, Storage};

pub type Result<T> = StdResult<T, DefaultEngineError>;

#[derive(Debug)]
pub enum DefaultEngineError {
    CreateBackupPath(BackupPathError),
    StartWatcher(BackupPathError),
    GetFile(BackupPathError),
    Index(Box<StdError>),
    Storage(Box<StdError>),
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
        write!(f, "some engine error");
        Ok(())
    }
}

pub struct DefaultEngine<I, S>
    where I: Index,
          S: Storage
{
    path: String,
    index: I,
    storage: S,
}

impl<I, S> DefaultEngine<I, S>
    where I: Index,
          S: Storage
{
    pub fn new<T>(path: T, index: I, storage: S) -> Self
        where T: Into<String>
    {
        DefaultEngine {
            path: path.into(),
            index: index,
            storage: storage,
        }
    }

    fn process_change(&self, bp: &BackupPath, change: Change) -> Result<()> {

        let node = try!(self.index.latest(change.path()).map_err(|e| DefaultEngineError::Index(e)));
        let file = try!(bp.get_file(change.path()).map_err(|e| DefaultEngineError::GetFile(e)));

        match file {
            None => {
                match node {
                    None => {
                        debug!("Skipping transient {:?}", change);
                    }
                    Some(existing_node) => {
                        debug!("Detected DELETE on {:?}, {:?}", change, existing_node);
                    }
                }
            }
            Some(existing_file) => {
                match node {
                    None => {
                        debug!("Detected NEW on {:?}, {:?}", change, existing_file);
                        let sent_file = try!(self.storage
                            .send(existing_file)
                            .map_err(|e| DefaultEngineError::Storage(e)));
                        let inserted_file = try!(self.index
                            .insert(sent_file)
                            .map_err(|e| DefaultEngineError::Index(e)));
                    }
                    Some(existing_node) => {
                        debug!("Detected UPDATE on {:?}, {:?}, {:?}",
                               change,
                               existing_file,
                               existing_node);
                    }
                }
            }
        }

        Ok(())
    }
}

impl<I, S> Engine for DefaultEngine<I, S>
    where I: Index,
          S: Storage
{
    fn run(&self) -> StdResult<u64, Box<StdError>> {
        info!("Starting backup engine on {}", self.path);

        let mut bp = try!(BackupPath::new("/Users/sgibbs/Documents")
            .map_err(|e| DefaultEngineError::CreateBackupPath(e)));

        let mut changes = Arc::new(Mutex::new(HashSet::new()));

        {
            let watcher = try!(bp.watcher().map_err(|e| DefaultEngineError::StartWatcher(e)));
            let changes = changes.clone();
            thread::spawn(move || {
                watcher.watch(move |change| {
                    let mut changes = changes.lock().unwrap();
                    changes.insert(change);
                });
            });
        }

        bp.scan();

        loop {
            let mut work_queue = vec![];
            {
                let mut changes = changes.lock().unwrap();
                for c in changes.drain() {
                    // drain changes into the work queue
                    work_queue.push(c);
                }
            }

            for change in work_queue {
                self.process_change(&bp, change).unwrap();
            }

            debug!("Sleeping");
            sleep(Duration::from_secs(3));
        }

        Ok(0)
    }
}
