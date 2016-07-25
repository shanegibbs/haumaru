use filesystem::{BackupPath, BackupPathError};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::thread;
use std::thread::sleep;
use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use std::error::Error as StdError;
use std::fmt::{Formatter, Display};
use std::fmt::Error as FmtError;
use std::time::{Duration, SystemTime};

use filesystem::Change;
use {Engine, Index, Storage, get_key};

pub type Result<T> = StdResult<T, DefaultEngineError>;

#[derive(Debug)]
pub enum DefaultEngineError {
    CreateBackupPath(BackupPathError),
    StartWatcher(BackupPathError),
    GetFile(BackupPathError),
    Scan(BackupPathError),
    Index(Box<StdError>),
    Storage(String, Box<StdError>),
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
        }
        Ok(())
    }
}

pub struct DefaultEngine<'i, I, S>
    where I: Index,
          S: Storage,
          I: 'i
{
    path: String,
    excludes: HashSet<String>,
    index: &'i mut I,
    storage: S,
    backup_path: BackupPath,
}

impl<'i, I, S> DefaultEngine<'i, I, S>
    where I: Index,
          S: Storage
{
    pub fn new<T>(path: T,
                  excludes: HashSet<String>,
                  index: &'i mut I,
                  storage: S)
                  -> StdResult<Self, Box<StdError>>
        where T: Into<String>
    {
        let path = path.into();
        let path_buf = PathBuf::from(&path).canonicalize().unwrap();
        let abs_path = path_buf.to_str().unwrap().to_string();
        // let path = path.as_ref().to_path_buf();
        // let path = path.canonicalize().unwrap();

        debug!("Base path: {}", path);
        debug!("Exclude paths: {:?}", excludes);

        let bp = try!(BackupPath::new(abs_path.clone())
            .map_err(|e| DefaultEngineError::CreateBackupPath(e)));

        Ok(DefaultEngine {
            path: abs_path,
            excludes: excludes,
            index: index,
            storage: storage,
            backup_path: bp,
        })
    }

    pub fn scan(&mut self) -> StdResult<(), Box<StdError>> {
        info!("Beginning full scan");

        use std::collections::VecDeque;
        use std::fs::read_dir;
        use std::fs::DirEntry;

        let mut queue = VecDeque::new();
        queue.push_back(self.path.clone());

        while let Some(p) = queue.pop_front() {
            debug!("Scanning {:?}", p);

            let mut ls: Vec<DirEntry> = vec![];
            for entry in read_dir(&p)? {
                ls.push(entry?);
            }
            let known_nodes = self.index.list(get_key(&self.path, &p))?;

            // process each item that exists
            for entry in &ls {

                let entry_path = entry.path();

                self.process_change(Change::new(entry_path.clone()))?;

                if entry_path.is_dir() {
                    debug!("Scan dir  {:?}", entry_path);
                    queue.push_back(entry_path.to_str().unwrap().to_string());
                }

            }

            // check each item we know about still exists
            // i.e. check for deleted ndoes
            debug!("known_nodes.len={}", known_nodes.len());
            for known_node in known_nodes {
                debug!("Checking {}", known_node.path);
                let mut found = false;
                for entry in &ls {
                    let entry_key = get_key(&self.path, entry.path().to_str().unwrap());
                    debug!("Compare {} and {:?}", known_node.path, entry_key);
                    if known_node.path == entry_key {
                        found = true;
                    }
                }
                debug!("found={}", found);
                if !found {
                    let mut change_path = PathBuf::new();
                    change_path.push(&self.path);
                    change_path.push(&known_node.path);
                    self.process_change(Change::new(change_path))?;
                }
            }

        }

        info!("Full scan complete");
        Ok(())
    }
}

fn is_excluded(excludes: &HashSet<String>, change: &Change, base_path: &String) -> bool {
    let change_path_str = change.path().to_str().unwrap();
    for exclude in excludes {
        if change_path_str.starts_with(exclude) {
            return true;
        }
    }
    if change_path_str == base_path {
        return true;
    }
    false
}

impl<'i, I, S> Engine for DefaultEngine<'i, I, S>
    where I: Index,
          S: Storage
{
    fn run(&mut self) -> StdResult<u64, Box<StdError>> {

        info!("Starting backup engine on {}", self.path);

        let changes = Arc::new(Mutex::new(HashSet::new()));

        {
            let watcher =
                try!(self.backup_path.watcher().map_err(|e| DefaultEngineError::StartWatcher(e)));
            let changes = changes.clone();
            let local_excludes = self.excludes.clone();
            let local_path = self.path.clone();
            thread::spawn(move || {
                match watcher.watch(move |change| {
                    if is_excluded(&local_excludes, &change, &local_path) {
                        trace!("Skipping excluded path: {:?}", change.path());
                        return;
                    }

                    let mut changes = changes.lock().unwrap();
                    changes.insert(change);
                }) {
                    Ok(_) => {
                        warn!("Watch ended");
                    }
                    Err(e) => {
                        error!("Watch ended: {}", e);
                    }
                };
            });
        }

        self.scan()?;

        loop {
            let start = SystemTime::now();

            let mut work_queue = vec![];
            {
                let mut changes = changes.lock().unwrap();
                for c in changes.drain() {
                    // drain changes into the work queue
                    work_queue.push(c);
                }
            }

            if !work_queue.is_empty() {
                info!("Beginning backup run");
            }

            for change in work_queue {
                self.process_change(change).unwrap();
            }

            loop {
                sleep(Duration::new(1, 0));
                match start.elapsed() {
                    Ok(elapsed) => {
                        if elapsed.as_secs() >= 900 {
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Timer failed: {}", e);
                    }
                }
            }

            // debug!("Sleeping");
            // sleep(Duration::from_secs(3));
        }

    }

    fn process_change(&mut self, change: Change) -> StdResult<(), Box<StdError>> {
        if is_excluded(&self.excludes, &change, &self.path) {
            trace!("Skipping excluded path: {:?}", change.path());
            return Ok(());
        }

        debug!("Received {:?}", change);

        let change_path_str = change.path().to_str().unwrap();
        let key = get_key(&self.path, change_path_str);
        debug!("Change key = {}", key);

        let node = try!(self.index
            .latest(key.clone())
            .map_err(|e| DefaultEngineError::Index(e)));
        let file = try!(self.backup_path
            .get_file(change.path())
            .map_err(|e| DefaultEngineError::GetFile(e)));

        match file {
            None => {
                match node {
                    None => {
                        debug!("Skipping transient {:?}", change);
                    }
                    Some(existing_node) => {
                        info!("- {}", key);
                        debug!("Detected DELETE on {:?}, {:?}", change, existing_node);
                        self.index
                            .insert(existing_node.deleted())
                            .map_err(|e| DefaultEngineError::Index(e))?;
                    }
                }
            }
            Some(new_node) => {

                if new_node.size > 1024 * 1024 * 10 {
                    warn!("Skipping large file {}", key);
                    return Ok(());
                }

                match node {
                    None => {
                        info!("+ {}", key);
                        debug!("Detected NEW on {:?}, {:?}", change, new_node);
                        let sent_file = match new_node.is_dir() {
                            true => new_node,
                            false => {
                                try!(self.storage
                                    .send(&self.path, new_node)
                                    .map_err(|e| {
                                        DefaultEngineError::Storage(format!("Failed to send \
                                                                             file: {}",
                                                                            key),
                                                                    e)
                                    }))
                            }
                        };
                        let _inserted_file = try!(self.index
                            .insert(sent_file)
                            .map_err(|e| DefaultEngineError::Index(e)));
                    }
                    Some(existing_node) => {

                        // no need to update directory
                        if existing_node.is_dir() && new_node.is_dir() {
                            debug!("  {} (skipping dir)", key);
                            return Ok(());
                        }

                        // size and mtime match, skip.
                        if new_node.size == existing_node.size &&
                           new_node.mtime == existing_node.mtime {
                            debug!("  {} (assume match)", key);
                            return Ok(());
                        }

                        info!(". {}", key);
                        debug!("Detected UPDATE on {:?},\n{:?},\n{:?}",
                               change,
                               existing_node,
                               new_node);
                        let sent_file = match new_node.is_dir() {
                            true => new_node,
                            false => {
                                try!(self.storage
                                    .send(&self.path, new_node)
                                    .map_err(|e| {
                                        DefaultEngineError::Storage(format!("Failed to send \
                                                                             file: {}",
                                                                            key),
                                                                    e)
                                    }))
                            }
                        };
                        let _inserted_file = try!(self.index
                            .insert(sent_file)
                            .map_err(|e| DefaultEngineError::Index(e)));
                    }
                }
            }
        }

        Ok(())
    }
}
