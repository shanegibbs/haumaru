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
use std::time::Duration;
use std::num::ParseIntError;
use time;
use time::Timespec;

use filesystem::Change;
use {Engine, Index, Storage, get_key};

#[derive(Debug)]
pub struct EngineConfig {
    path: String,
    working: String,
    period: u32,
}

impl EngineConfig {
    pub fn new(path: &str, working: &str, period: &str) -> StdResult<Self, ParseIntError> {
        let period = period.parse::<u32>()?;
        Ok(EngineConfig {
            path: path.to_string(),
            working: working.to_string(),
            period: period,
        })
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn working(&self) -> &str {
        &self.working
    }
    pub fn period(&self) -> u32 {
        self.period
    }
}

pub type Result<T> = StdResult<T, DefaultEngineError>;

#[derive(Debug)]
pub enum DefaultEngineError {
    CreateBackupPath(BackupPathError),
    StartWatcher(BackupPathError),
    GetFile(BackupPathError),
    Scan(BackupPathError),
    Index(Box<StdError>),
    Storage(String, Box<StdError>),
    Other(String),
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
        }
        Ok(())
    }
}

pub struct DefaultEngine<'i, I, S>
    where I: Index,
          S: Storage,
          I: 'i
{
    config: EngineConfig,
    excludes: HashSet<String>,
    index: &'i mut I,
    storage: S,
    backup_path: BackupPath,
}

impl<'i, I, S> DefaultEngine<'i, I, S>
    where I: Index,
          S: Storage
{
    pub fn new(config: EngineConfig,
               excludes: HashSet<String>,
               index: &'i mut I,
               storage: S)
               -> StdResult<Self, Box<StdError>> {
        let mut config = config;
        let path_buf = PathBuf::from(config.path())
            .canonicalize()
            .map_err(|e| {
                DefaultEngineError::Other(format!("Unable to canonicalize backup path {}: {}",
                                                  config.path(),
                                                  e))
            })?;
        let abs_path = path_buf.to_str().unwrap().to_string();
        config.path = abs_path.clone();

        debug!("Base path: {}", config.path());
        debug!("Exclude paths: {:?}", excludes);

        let bp = try!(BackupPath::new(abs_path.clone())
            .map_err(|e| DefaultEngineError::CreateBackupPath(e)));

        Ok(DefaultEngine {
            config: config,
            excludes: excludes,
            index: index,
            storage: storage,
            backup_path: bp,
        })
    }

    pub fn scan(&mut self, backup_set: u64) -> StdResult<(), Box<StdError>> {
        info!("Beginning full scan");

        use std::collections::VecDeque;
        use std::fs::read_dir;
        use std::fs::DirEntry;

        let mut queue = VecDeque::new();
        queue.push_back(self.config.path().to_string());

        while let Some(p) = queue.pop_front() {
            debug!("Scanning {:?}", p);

            let mut ls: Vec<DirEntry> = vec![];
            for entry in read_dir(&p)? {
                ls.push(entry?);
            }
            let known_nodes = self.index.list(get_key(self.config.path(), &p))?;

            // process each item that exists
            for entry in &ls {

                let ftype = entry.file_type()?;
                if ftype.is_symlink() {
                    // TODO handle symlinks
                    warn!("Skipping symlink {:?}", entry.file_name());
                    continue;
                }

                let entry_path = entry.path();

                self.process_change(backup_set, Change::new(entry_path.clone()))?;

                if entry_path.is_dir() {
                    debug!("Scan dir  {:?}", entry_path);
                    queue.push_front(entry_path.to_str().unwrap().to_string());
                }

            }

            // check each item we know about still exists
            // i.e. check for deleted ndoes
            debug!("known_nodes.len={}", known_nodes.len());
            for known_node in known_nodes {
                debug!("Checking {}", known_node.path());
                let mut found = false;
                let mut found_at = 0;
                for i in 0..ls.len() {
                    let entry = &ls.get(i).unwrap();
                    let entry_key = get_key(self.config.path(), entry.path().to_str().unwrap());
                    // debug!("Compare {} and {:?}", known_node.path, entry_key);
                    if known_node.path() == entry_key {
                        found = true;
                        found_at = i;
                        break;
                    }
                }
                if found {
                    // remove from search list to speed up iteration
                    let removed = ls.remove(found_at);
                    assert_eq!(&get_key(self.config.path(), removed.path().to_str().unwrap()),
                               known_node.path());
                } else {
                    debug!("Found node no longer on disk: {}", known_node.path());
                    let mut change_path = PathBuf::new();
                    change_path.push(self.config.path());
                    change_path.push(&known_node.path());
                    self.process_change(backup_set, Change::new(change_path))?;
                }
            }

        }

        info!("Full scan complete");
        Ok(())
    }
}

fn is_excluded(excludes: &HashSet<String>, change: &Change, base_path: &str) -> bool {
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

        info!("Starting backup engine on {}", self.config.path());

        let changes = Arc::new(Mutex::new(HashSet::new()));

        {
            let watcher =
                try!(self.backup_path.watcher().map_err(|e| DefaultEngineError::StartWatcher(e)));
            let changes = changes.clone();
            let local_excludes = self.excludes.clone();
            let local_path = self.config.path().to_string();
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

        {
            let now = time::now_utc().to_timespec();
            let backup_set = self.index.create_backup_set(now.sec)?;
            self.scan(backup_set)?;
        }

        loop {
            let now = time::now_utc().to_timespec();
            let seconds_div = (now.sec / self.config.period() as i64) as i64;
            let seconds = (seconds_div + 1) * self.config.period() as i64;
            let next_time = Timespec::new(seconds, 0);

            loop {
                let now = time::now_utc().to_timespec();
                if now >= next_time {
                    break;
                }
                sleep(Duration::new(1, 0));
            }

            info!("Beginning backup run");

            let mut work_queue = vec![];
            {
                let mut changes = changes.lock().unwrap();
                for c in changes.drain() {
                    // drain changes into the work queue
                    work_queue.push(c);
                }
            }

            if work_queue.len() > 0 {
                let backup_set = self.index.create_backup_set(next_time.sec)?;
                for change in work_queue {
                    self.process_change(backup_set, change).unwrap();
                }
            }

        }
    }

    fn process_change(&mut self, backup_set: u64, change: Change) -> StdResult<(), Box<StdError>> {
        if is_excluded(&self.excludes, &change, self.config.path()) {
            trace!("Skipping excluded path: {:?}", change.path());
            return Ok(());
        }

        debug!("Received {:?}", change);

        let change_path_str = change.path().to_str().unwrap();
        let key = get_key(self.config.path(), change_path_str);
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
                            .insert(existing_node.as_deleted().with_backup_set(backup_set))
                            .map_err(|e| DefaultEngineError::Index(e))?;
                    }
                }
            }
            Some(new_node) => {

                if new_node.size() > 1024 * 1024 * 10 {
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
                                    .send(self.config.path().to_string(), new_node)
                                    .map_err(|e| {
                                        DefaultEngineError::Storage(format!("Failed to send \
                                                                             file: {}",
                                                                            key),
                                                                    e)
                                    }))
                            }
                        };
                        let _inserted_file = try!(self.index
                            .insert(sent_file.with_backup_set(backup_set))
                            .map_err(|e| DefaultEngineError::Index(e)));
                    }
                    Some(existing_node) => {

                        // no need to update directory
                        if existing_node.is_dir() && new_node.is_dir() {
                            debug!("  {} (skipping dir)", key);
                            return Ok(());
                        }

                        // size and mtime match, skip.
                        if new_node.size() == existing_node.size() &&
                           new_node.mtime() == existing_node.mtime() {
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
                                    .send(self.config.path().to_string(), new_node)
                                    .map_err(|e| {
                                        DefaultEngineError::Storage(format!("Failed to send \
                                                                             file: {}",
                                                                            key),
                                                                    e)
                                    }))
                            }
                        };
                        let _inserted_file = try!(self.index
                            .insert(sent_file.with_backup_set(backup_set))
                            .map_err(|e| DefaultEngineError::Index(e)));
                    }
                }
            }
        }

        Ok(())
    }
}
