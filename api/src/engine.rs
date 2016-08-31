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
use time;
use time::{Timespec, at, strftime};
use std::fs::create_dir_all;
use std::io::{Write, Cursor, copy};
use std::fs::File;
use rustc_serialize::hex::ToHex;

use filesystem::Change;
use {Node, Engine, Index, Storage, get_key};
use hasher::Hasher;

#[derive(Debug)]
pub struct EngineConfig {
    path: Option<String>,
    working: String,
    period: Option<u32>,
    max_file_size: Option<u64>,
    detached: bool,
}

impl EngineConfig {
    /// Create new config
    pub fn new(working: String) -> Self {
        EngineConfig {
            path: None,
            working: working,
            period: None,
            max_file_size: None,
            detached: false,
        }
    }

    pub fn with_path(mut self, path: String) -> Self {
        self.path = Some(path);
        self
    }

    pub fn with_period(mut self, period: u32) -> Self {
        self.period = Some(period);
        self
    }

    pub fn with_max_file_size(mut self, max_file_size: u64) -> Self {
        self.max_file_size = Some(max_file_size);
        self
    }

    pub fn detached(mut self) -> Self {
        self.detached = true;
        self
    }

    /// Create config for running without a backup path (for e.g. verify)
    pub fn new_detached(working: &str) -> EngineConfig {
        EngineConfig {
            path: None,
            working: working.to_string(),
            period: None,
            max_file_size: None,
            detached: true,
        }
    }
    pub fn path(&self) -> &str {
        self.path.as_ref().expect("path not specified")
    }
    pub fn working(&self) -> &str {
        &self.working
    }
    pub fn abs_working(&self) -> PathBuf {
        let mut working_path = PathBuf::new();
        working_path.push(self.working());
        create_dir_all(&working_path).unwrap();
        working_path.canonicalize().expect("Failed to get absolute path to working directory")
    }
    pub fn period(&self) -> u32 {
        self.period.expect("period not specified")
    }
    pub fn is_detached(&self) -> bool {
        self.detached
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

pub struct DefaultEngine<'i, I, S>
    where I: Index,
          S: Storage,
          I: 'i
{
    config: EngineConfig,
    excludes: HashSet<String>,
    index: &'i mut I,
    storage: S,
    backup_path: Option<BackupPath>,
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

        if config.is_detached() {
            Ok(DefaultEngine {
                config: config,
                excludes: excludes,
                index: index,
                storage: storage,
                backup_path: None,
            })

        } else {

            let mut config = config;
            let path_buf = PathBuf::from(config.path())
                .canonicalize()
                .map_err(|e| {
                    DefaultEngineError::Other(format!("Unable to canonicalize backup path {}: {}",
                                                      config.path(),
                                                      e))
                })?;
            let abs_path = path_buf.to_str().unwrap().to_string();
            config.path = Some(abs_path.clone());

            debug!("Base path: {}", config.path());
            debug!("Exclude paths: {:?}", excludes);

            let bp = try!(BackupPath::new(abs_path.clone())
                .map_err(|e| DefaultEngineError::CreateBackupPath(e)));

            Ok(DefaultEngine {
                config: config,
                excludes: excludes,
                index: index,
                storage: storage,
                backup_path: Some(bp),
            })
        }
    }

    pub fn backup_path(&mut self) -> &mut BackupPath {
        self.backup_path.as_mut().expect("some BackupPath")
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
            let known_nodes = self.index.list(get_key(self.config.path(), &p), None)?;

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

    fn restore_node(&mut self,
                    node: Node,
                    node_base: &str,
                    from: Option<Timespec>,
                    target: &str)
                    -> StdResult<(), Box<StdError>> {

        debug!("node_base={}", node_base);

        let n = match node_base.is_empty() {
            true => 0,
            false => node_base.len() + 1,
        };
        let node_restore_path = &node.path()[n..];
        debug!("node_restore_path={}", node_restore_path);

        let mut restore_path = PathBuf::new();
        restore_path.push(target);
        restore_path.push(node_restore_path);

        if node.is_dir() {
            debug!("Creating dir {:?}", restore_path);
            create_dir_all(restore_path)?;
            for node in self.index.list(node.path().to_string(), from)? {
                self.restore_node(node, node_base, from, target)?;
            }
        } else if node.is_file() {
            let hash = node.hash().as_ref().expect("File must have hash");

            debug!("Retrieving hash {}", hash.as_slice().to_hex());
            let mut ingest = match self.storage.retrieve(hash.as_slice())? {
                None => {
                    let msg = format!("Unable to restore {}, hash is missing from storage",
                                      node.path());
                    return Err(box DefaultEngineError::GeneralWithNode(msg, node.clone()));
                }
                Some(i) => i,
            };

            let restore_path_str = restore_path.to_str()
                .expect("restore_path_str string");

            debug!("Restoring {}", restore_path_str);
            let mut outgest = File::create(&restore_path)
                .map_err(|e| {
                    let msg = format!("Unable to create file  {}: {}", node.path(), e);
                    box DefaultEngineError::GeneralWithNode(msg, node.clone())
                })?;
            copy(&mut ingest, &mut outgest)
                .map_err(|e| {
                    DefaultEngineError::GeneralWithNode(format!("Failed writing {}: {}",
                                                                restore_path_str,
                                                                e),
                                                        node.clone())
                })?;
        }

        Ok(())
    }

    fn send(&self, mut n: Node) -> Result<Node> {
        use std::io::{Cursor, copy};

        debug!("Sending {:?}", n);

        let mut path = PathBuf::new();
        path.push(self.config.path());
        path.push(n.path());

        let mut buffer = Cursor::new(vec![]);

        let mut src_file = File::open(&path)
            .map_err(|e| DefaultEngineError::Storage(format!("Faild opening {:?}", path), box e))?;

        match copy(&mut src_file, &mut buffer) {
            Err(e) => {
                return Err(DefaultEngineError::Storage(format!("Faild reading {:?}", path), box e));
            }
            _ => (),
        };

        let size = buffer.position();
        buffer.set_position(0);

        let mut hasher = Hasher::new();
        match copy(&mut buffer, &mut hasher) {
            Err(e) => {
                return Err(DefaultEngineError::Storage(format!("Faild to hash {:?}", path), box e));
            }
            _ => (),
        };

        let (md5, sha256) = hasher.result();
        n.set_hash(sha256.clone());

        buffer.set_position(0);
        self.storage
            .send(&md5, &sha256, size, box buffer)
            .map_err(|e| DefaultEngineError::Storage(format!("Failed to send {}:", n.path()), e))?;

        Ok(n)
    }
}

fn perms_string(mode: u32) -> String {
    let mut out = Cursor::new(Vec::new());
    if mode & 2u32.pow(8) == 2u32.pow(8) {
        write!(out, "r").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    if mode & 2u32.pow(7) == 2u32.pow(7) {
        write!(out, "w").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    if mode & 2u32.pow(6) == 2u32.pow(6) {
        write!(out, "x").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    if mode & 2u32.pow(5) == 2u32.pow(5) {
        write!(out, "r").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    if mode & 2u32.pow(4) == 2u32.pow(4) {
        write!(out, "w").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    if mode & 2u32.pow(3) == 2u32.pow(3) {
        write!(out, "x").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    if mode & 2u32.pow(2) == 2u32.pow(2) {
        write!(out, "r").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    if mode & 2u32.pow(1) == 2u32.pow(1) {
        write!(out, "w").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    if mode & 2u32.pow(0) == 2u32.pow(0) {
        write!(out, "x").expect("write");
    } else {
        write!(out, "-").expect("write");
    }
    String::from_utf8(out.into_inner()).expect("from_utf8")
}

#[test]
fn test_perms_string() {
    assert_eq!("---------", &perms_string(0));
    assert_eq!("rwxrwxrwx", &perms_string(511));
    assert_eq!("rw-r--r--", &perms_string(420));
    assert_eq!("rw-------", &perms_string(384));
    assert_eq!("------rwx", &perms_string(7));
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
                self.backup_path().watcher().map_err(|e| DefaultEngineError::StartWatcher(e))?;
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

        let node = self.index
            .get(key.clone(), None)
            .map_err(|e| DefaultEngineError::Index(e))?;
        let file = self.backup_path()
            .get_file(change.path())
            .map_err(|e| DefaultEngineError::GetFile(e))?;

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

                if let Some(size) = self.config.max_file_size.as_ref() {
                    if new_node.size() > *size {
                        warn!("Skipping large file {}", key);
                        return Ok(());
                    }
                }

                match node {
                    None => {
                        info!("+ {}", key);
                        debug!("Detected NEW on {:?}, {:?}", change, new_node);
                        let sent_file = match new_node.is_dir() {
                            true => new_node,
                            false => self.send(new_node)?,
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
                            false => self.send(new_node)?,
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

    fn verify_store(&mut self) -> StdResult<(), Box<StdError>> {
        info!("Verifying store");
        let mut failed = vec![];
        let storage = &self.storage;

        self.index
            .visit_all_hashable(&mut |node| {
                if let Some(node) = storage.verify(node)? {
                    debug!("Verification failed for {:?}", node);
                    failed.push(node);
                }
                Ok(())
            })?;

        if failed.is_empty() {
            info!("Verification OK");
        }

        Ok(())
    }

    fn restore(&mut self,
               key: &str,
               from: Option<Timespec>,
               target: &str)
               -> StdResult<(), Box<StdError>> {

        if key.is_empty() {
            info!("Performing full restore to {}", target);

            create_dir_all(target)?;
            for node in self.index.list("".to_string(), from)? {
                self.restore_node(node, "", from, target)?;
            }
            Ok(())

        } else {

            info!("Restoring {} to {}", key, target);
            let node = match self.index.get(key.to_string(), from)? {
                Some(n) => n,
                None => {
                    return Err(box DefaultEngineError::Other(format!("Not Found: {:?}", key)));
                }
            };

            let mut tmp = PathBuf::new();
            tmp.push(key);
            let parent = tmp.parent().expect("restore.parent").to_str().expect("UTF-8 validity");
            debug!("Parent of key is {:?}", parent);

            self.restore_node(node, parent, from, target)
        }
    }

    fn list(&mut self,
            key: &str,
            from: Option<Timespec>,
            out: &mut Write)
            -> StdResult<(), Box<StdError>> {

        if key == "" {
            for node in self.index.list("".to_string(), from)? {
                write_ls_node(out, &node);
            }
            return Ok(());
        }

        let node = match self.index.get(key.to_string(), from)? {
            Some(n) => n,
            None => {
                return Err(box DefaultEngineError::Other(format!("Not Found: {}", key)));
            }
        };

        if node.is_file() {
            let t = at(node.mtime().clone());
            let tm = strftime("%b %e %H:%M %z", &t).expect("mtime format");
            write!(out, "Name:   {}\n", node.path()).expect("write");
            write!(out, "Size:   {} bytes\n", node.size()).expect("write");
            write!(out, "Time:   {}\n", tm).expect("write");
            write!(out, "SHA256: {}\n", node.hash_string()).expect("write");

        } else if node.is_dir() {
            for node in self.index.list(node.path().to_string(), from)? {
                write_ls_node(out, &node);
            }
        }

        Ok(())
    }
}

fn write_ls_node(out: &mut Write, node: &Node) {
    let d = match node.is_dir() {
        true => "d",
        false => "-",
    };
    let mode = perms_string(node.mode());
    let t = at(node.mtime().clone());
    let tm = strftime("%b %e %H:%M", &t).expect("mtime format");
    write!(out,
           "{}{} {}B {} {}\n",
           d,
           mode,
           node.size(),
           tm,
           node.path())
        .expect("write");
}

#[cfg(test)]
mod test {
    extern crate env_logger;

    use std::io::Cursor;
    use std::collections::HashSet;
    use rusqlite::Connection;
    use time::Timespec;

    use index::SqlLightIndex;
    use storage::LocalStorage;
    use engine::DefaultEngine;
    use {Node, Index, Engine, EngineConfig};

    // use super::*;

    fn test_list(key: &str, f: &Fn(&mut Index)) -> String {
        let _ = env_logger::init();

        let conn = Connection::open_in_memory().expect("conn");
        let mut index = SqlLightIndex::new(&conn).expect("index");
        let config = EngineConfig::new_detached("target/test/list_file");
        let store = LocalStorage::new(&config).expect("store");

        f(&mut index);

        let mut engine = DefaultEngine::new(config, HashSet::new(), &mut index, store)
            .expect("new engine");
        let mut cur = Cursor::new(Vec::new());
        engine.list(key, None, &mut cur).expect("list");
        String::from_utf8(cur.into_inner()).expect("from_utf8")
    }

    #[test]
    fn list_root_empty() {
        let output = test_list("", &|_index| {});
        assert_eq!("", output.as_str());
    }

    #[test]
    fn list_root() {
        let output = test_list("",
                               &|index| {
            index.insert(Node::new_file("a", Timespec::new(10, 0), 1024, 500)
                    .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31])
                    .with_backup_set(5))
                .expect("insert");
        });
        assert_eq!("-rwxrw-r-- 1024B Dec 31 18:00 a\n", output.as_str());
    }

    #[test]
    fn list_file() {
        let output = test_list("a",
                               &|index| {
            index.insert(Node::new_file("a", Timespec::new(10, 0), 1024, 500)
                    .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31])
                    .with_backup_set(5))
                .expect("insert");
        });
        assert_eq!("Name:   a\n\
                    Size:   1024 bytes\n\
                    Time:   Dec 31 18:00 -0600\n\
                    SHA256: 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
                   output.as_str());
    }

    #[test]
    fn list_dir() {
        let output = test_list("a",
                               &|index| {
            index.insert(Node::new_dir("a", Timespec::new(10, 0), 500).with_backup_set(5))
                .expect("insert dir");
            index.insert(Node::new_dir("a/dir", Timespec::new(10, 0), 488).with_backup_set(5))
                .expect("insert dir");
            index.insert(Node::new_file("a/file", Timespec::new(10, 0), 1024, 420)
                    .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                                    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31])
                    .with_backup_set(5))
                .expect("insert_file");
        });
        assert_eq!("drwxr-x--- 0B Dec 31 18:00 a/dir\n\
                    -rw-r--r-- 1024B Dec 31 18:00 a/file\n",
                   output.as_str());
    }

    #[test]
    fn list_empty_dir() {
        let output = test_list("a",
                               &|index| {
                                   index.insert(Node::new_dir("a", Timespec::new(10, 0), 500)
                                           .with_backup_set(5))
                                       .expect("insert dir");

                               });
        assert_eq!("", output.as_str());
    }

}
