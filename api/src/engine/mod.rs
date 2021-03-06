use std::path::PathBuf;
use std::result::Result as StdResult;
use std::thread;
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::io::{Write, Cursor, copy};
use std::fs::File;
use time::{Timespec, at, strftime};
use rustc_serialize::hex::ToHex;
use std::error::Error as StdError;

use {Node, Index, Storage, get_key};
use filesystem::{Change, BackupPath};
use queue::Queue;
use engine::pre_send::PreSendWorker;
use storage::SendRequest;

mod config;
mod pre_send;
pub use self::config::EngineConfig;

mod error;
pub use self::error::DefaultEngineError;

mod engine;

#[cfg(test)]
mod test;

pub type Result<T> = StdResult<T, DefaultEngineError>;

pub struct DefaultEngine<I, S>
    where I: Index + Send + Clone,
          S: Storage
{
    config: EngineConfig,
    excludes: HashSet<String>,
    index: I,
    storage: S,
    backup_path: Option<BackupPath>,
    pre_send_queue: Queue<Node>,
    send_queue: Queue<SendRequest>,
    sent_queue: Queue<Node>,
}

impl<I, S> DefaultEngine<I, S>
    where I: Index + Send + Clone + 'static,
          S: Storage + 'static
{
    pub fn new(config: EngineConfig,
               excludes: HashSet<String>,
               index: I,
               storage: S)
               -> StdResult<Self, Box<StdError>> {

        let pre_send_queue = Queue::new("pre-process").with_max_len(4);
        let send_queue = Queue::new("send").with_max_len(4);
        let sent_queue = Queue::new("sent").with_max_len(4);

        if config.is_detached() {
            Ok(DefaultEngine {
                config: config,
                excludes: excludes,
                index: index,
                storage: storage,
                backup_path: None,
                pre_send_queue: pre_send_queue,
                send_queue: send_queue,
                sent_queue: sent_queue,
            })

        } else {

            let mut config = config;
            let path_buf = PathBuf::from(config.path()).canonicalize()
                .map_err(|e| {
                    DefaultEngineError::Other(format!("Unable to canonicalize backup path {}: {}",
                                                      config.path(),
                                                      e))
                })?;
            let abs_path = path_buf.to_str().unwrap().to_string();
            config.set_path(Some(abs_path.clone()));

            debug!("Base path: {}", config.path());
            debug!("Exclude paths: {:?}", excludes);

            let bp = try!(BackupPath::new(abs_path.clone())
                .map_err(|e| DefaultEngineError::CreateBackupPath(e)));

            let de = DefaultEngine {
                config: config,
                excludes: excludes,
                index: index.clone(),
                storage: storage.clone(),
                backup_path: Some(bp),
                pre_send_queue: pre_send_queue.clone(),
                send_queue: send_queue.clone(),
                sent_queue: sent_queue.clone(),
            };

            // pre-processing worker threads that [pre_send -> send] queues
            for _ in 0..4 {
                let worker = PreSendWorker::new(de.config.clone(),
                                                pre_send_queue.clone(),
                                                send_queue.clone());
                thread::spawn(move || {
                    worker.run();
                });
            }

            // sending worker threads that [send -> sent]
            for _ in 0..12 {
                let mut send_queue = send_queue.clone();
                let mut sent_queue = sent_queue.clone();
                let storage = storage.clone();
                thread::spawn(move || {
                    loop {
                        let mut item = send_queue.pop();
                        let path = item.as_ref().node().path().to_string();
                        match storage.send(item.as_mut()) {
                            Ok(()) => {
                                sent_queue.push(item.as_ref().node().clone());
                                item.success();
                            }
                            Err(e) => error!("Failing sending {}: {}", path, e),
                        }
                    }
                });
            }

            // insert node thread [sent -> db]
            {
                let mut sent_queue = sent_queue.clone();
                let mut index = index;
                thread::spawn(move || {
                    loop {
                        let item = sent_queue.pop();
                        let path = item.as_ref().path().to_string();
                        match index.insert(item.as_ref().clone()) {
                            Ok(n) => {
                                debug!("Inserted {} - {:?}", path, n);
                                item.success();
                            }
                            Err(e) => error!("Failed to insert {}: {}", path, e),
                        }
                    }
                });
            }

            Ok(de)
        }
    }

    pub fn backup_path(&mut self) -> &mut BackupPath {
        self.backup_path.as_mut().expect("some BackupPath")
    }

    pub fn scan_as_backup_set(&mut self, now: i64) -> StdResult<(), Box<StdError>> {
        let backup_set = self.index.create_backup_set(now).map_err(|e| box e)?;
        self.scan(backup_set)?;
        self.index.close_backup_set()?;
        Ok(())
    }

    pub fn wait_for_queue_drain(&mut self) {
        self.pre_send_queue.wait();
        self.send_queue.wait();
        self.sent_queue.wait();

        let pre_send_len = self.pre_send_queue.len();
        let send_queue_len = self.send_queue.len();
        let sent_queue_len = self.sent_queue.len();

        if pre_send_len + send_queue_len + sent_queue_len > 0 {
            let queue_stats = format!("{}/{}/{}", pre_send_len, send_queue_len, sent_queue_len);
            error!("Items still in queue: {}", queue_stats);
            panic!("Items still in queue");
        }

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
                    debug!("Skipping symlink {:?}", entry.file_name());
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

        self.wait_for_queue_drain();
        info!("Full scan complete");
        Ok(())
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
            .map_err(|e| DefaultEngineError::Index(box e))?;
        let file = self.backup_path()
            .get_file(change.path())
            .map_err(|e| DefaultEngineError::GetFile(e))?;

        let queue_stats = format!("{}/{}/{}",
                                  self.pre_send_queue.len(),
                                  self.send_queue.len(),
                                  self.sent_queue.len());

        match file {
            None => {
                match node {
                    None => {
                        debug!("Skipping transient {:?}", change);
                    }
                    Some(existing_node) => {
                        info!("{} - {}", queue_stats, key);
                        debug!("Detected DELETE on {:?}, {:?}", change, existing_node);
                        self.index
                            .insert(existing_node.as_deleted().with_backup_set(backup_set))
                            .map_err(|e| DefaultEngineError::Index(box e))?;
                    }
                }
            }
            Some(new_node) => {

                if let Some(size) = self.config.max_file_size() {
                    if new_node.size() > size {
                        debug!("Skipping large file {}", key);
                        return Ok(());
                    }
                }

                match node {
                    None => {
                        info!("{} + {}", queue_stats, key);
                        debug!("Detected NEW on {:?}, {:?}", change, new_node);
                        if let Err(e) = self.queue_for_send(new_node.with_backup_set(backup_set)) {
                            error!("Failed queuing new {}: {}", key, e);
                        }
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

                        info!("{} . {}", queue_stats, key);
                        debug!("Detected UPDATE on {:?},\n{:?},\n{:?}",
                               change,
                               existing_node,
                               new_node);
                        if let Err(e) = self.queue_for_send(new_node.with_backup_set(backup_set)) {
                            error!("Failed queuing updated {}: {}", key, e);
                        }
                    }
                }
            }
        }

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
            let mut outgest = File::create(&restore_path).map_err(|e| {
                    let msg = format!("Unable to create file  {}: {}", node.path(), e);
                    box DefaultEngineError::GeneralWithNode(msg, node.clone())
                })?;
            copy(&mut ingest, &mut outgest).map_err(|e| {
                    DefaultEngineError::GeneralWithNode(format!("Failed writing {}: {}",
                                                                restore_path_str,
                                                                e),
                                                        node.clone())
                })?;
        }

        Ok(())
    }

    fn queue_for_send(&mut self, n: Node) -> Result<()> {
        Ok(if n.is_file() {
            self.pre_send_queue.push(n);
        } else {
            self.index.insert(n).map_err(|e| DefaultEngineError::Index(box e))?;
            ()
        })
    }
}

pub fn perms_string(mode: u32) -> String {
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

pub fn is_excluded(excludes: &HashSet<String>, change: &Change, base_path: &str) -> bool {
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

pub fn write_ls_node(out: &mut Write, node: &Node) {
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
