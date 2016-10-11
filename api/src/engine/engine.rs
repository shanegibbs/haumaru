use std::path::PathBuf;
use std::result::Result as StdResult;
use std::thread;
use std::thread::sleep;
use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use std::time::Duration;
use std::fs::create_dir_all;
use std::io::Write;
use time;
use time::{Timespec, at, strftime};
use std::error::Error as StdError;

use super::*;
use {Engine, Index, Storage, get_key};
use filesystem::Change;
use index::IndexError;

impl<I, S> Engine for DefaultEngine<I, S>
    where I: Index + Send + Clone + 'static,
          S: Storage + 'static
{
    fn run(&mut self) -> StdResult<(), Box<StdError>> {

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
            let backup_set = self.index.create_backup_set(now.sec).map_err(|e| box e)?;
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

            self.wait_for_queue_drain();

            info!("Backup run complete");

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
                            .insert(&existing_node.as_deleted().with_backup_set(backup_set))
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

    fn verify_store(&mut self) -> StdResult<(), Box<StdError>> {
        info!("Verifying store");
        let mut failed = vec![];
        let storage = &self.storage;

        self.index
            .visit_all_hashable(&mut |node| {
                if let Some(node) = storage.verify(node)
                    .map_err(|e| IndexError::Fatal(format!("Verify error: {}", e), None))? {
                    error!("Verification failed for {}", node.hash_string());
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