use {Engine, Index, Storage};
use filesystem::Change;
use index::IndexError;
use std::collections::HashSet;
use std::error::Error as StdError;
use std::fs::create_dir_all;
use std::io::Write;
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use super::*;
use time;
use time::{Timespec, at, strftime};

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

        // full scan into backup set
        let now = time::now_utc().to_timespec();
        self.scan_as_backup_set(now.sec)?;

        // start long running backup loop
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

            self.process_changes(next_time.sec, work_queue)?;
            info!("Backup run complete");
        }
    }

    fn process_changes(&mut self,
                       next_time: i64,
                       work_queue: Vec<Change>)
                       -> StdResult<(), Box<StdError>> {
        if work_queue.is_empty() {
            return Ok(());
        }
        let backup_set = self.index.create_backup_set(next_time)?;
        for change in work_queue {
            self.process_change(backup_set, change).unwrap();
        }
        self.wait_for_queue_drain();
        self.index.close_backup_set()?;
        Ok(())
    }

    fn verify_store(&mut self, like: String) -> StdResult<(), Box<StdError>> {
        info!("Verifying store");
        let mut failed = vec![];
        let storage = &self.storage;

        self.index
            .visit_all_hashable(like,
                                &mut |node| {
                let (node, valid) = storage.verify(node)
                    .map_err(|e| IndexError::Fatal(format!("Verify error: {}", e), None))?;
                if valid {
                    if valid {
                        info!("{:4} {} OK",
                              node.backup_set().expect("backup set"),
                              node.path());
                    } else {
                        error!("Verification failed for {}", node.hash_string());
                        failed.push(node);
                    }
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