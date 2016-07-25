mod watcher;

use notify;
use notify::Event;
use notify::Error as NotifyError;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::sync_channel;
use std::thread;
use notify::RecommendedWatcher;
use notify::Watcher as NotifyWatcher;
use std::sync::mpsc::channel;
use std::path::Path;
use std::result::Result as StdResult;
use std::{fs, io};
use std::time::UNIX_EPOCH;
use std::os::unix::fs::PermissionsExt;
use time::Timespec;
use std::path::PathBuf;

use Node;

pub use filesystem::watcher::Change;
pub use filesystem::watcher::Watcher;

pub type Result<T> = StdResult<T, BackupPathError>;

#[derive(Debug)]
pub enum BackupPathError {
    CreateWatcher(NotifyError),
    StartWatcher(NotifyError),
    Metadata(io::Error),
    ReadMtime(io::Error),
    ReadCtime(io::Error),
    UnknownFileType,
}

pub struct BackupPath {
    path: String,
    watcher: RecommendedWatcher,
    rx: Option<Receiver<Event>>,
}

impl BackupPath {
    pub fn new<S>(path: S) -> Result<Self>
        where S: Into<String>
    {
        let path = path.into();
        let (tx, rx) = channel();
        Ok(BackupPath {
            path: path,
            watcher: try!(NotifyWatcher::new(tx).map_err(|e| BackupPathError::CreateWatcher(e))),
            rx: Some(rx),
        })
    }

    pub fn get_file(&self, path: &Path) -> Result<Option<Node>> {
        let metadata = match fs::metadata(path) {
            Ok(m) => m,
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(BackupPathError::Metadata(e));
                }
            }
        };

        let mut msystime = try!(metadata.modified().map_err(|e| BackupPathError::ReadMtime(e)));
        let csystime = try!(metadata.created().map_err(|e| BackupPathError::ReadCtime(e)));
        if csystime > msystime {
            msystime = csystime;
        }

        let mtime_secs = msystime.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mtime = Timespec::new(mtime_secs as i64, 0);

        let mode = metadata.permissions().mode();

        if metadata.is_file() {
            return Ok(Some(Node::new_file(&path.to_path_buf(), mtime, mode, metadata.len())));
        }

        if metadata.is_dir() {
            return Ok(Some(Node::new_file(&path.to_path_buf(), mtime, mode, metadata.len())));
        }

        Err(BackupPathError::UnknownFileType)
    }

    pub fn scan(&self) {}

    /// Take watcher
    pub fn watcher(&mut self) -> Result<Watcher> {
        debug!("Starting watcher on {}", &self.path);
        try!(self.watcher.watch(&self.path).map_err(|e| BackupPathError::StartWatcher(e)));
        Ok(Watcher::new(self.rx.take().unwrap()))
    }
}
