mod watcher;

use {Node, get_key};
pub use filesystem::watcher::Change;
pub use filesystem::watcher::Watcher;
use notify::Error as NotifyError;
use notify::Event;
use notify::RecommendedWatcher;
use notify::Watcher as NotifyWatcher;
use std::{fmt, fs, io};
use std::error::Error;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::result::Result as StdResult;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::channel;
use std::time::UNIX_EPOCH;
use time::Timespec;

pub type Result<T> = StdResult<T, BackupPathError>;

#[derive(Debug)]
pub enum BackupPathError {
    CreateWatcher(NotifyError),
    StartWatcher(NotifyError),
    Metadata(io::Error),
    ReadMtime(io::Error),
    ReadCtime(io::Error),
    Scan(String),
    UnknownFileType,
}

impl fmt::Display for BackupPathError {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        match *self {
            BackupPathError::CreateWatcher(ref e) => {
                write!(f, "Unable to create watcher: {}", e).unwrap()
            }
            BackupPathError::StartWatcher(ref e) => {
                write!(f, "Unable to start watcher: {}", e).unwrap()
            }
            BackupPathError::Metadata(ref e) => {
                write!(f, "Unable to get file metadata: {}", e).unwrap()
            }
            BackupPathError::ReadMtime(ref e) => write!(f, "Unable read mtime: {}", e).unwrap(),
            BackupPathError::ReadCtime(ref e) => write!(f, "Unable to read ctime: {}", e).unwrap(),
            BackupPathError::Scan(ref e) => write!(f, "Failed to scan: {}", e).unwrap(),
            BackupPathError::UnknownFileType => write!(f, "Unknown file type").unwrap(),
        }
        Ok(())
    }
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
        debug!("Creating BackupPath on {}", path);
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
        match metadata.created() {
            Ok(csystime) => {
                if csystime > msystime {
                    msystime = csystime;
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::Other &&
                   e.description() == "creation time is not available on this platform currently" {
                    debug!("warn: ctime not supported on this platform yet")
                } else {
                    return Err(BackupPathError::ReadCtime(e));
                }
            }
        }

        let mtime_secs = msystime.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mtime = Timespec::new(mtime_secs as i64, 0);

        let mode = metadata.permissions().mode();

        let key = get_key(&self.path, path.to_str().unwrap());
        debug!("self.path = {}", self.path);
        debug!("get_file key = {}", key);

        if metadata.is_file() {
            return Ok(Some(Node::new_file(key, mtime, metadata.len(), mode)));
        }

        if metadata.is_dir() {
            return Ok(Some(Node::new_dir(key, mtime, mode)));
        }

        Err(BackupPathError::UnknownFileType)
    }

    /// Take watcher
    pub fn watcher(&mut self) -> Result<Watcher> {
        debug!("Starting watcher on {}", &self.path);
        self.watcher.watch(&self.path).map_err(|e| BackupPathError::StartWatcher(e))?;
        Ok(Watcher::new(self.rx.take().unwrap()))
    }
}
