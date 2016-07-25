use std::result::Result as StdResult;
use std::sync::mpsc::{Receiver, RecvError};
use notify::Event;
use std::path::{Path, PathBuf};

pub type Result<T> = StdResult<T, WatcherError>;

pub enum WatcherError {
    ChannelRecv(RecvError),
}

pub struct Watcher {
    rx: Receiver<Event>,
}

impl Watcher {
    pub fn new(rx: Receiver<Event>) -> Self {
        Watcher { rx: rx }
    }
    pub fn watch<F>(&self, mut f: F) -> Result<u64>
        where F: FnMut(Change)
    {
        loop {
            let event = try!(self.rx.recv().map_err(|e| WatcherError::ChannelRecv(e)));

            let path = match event.path.as_ref() {
                Some(p) => p,
                None => {
                    debug!("Received without path, ignoring: {:?}", event);
                    continue;
                }
            };

            let op = match event.op.as_ref() {
                Ok(o) => o,
                Err(e) => {
                    warn!("Received notify without op, ignoring: {:?}", event);
                    continue;
                }
            };

            debug!("Received notify {:?}, {:?}", op.clone(), path);

            f(Change::new(path.clone()));
        }
        Ok(0)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Change {
    path: PathBuf,
}

impl Change {
    pub fn new(path: PathBuf) -> Self {
        Change { path: path }
    }
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }
}
