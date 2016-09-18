use std::sync::Arc;
use std::sync::Mutex;
use std::error::Error;
use std::sync::mpsc::{channel, Sender, Receiver};
use time::Timespec;
use std::thread;
use std::fmt;

mod sql_light_index;
use {Node, Index, Record};
pub use index::sql_light_index::*;

#[derive(Debug)]
pub enum IndexError {
    Fatal(String, Option<Box<IndexError>>),
}

impl Error for IndexError {
    fn description(&self) -> &str {
        match *self {
            IndexError::Fatal(ref s, ref _e) => "Unrecoverable fatal error",
        }
    }
    fn cause(&self) -> Option<&Error> {
        match *self {
            IndexError::Fatal(ref _s, None) => None,
            IndexError::Fatal(ref _s, Some(ref e)) => Some(e.as_ref()),
        }
    }
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            IndexError::Fatal(ref s, None) => write!(f, "Fatal error: {}", s)?,
            IndexError::Fatal(ref s, Some(ref e)) => write!(f, "{}, caused by: {}", s, e)?,
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SingleThreadIndex {
    tx: Sender<IndexCommand>,
}

enum IndexCommand {
    Get(Sender<IndexCommandResponse>, String, Option<Timespec>),
}

enum IndexCommandResponse {
    Get(Result<Option<Node>, IndexError>),
}

struct IndexListener<I>
    where I: Index
{
    inner: I,
    rx: Receiver<IndexCommand>,
}

impl<I> IndexListener<I>
    where I: Index
{
    fn listen(&mut self) {
        loop {
            match self.rx.recv() {
                Ok(IndexCommand::Get(cb, s, t)) => {
                    let result = self.inner.get(s, t);
                    cb.send(IndexCommandResponse::Get(result));
                }
                Err(e) => {
                    error!("listen failed: {}", e);
                }
            }
        }
    }
}

impl SingleThreadIndex {
    pub fn new<I, F>(factory: F) -> Self
        where I: Index,
              F: Fn() -> I
    {
        let (tx, rx) = channel();
        // thread::spawn(move || {
        // IndexListener {
        // inner: factory(),
        // rx: rx,
        // }
        // .listen();
        // });
        SingleThreadIndex { tx: tx }
    }
}

impl Index for SingleThreadIndex {
    fn get(&mut self, path: String, from: Option<Timespec>) -> Result<Option<Node>, IndexError> {
        let (tx, rx) = channel();
        self.tx.send(IndexCommand::Get(tx, path, from));
        let IndexCommandResponse::Get(r) = rx.recv().expect("get.recv");
        r
    }
    fn list(&mut self, path: String, from: Option<Timespec>) -> Result<Vec<Node>, IndexError> {
        Ok(vec![])
    }
    fn visit_all_hashable(&mut self,
                          f: &mut FnMut(Node) -> Result<(), IndexError>)
                          -> Result<(), IndexError> {
        Ok(())
    }
    fn insert(&mut self, node: Node) -> Result<Node, IndexError> {
        Ok(node)
    }
    fn create_backup_set(&mut self, timestamp: i64) -> Result<u64, IndexError> {
        Ok(0)
    }

    fn dump(&self) -> Vec<Record> {
        vec![]
    }
}