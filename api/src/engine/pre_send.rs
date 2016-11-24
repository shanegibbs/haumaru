use std::path::PathBuf;
use std::fs::File;

use engine::{EngineConfig, DefaultEngineError};
use Node;
use queue::Queue;
use hasher::Hasher;
use storage::{SendRequest, SendRequestReader};

pub struct PreSendWorker {
    config: EngineConfig,
    ingest: Queue<Node>,
    outgest: Queue<SendRequest>,
}

impl PreSendWorker {
    pub fn new(config: EngineConfig, ingest: Queue<Node>, outgest: Queue<SendRequest>) -> Self {
        PreSendWorker {
            config: config,
            ingest: ingest,
            outgest: outgest,
        }
    }
    pub fn run(mut self) {
        loop {
            let item = self.ingest.pop();

            match self.process(item.as_ref()) {
                Ok(req) => {
                    self.outgest.push(req);
                    item.success();
                }
                Err(e) => {
                    error!("Failed processing: {}", e);
                    continue;
                }
            }
        }
    }

    fn process(&self, node: &Node) -> Result<SendRequest, DefaultEngineError> {
        use std::io::{Cursor, copy};

        assert!(node.is_file(), true);

        debug!("Processing {}", node.path());

        let mut path = PathBuf::new();
        path.push(self.config.path());
        path.push(node.path());

        let mut buffer = Cursor::new(vec![]);

        let mut src_file = File::open(&path)
            .map_err(|e| DefaultEngineError::Storage(format!("Failed opening {:?}", path), box e))?;

        match copy(&mut src_file, &mut buffer) {
            Err(e) => {
                return Err(DefaultEngineError::Storage(format!("Failed reading {:?}", path),
                                                       box e));
            }
            _ => (),
        };

        let size = buffer.position();
        buffer.set_position(0);

        let mut hasher = Hasher::new();
        match copy(&mut buffer, &mut hasher) {
            Err(e) => {
                return Err(DefaultEngineError::Storage(format!("Failed to hash {:?}", path),
                                                       box e));
            }
            _ => (),
        };

        let (md5, sha256) = hasher.result();
        let mut node = node.clone();
        node.set_hash(sha256.clone());

        buffer.set_position(0);

        let reader = SendRequestReader::InMemory(buffer);
        debug!("Processing {} complete", node.path());
        Ok(SendRequest::new(md5, sha256, node, reader, size))
    }
}
