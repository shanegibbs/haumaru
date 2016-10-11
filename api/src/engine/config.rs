use std::path::PathBuf;
use std::fs::create_dir_all;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    path: Option<String>,
    working: String,
    period: Option<u32>,
    max_file_size: Option<u64>,
    bucket: Option<String>,
    prefix: Option<String>,
    detached: bool,
}

impl EngineConfig {
    /// Create new config
    pub fn new(working: &str) -> Self {
        EngineConfig {
            path: None,
            working: working.into(),
            period: None,
            max_file_size: None,
            bucket: None,
            prefix: None,
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

    pub fn with_bucket(mut self, bucket: &str) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn detached(mut self) -> Self {
        self.detached = true;
        self
    }

    /// Create config for running without a backup path (for e.g. verify)
    pub fn new_detached(working: &str) -> EngineConfig {
        Self::new(working).detached()
    }
    pub fn path(&self) -> &str {
        self.path.as_ref().expect("path not specified")
    }
    pub fn set_path(&mut self, path: Option<String>) {
        self.path = path;
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
    pub fn max_file_size(&self) -> Option<u64> {
        self.max_file_size.clone()
    }
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_ref().map(|s| s.as_ref())
    }
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_ref().map(|s| s.as_ref())
    }
    pub fn is_detached(&self) -> bool {
        self.detached
    }
}