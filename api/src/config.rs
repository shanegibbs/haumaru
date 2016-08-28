use std::convert::TryFrom;
use std::error::Error;
use std::io::Read;

use serde_yaml;

use {HaumaruError, EngineConfig};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    path: Option<String>,
    working: Option<String>,
    period: Option<String>,
    max_file_size: Option<String>,
    s3: Option<bool>,
}

impl Config {
    pub fn path(&self) -> Option<String> {
        self.path.clone()
    }

    pub fn working(&self) -> Option<String> {
        self.working.clone()
    }

    pub fn set_path(&mut self, path: String) {
        self.path = Some(path);
    }

    pub fn set_working(&mut self, working: String) {
        self.working = Some(working);
    }

    pub fn period(&self) -> String {
        self.period.clone().unwrap_or("900".to_string())
    }
}

pub trait AsConfig {
    fn as_config(&mut self) -> Result<Config, Box<Error>>;
}

impl<T: Read> AsConfig for T {
    fn as_config(&mut self) -> Result<Config, Box<Error>> {
        let mut buf = String::new();
        self.read_to_string(&mut buf)?;
        let config: Config = serde_yaml::from_str(&buf)
            .map_err(|e| box HaumaruError::Config(box e))?;
        Ok(config)
    }
}

impl TryFrom<Config> for EngineConfig {
    type Err = HaumaruError;
    fn try_from(c: Config) -> Result<Self, HaumaruError> {
        let working = c.working.expect("working");
        let mut config = EngineConfig::new(working);

        if let Some(path) = c.path {
            config = config.with_path(path);
        }

        if let Some(period) = c.period {
            config =
                config.with_period(period.parse::<u32>().map_err(|e| HaumaruError::Config(box e))?);
        }

        if let Some(max_file_size) = c.max_file_size {
            config = config.with_max_file_size(max_file_size.parse::<u64>()
                .map_err(|e| HaumaruError::Config(box e))?);
        }

        Ok(config)
    }
}
