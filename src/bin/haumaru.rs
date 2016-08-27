#![deny(warnings)]
#![feature(question_mark)]
#[macro_use]
extern crate log;
extern crate haumaru;
extern crate haumaru_api;
extern crate clap;

use std::error::Error;
use clap::{Arg, App, SubCommand};
use std::fmt;

#[derive(Debug)]
enum CliError {
    Missing(String),
}

impl Error for CliError {
    fn description(&self) -> &str {
        "CliError"
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            CliError::Missing(ref s) => write!(f, "Missing arg: {}", s).unwrap(),
        };
        Ok(())
    }
}

fn app<'a, 'b>(default_path: &'a str,
               default_working_path: &'a str,
               config_file: &'a str)
               -> App<'a, 'b> {
    return App::new("haumaru")
        .version("0.0.1a")
        .author("Shane Gibbs <shane@hands.net.nz>")
        .about("Performs and manages file backups to multiple targets.")
        .arg(Arg::with_name("config")
            .long("config")
            .short("c")
            .value_name("FILE")
            .help("Backup config")
            .default_value(config_file)
            .takes_value(true))
        .subcommand(SubCommand::with_name("backup")
            .about("Start backup service")
            .arg(Arg::with_name("path")
                .long("path")
                .short("p")
                .value_name("PATH")
                .help("Path to backup")
                .default_value(default_path)
                .takes_value(true))
            .arg(Arg::with_name("working")
                .long("working")
                .short("w")
                .value_name("PATH")
                .help("Working path for haumaru")
                .default_value(default_working_path)
                .takes_value(true)
                .required(true)))
        .subcommand(SubCommand::with_name("verify")
            .about("Verify backup integrity")
            .arg(Arg::with_name("working")
                .long("working")
                .short("w")
                .value_name("PATH")
                .help("Working path for haumaru")
                .default_value(default_working_path)
                .takes_value(true)
                .required(true)))
        .subcommand(SubCommand::with_name("ls")
            .about("List file(s)")
            .arg(Arg::with_name("key")
                .long("key")
                .short("k")
                .value_name("KEY")
                .help("List file(s) on key. Format: [<path>][@<utc_unix_ts>]")
                .default_value("")
                .takes_value(true)
                .required(true))
            .arg(Arg::with_name("working")
                .long("working")
                .short("w")
                .value_name("PATH")
                .help("Working path for haumaru")
                .default_value(default_working_path)
                .takes_value(true)
                .required(true)))
        .subcommand(SubCommand::with_name("restore")
            .about("Restore file(s)")
            .arg(Arg::with_name("key")
                .long("key")
                .short("k")
                .value_name("KEY")
                .help("Restore file(s) on key. Format: [<path>][@<utc_unix_ts>]")
                .default_value("")
                .takes_value(true)
                .required(true))
            .arg(Arg::with_name("target")
                .long("target")
                .short("t")
                .value_name("PATH")
                .help("Destination to restore file(s) to.")
                .default_value(".")
                .takes_value(true)
                .required(true))
            .arg(Arg::with_name("working")
                .long("working")
                .short("w")
                .value_name("PATH")
                .help("Working path for haumaru")
                .default_value(default_working_path)
                .takes_value(true)
                .required(true)));

}

fn find_config_file() -> (String, String, String) {
    use std::path::{Path, PathBuf};

    let mut current_dir: Option<PathBuf> =
        Some(Path::new(".").canonicalize().expect("canonicalize").to_path_buf());

    while let Some(c) = current_dir {
        let mut working_path = c.clone();
        working_path.push(".haumaru");
        let mut config_file = working_path.clone();
        config_file.push("config.yml");
        if config_file.exists() && config_file.is_file() {
            info!("Found config at {:?}", config_file);
            return (c.to_str()
                .expect("c.to_str")
                .to_string(),
                    working_path.to_str()
                .expect("c.to_str")
                .to_string(),
                    config_file.to_str()
                .expect("config_file.to_str")
                .to_string());
        }
        current_dir = c.parent().map(|c| c.to_path_buf());
    }

    (".".to_string(), ".haumaru".to_string(), ".haumaru/config.yml".to_string())
}

fn run() -> Result<i64, Box<Error>> {
    let (default_path, default_working_path, config_file) = find_config_file();
    let matches = app(default_path.as_str(),
                      default_working_path.as_str(),
                      config_file.as_str())
        .get_matches();

    let config = matches.value_of("config").ok_or(CliError::Missing("config".to_string()))?;
    info!("Using config from {}", config);

    use std::fs::File;
    use haumaru_api::AsConfig;

    let mut config_f = File::open(config)?;
    let user_config = config_f.as_config()?;
    info!("{:?}", user_config);

    if let Some(cmd) = matches.subcommand_matches("backup") {
        let path = cmd.value_of("path").ok_or(CliError::Missing("path".to_string()))?;
        let working = cmd.value_of("working").ok_or(CliError::Missing("working".to_string()))?;
        let config = haumaru_api::EngineConfig::new(path, working, &user_config.period())?;
        haumaru_api::run(config)?;

    } else if let Some(cmd) = matches.subcommand_matches("verify") {
        let working = cmd.value_of("working").ok_or(CliError::Missing("working".to_string()))?;
        let config = haumaru_api::EngineConfig::new_detached(working);
        haumaru_api::verify(config)?;

    } else if let Some(cmd) = matches.subcommand_matches("ls") {
        let working = cmd.value_of("working").ok_or(CliError::Missing("working".to_string()))?;
        let key = cmd.value_of("key").ok_or(CliError::Missing("key".to_string()))?;

        let config = haumaru_api::EngineConfig::new_detached(working);
        haumaru_api::list(config, key)?;

    } else if let Some(cmd) = matches.subcommand_matches("restore") {
        let working = cmd.value_of("working").ok_or(CliError::Missing("working".to_string()))?;
        let key = cmd.value_of("key").ok_or(CliError::Missing("key".to_string()))?;
        let target = cmd.value_of("target").ok_or(CliError::Missing("target".to_string()))?;

        let config = haumaru_api::EngineConfig::new_detached(working);
        haumaru_api::restore(config, key, target)?;

    } else {
        app(default_path.as_str(),
            default_working_path.as_str(),
            config_file.as_str())
            .print_help()
            .unwrap();
        println!("");
    }

    Ok(0)
}

fn main() {
    haumaru::setup_logging("info");

    match run() {
        Err(e) => {
            error!("{}", e);
            debug!("{:?}", e);
            return;
        }
        Ok(_) => (),
    };

}
