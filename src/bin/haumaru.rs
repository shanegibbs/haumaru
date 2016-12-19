#![deny(warnings)]
#[macro_use]
extern crate log;
extern crate haumaru;
extern crate haumaru_api;
extern crate clap;

use clap::{App, AppSettings, Arg, SubCommand};
use std::error::Error;
use std::fmt;
use std::path;

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
               default_working: &'a str,
               default_config_file: &'a str)
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
            .default_value(default_config_file)
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
                .default_value(default_working)
                .takes_value(true)
                .required(true)))
        .subcommand(SubCommand::with_name("verify")
            .about("Verify backup integrity")
            .setting(AppSettings::TrailingVarArg)
            .arg(Arg::with_name("working")
                .long("working")
                .short("w")
                .value_name("PATH")
                .help("Working path for haumaru")
                .default_value(default_working)
                .takes_value(true)
                .required(true))
            .arg(Arg::with_name("like").multiple(true)))
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
                .default_value(default_working)
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
                .default_value(default_working)
                .takes_value(true)
                .required(true)));

}

fn find_default_config_file() -> (String, String, String) {
    use std::path::{Path, PathBuf};

    let mut current_dir: Option<PathBuf> =
        Some(Path::new(".").canonicalize().expect("canonicalize").to_path_buf());

    while let Some(c) = current_dir {
        let mut working_path = c.clone();
        working_path.push(".haumaru");
        let mut default_config_file = working_path.clone();
        default_config_file.push("config.yml");
        if default_config_file.exists() && default_config_file.is_file() {
            debug!("Found config at {:?}", default_config_file);
            return (c.to_str()
                        .expect("c.to_str")
                        .to_string(),
                    working_path.to_str()
                        .expect("c.to_str")
                        .to_string(),
                    default_config_file.to_str()
                        .expect("default_config_file.to_str")
                        .to_string());
        }
        current_dir = c.parent().map(|c| c.to_path_buf());
    }

    (".".to_string(), ".haumaru".to_string(), ".haumaru/config.yml".to_string())
}

fn config_with_args(config: haumaru_api::Config,
                    cmd: &clap::ArgMatches)
                    -> Result<haumaru_api::Config, haumaru_api::HaumaruError> {
    let mut config = config;

    if config.path().is_none() {
        // if path is not in config, set it from cmd if it exists
        if let Some(path) = cmd.value_of("path") {
            config.set_path(path.to_string());
        }
    } else {
        // override config if path is specified on cli
        if cmd.occurrences_of("path") > 0 {
            config.set_path(cmd.value_of("path").expect("path from cli").to_string());
        }
    }

    if config.working().is_none() {
        // if working is not in config, set it from cmd if it exists
        if let Some(working) = cmd.value_of("working") {
            config.set_working(working.to_string());
        }
    } else {
        // override config if working is specified on cli
        if cmd.occurrences_of("working") > 0 {
            config.set_working(cmd.value_of("working").expect("working from cli").to_string());
        }
    }

    info!("{:?}", config);
    Ok(config)
}

fn run() -> Result<i64, Box<Error>> {
    let (mut default_path, mut default_working, default_config_file) = find_default_config_file();
    {
        // load defaults from config file
        if path::Path::new(&default_config_file).exists() {
            debug!("Loading auto found config at {}", default_config_file);
            let found_config = File::open(default_config_file.clone())?
                .as_config()
                .map_err(|e| format!("Failed to load config from {}: {}", default_config_file, e))?;
            if let Some(path) = found_config.path() {
                default_path = path;
            }
            if let Some(working) = found_config.working() {
                default_working = working;
            }
        } else {
            debug!("Config file not auto found at {}", default_config_file);
        }
    }

    let matches = app(default_path.as_str(),
                      default_working.as_str(),
                      default_config_file.as_str())
        .get_matches();

    use std::fs::File;
    use haumaru_api::AsConfig;

    let config = matches.value_of("config").ok_or(CliError::Missing("config".to_string()))?;
    info!("Using config at {}", config);

    let user_config =
        File::open(config).map_err(|e| format!("Failed to open config file {}: {}", config, e))?
            .as_config()
            .map_err(|e| format!("Failed to load config from {}: {}", config, e))?;
    debug!("{:?}", user_config);

    if let Some(cmd) = matches.subcommand_matches("backup") {
        haumaru_api::run(config_with_args(user_config, &cmd)?)?;

    } else if let Some(cmd) = matches.subcommand_matches("verify") {
        let mut like = "%".to_owned();
        let like_arg = cmd.value_of("like");
        if let Some(has_like_arg) = like_arg {
            like = has_like_arg.to_owned();
        }
        haumaru_api::verify(config_with_args(user_config, &cmd)?, like)?;

    } else if let Some(cmd) = matches.subcommand_matches("ls") {
        let key = cmd.value_of("key").ok_or(CliError::Missing("key".to_string()))?;
        haumaru_api::list(config_with_args(user_config, &cmd)?, key)?;

    } else if let Some(cmd) = matches.subcommand_matches("restore") {
        let key = cmd.value_of("key").ok_or(CliError::Missing("key".to_string()))?;
        let target = cmd.value_of("target").ok_or(CliError::Missing("target".to_string()))?;
        haumaru_api::restore(config_with_args(user_config, &cmd)?, key, target)?;

    } else {
        app(default_path.as_str(),
            default_working.as_str(),
            default_config_file.as_str())
            .print_help()
            .unwrap();
        println!("");
    }

    Ok(0)
}

fn main() {
    haumaru::setup_logging("info");
    debug!("Logging setup");

    match run() {
        Err(e) => {
            error!("{}", e);
            debug!("{:?}", e);
            return;
        }
        Ok(_) => (),
    };

}
