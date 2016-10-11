use std::io::{Write, Cursor};
use log::{LogRecord, LogLevelFilter, LogLevel};
use env_logger::LogBuilder;
use std::env;
use time;

pub fn setup_logging(default_log_str: &str) {

    let format = |record: &LogRecord| {
        let v: Vec<u8> = vec![];
        let mut buf = Cursor::new(v);

        let t = time::now();

        write!(buf, "{} ", t.rfc3339()).unwrap();

        write!(buf,
               "[{}",
               match record.level() {
                   LogLevel::Error => "\x1b[31m",
                   LogLevel::Warn => "\x1b[33m",
                   LogLevel::Info => "\x1b[34m",
                   LogLevel::Debug => "\x1b[36m",
                   LogLevel::Trace => "\x1b[36m",
               })
            .unwrap();

        write!(buf, "{}", record.level()).unwrap();
        if record.level() == LogLevel::Warn || record.level() == LogLevel::Info {
            write!(buf, " ").unwrap();
        }
        write!(buf, "\x1b[0m] ").unwrap();

        write!(buf, "{} ", record.location().module_path()).unwrap();

        write!(buf, "{}", record.args()).unwrap();

        return String::from_utf8(buf.into_inner()).unwrap();
    };

    let mut builder = LogBuilder::new();
    builder.format(format).filter(None, LogLevelFilter::Info);

    if let Ok(l) = env::var("LOG") {
        builder.parse(&l);
    } else {
        builder.parse(default_log_str);
    }

    builder.init().unwrap();
}