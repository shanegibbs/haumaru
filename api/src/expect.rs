use std::fmt::Display;

pub trait Expectable<T> {
    fn as_expect(self) -> Result<T, String>;
}

impl<T> Expectable<T> for Option<T> {
    fn as_expect(self) -> Result<T, String> {
        self.ok_or(format!("was None"))
    }
}

impl<T, E: Display> Expectable<T> for Result<T, E> {
    fn as_expect(self) -> Result<T, String> {
        self.map_err(|e| format!("failed: {}", e))
    }
}

/// Macro designed to replace .expect(n) on Option and Result.
/// Advantage being that it panics at the calling site, and also adds a nicer description.
macro_rules! expect {
    ($x:expr, $msg:expr) => {{
        use ::expect::Expectable;
        match $x.as_expect() {
            Ok(t) => t,
            Err(e) => panic!(format!("Expected {:?}, but {}", $msg, e)),
        }
    }}
}