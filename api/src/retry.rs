#![allow(warnings)]

use std::fmt::Display;

pub fn retry_forever<F, T, E>(mut f: F) -> T
    where F: FnMut() -> Result<T, E>,
          E: Display
{
    let mut i = 1;
    loop {
        match f() {
            Ok(t) => return t,
            Err(e) => {
                warn!("Attempt {}. {}", i, e);
            }
        }
        i += 1;
    }
}
