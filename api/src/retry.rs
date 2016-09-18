
pub fn retry_forever<F, T, E>(mut f: F) -> T
    where F: FnMut() -> Result<T, E>
{
    loop {
        if let Ok(t) = f() {
            return t;
        }
        warn!("Attempt failed");
    }
}
