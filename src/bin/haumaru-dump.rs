extern crate haumaru_api;
extern crate env_logger;

fn main() {
    env_logger::init().unwrap();
    match haumaru_api::dump() {
        Err(e) => {
            println!("ERROR: {:?}", e);
            return;
        }
        Ok(_) => (),
    };
}
