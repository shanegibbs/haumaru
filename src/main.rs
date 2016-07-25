extern crate haumaru_api;
extern crate env_logger;

use haumaru_api::Engine;

fn main() {
    env_logger::init().unwrap();

    let backup = haumaru_api::new("/Users/sgibbs/Documents");
    backup.run().unwrap();
}
