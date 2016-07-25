extern crate haumaru;
extern crate haumaru_api;

fn main() {
    haumaru::setup_logging("info");

    match haumaru_api::run("/Users/sgibbs/Documents") {
        Err(e) => {
            println!("ERROR: {}", e);
            println!("{:?}", e);
            return;
        }
        Ok(_) => (),
    };
}
