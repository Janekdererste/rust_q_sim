use std::env;

fn main() {
    let update_interval = match env::var("RUST_Q_SIM_ROUTER_UPDATE_INTERVAL") {
        Ok(interval) => interval.parse::<u32>().unwrap_or({
            println!("unwarp failed");
            1
        }),
        Err(_) => {
            println!("no env variable");
            1
        }
    };

    println!("{}", update_interval)
}
