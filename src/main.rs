use args::Args;
use getopts::Occur;
use log::*;

fn parse_arguments() -> Args {
    let mut args = Args::new("rusty-raft", "A simple implementation of the Rust algorithm.");
    args.flag("s", "simulated-bad-connection", "Simulates a bad connection by dropping packages.");
    args.option("n", "name", "The name of this server.", "NAME", Occur::Req, None);
    args.option("v", "verbosity", "Indicates log level, higher shows more", "VERBOSITY", Occur::Optional, Some(String::from("2")));

    if let Result::Err(message) = args.parse_from_cli() {
        println!("Failed to parse arguments: {}", message);
        println!("{}", args.full_usage());
        panic!("Exiting!");
    }

    args
}

fn main() {
    let args = parse_arguments();

    if let Result::Err(message) = stderrlog::new()
        .timestamp(stderrlog::Timestamp::Second)
        .verbosity(args.value_of("verbosity").unwrap())
        .init() {
            panic!("Failed to initialize log: {}!", message);
    };

    let name = if let Result::Ok(n) = args.value_of("name") {
        n
    } else {
        error!("No valid name given.");
        args.full_usage();
        panic!("Exiting!");
    };

    let bad_connection = if let Ok(bad) = args.value_of::<bool>("simulated-bad-connection") { bad } else { false };

    let (timeout_config, mut peers) = match rusty_raft::config::Config::new() {
        Ok((conf, peers)) => (conf, peers),
        Err(message) => {
            println!("Could not load configuration: {}", message);
            args.full_usage();
            panic!("Aborting!");
        }
    };
    if peers.len() < 3 {
        println!("Invalid list of peers. At least three are required.");
        args.full_usage();
        panic!("Aborting!");
    }

    let local_address = match peers.remove(&name) {
        Some(address) => address,
        None => {
            println!("Could not find the address of the node name.");
            args.full_usage();
            panic!("Aborting!");
        }
    };

    rusty_raft::run(name, peers, local_address, bad_connection, timeout_config);
}
