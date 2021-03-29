use std::io;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use args::Args;
use getopts::Occur;
use log::*;

mod config;
mod context;
mod data;
mod engine;
mod external_communication;
mod messages;
mod roles;
mod system;
mod utils;

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

    let (timeout_config, mut peers) = match config::Config::new() {
        Ok((conf, peer)) => (conf, peer),
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
    let peer_names : Vec<String> = peers.iter().map(|(peer_name, _)| peer_name.clone()).collect();

    info!("Starting {}", name);

    let (inbound_channel_entrance, inbound_channel_exit) = mpsc::channel();
    let (outbound_channel_entrance, outbound_channel_exit) = mpsc::channel();
    let (log_channel, log_channel_reader) = mpsc::channel();

    let (loop_system_channel, loop_system_channel_reader) = mpsc::channel();

    let _external_connection = external_communication::ExternalConnection::new(inbound_channel_entrance, outbound_channel_exit, local_address, peers, bad_connection);

    let loop_handle = thread::spawn(move || { main_loop(name, loop_system_channel_reader, inbound_channel_exit, outbound_channel_entrance, log_channel_reader, &timeout_config, peer_names); });

    let stdin = io::stdin();
    loop {
        let mut buffer = String::new();
        match stdin.read_line(&mut buffer) {
            Ok(_) => {},
            Err(message) =>  {
                error!("Encountered error: {}", message);
                args.full_usage();
                info!("Exiting!");
                break;
            }
        };
        if buffer.trim().len() > 0 {
            match buffer.trim().parse::<i32>() {
                Ok(value) => {
                    match log_channel.send(messages::LogMessage { value }) {
                        Ok(_) => {},
                        Err(message) => {
                            error!("Encountered error: {}", message);
                            args.full_usage();
                            info!("Exiting!");
                            break;
                        }
                    };
                },
                Err(_) => {
                    error!("Invalid log value: {}", buffer);
                    info!("Exiting!");
                    break;
                }
            }
        } else {
            break;
        }
    }

    info!("Initiating exit.");

    match loop_system_channel.send(system::SystemMessage::Close) {
        Ok(_) => {},
        Err(message) => {
            error!("Failed to send close-message to the loop-channel: {}", message);
        }
    };

    match loop_handle.join() {
        Ok(_) => {},
        Err(message) => {
            error!("Failed to join the loop-channel: {:?}", message);
        }
    };

    info!("Node exiting!");
}

fn main_loop(name: String, system_channel: mpsc::Receiver<system::SystemMessage>, 
    inbound_channel: mpsc::Receiver<messages::DataMessage>, 
    outbound_channel: mpsc::Sender<messages::DataMessage>, 
    log_channel: mpsc::Receiver<messages::LogMessage>,
    config: &config::TimeoutConfig, 
    peers: Vec<String>) {

    let mut context = context::Context::new(name);

    let mut role = roles::Role::new_follower(config, &mut context);

    loop {
        if let Ok(_) = system_channel.try_recv() {
            info!("Main loop exiting!");
            break;
        }

        debug!("X: {:?}", context.persistent_state.log);

        let (new_role, outbound_messages) = if let Ok(message) = log_channel.try_recv() {
            engine::log_handling::role_receive_log(role, message, &mut context)
        } else {
            if let Ok(message) = inbound_channel.try_recv() {
                engine::message_handling::message_role(role, &message.raft_message, &message.peer, config, &mut context)
            } else {
                engine::tick_handling::tick_role(role, &peers, config, &mut context)
            }
        };

        role = new_role;

        for outbound_message in outbound_messages {
            match outbound_channel.send(outbound_message) {
                Ok(_) => {},
                Err(message) => {
                    panic!("Failed to send message: {:?}", message);
                }
            }
        }

        thread::sleep(Duration::from_millis(1000));
    }
}
