use std::collections::HashMap;
use std::io;
use std::sync::mpsc;

use log::*;

pub mod config;
mod context;
mod data;
mod engine;
mod external_communication;
mod messages;
mod raft_loop;
mod roles;
mod utils;

pub fn run(name: String, peers: HashMap<String, String>, local_address: String, bad_connection: bool, config: config::TimeoutConfig) {

    info!("Starting {}", name);

    let (inbound_channel_entrance, inbound_channel_exit) = mpsc::channel();
    let (outbound_channel_entrance, outbound_channel_exit) = mpsc::channel();
    let (log_channel, log_channel_reader) = mpsc::channel();

    let peer_names : Vec<String> = peers.iter().map(|(peer_name, _)| peer_name.clone()).collect();

    let _external_connection = external_communication::ExternalConnection::new(inbound_channel_entrance, outbound_channel_exit, local_address, peers, bad_connection);
    let _raft_loop = raft_loop::RaftLoop::new(name, inbound_channel_exit, outbound_channel_entrance, log_channel_reader, config, peer_names);

    let stdin = io::stdin();
    loop {
        let mut buffer = String::new();
        match stdin.read_line(&mut buffer) {
            Ok(_) => {},
            Err(message) =>  {
                error!("Encountered error: {}", message);
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

    info!("Node exiting!");
}
