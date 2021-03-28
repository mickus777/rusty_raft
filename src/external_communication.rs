use std::collections::HashMap;
use std::net::UdpSocket;
use std::str;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use log::*;
use rand::Rng;

use crate::messages;

pub struct ExternalConnection {
    handle: Option<std::thread::JoinHandle<()>>,
    close_flag: Arc<Mutex<bool>>
}

impl ExternalConnection {
    pub fn new(inbound_channel: mpsc::Sender<messages::DataMessage>, 
        outbound_channel: mpsc::Receiver<messages::DataMessage>, 
        local_address: String, 
        peers: HashMap<String, String>,
        bad_connection: bool) -> ExternalConnection {
        
        let close_flag = Arc::new(Mutex::new(false));
        let close_flag_copy = close_flag.clone();

        ExternalConnection {
            handle: Some(thread::spawn(move || { udp_loop(close_flag_copy, inbound_channel, outbound_channel, local_address, peers, bad_connection); })),
            close_flag: close_flag
        }
    }
}

impl Drop for ExternalConnection {
    fn drop(&mut self) {

        match self.close_flag.lock() {
            Ok(mut guard) => {
                *guard = true;
            },
            Err(message) => {
                error!("Failed to set close-flag of the external connection channel: {}", message);
            }
        }

        match self.handle.take().unwrap().join() {
            Ok(_) => {},
            Err(message) => {
                error!("Failed to join the external communications channel: {:?}", message);
            }
        }
    }
}

fn udp_loop(close_flag: Arc<Mutex<bool>>,
    inbound_channel: mpsc::Sender<messages::DataMessage>, 
    outbound_channel: mpsc::Receiver<messages::DataMessage>, 
    local_address: String, 
    peers: HashMap<String, String>,
    bad_connection: bool) {

    let value_lookup = peers.iter().map(|(key, value)| (value, key)).collect::<HashMap<&String, &String>>();
    debug!("Local udp-address: {}", local_address);

    let mut socket = create_socket(&local_address);

    let mut random = rand::thread_rng();
    let bad_connection_chance = 0.025;
    let mut transfers_to_miss = 0;

    loop {
        if *close_flag.lock().unwrap() {
            info!("UDP Connection exiting!");
            break;
        }

        if let Ok(sock) = &socket {
            if let Ok(message) = outbound_channel.try_recv() {
                if bad_connection && transfers_to_miss > 0 {
                    debug!("Dropped outgoing package!");
                    transfers_to_miss -= 1;
                } else if bad_connection && bad_connection_chance > random.gen_range(0.0..1.0) {
                    debug!("Dropped outgoing package!");
                    transfers_to_miss = random.gen_range(1..20);
                } else {
                    if let Err(error) = send_outbound_message(message, &peers, &sock) {
                        error!("{}", error);
                        break;
                    }
                }
            }

            let mut buf = [0; 4096];
            if let Result::Ok((number_of_bytes, source_address)) = sock.recv_from(&mut buf) {
                if number_of_bytes > 0 {
                    if bad_connection && transfers_to_miss > 0 {
                        debug!("Dropped incoming package!");
                        transfers_to_miss -= 1;
                    } else if bad_connection && bad_connection_chance > random.gen_range(0.0..1.0) {
                        debug!("Dropped incoming package!");
                        transfers_to_miss = random.gen_range(1..20);
                    } else {
                        if let Err(error) = send_inbound_message(buf, &value_lookup, &source_address, &inbound_channel) {
                            error!("{}", error);
                            break;
                        }
                    }
                }
            }
        } else {
            thread::sleep(Duration::from_millis(1000));
            socket = create_socket(&local_address);
        }
    }
}

fn create_socket(address: &str) -> Result<std::net::UdpSocket, std::io::Error> {
    let mut socket = UdpSocket::bind(address);

    if let Ok(s) = &mut socket {
        match s.set_read_timeout(Some(Duration::from_millis(10))) {
            Ok(_) => {},
            Err(message) => {
                error!("Failed to set read timeout for socket: {}", message);
            }
        }
    }

    socket
}

fn send_outbound_message(message: messages::DataMessage, peers: &HashMap<String, String>, socket: &UdpSocket) -> Result<(), String> {
    let msg = match serde_json::to_string(&message.raft_message) {
        Ok(msg) => msg,
        Err(msg) => {
            return Result::Err(format!("Failed to parse message: {}", msg));
        }
    };
    let peer_address = match peers.get(&message.peer) {
        Some(address) => address,
        None => {
            return Result::Err(format!("Could not find address of peer: {}", message.peer));
        }
    };
    match socket.send_to(msg.as_bytes(), peer_address) {
        Ok(_) => {
            return Result::Ok(());
        },
        Err(m) => {
            return Result::Err(format!("Failed to send message to peer: {}", m));
        }
    };
}

fn send_inbound_message(buf: [u8; 4096], peers_by_address: &HashMap<&String, &String>, sender: &std::net::SocketAddr, inbound_channel: &mpsc::Sender<messages::DataMessage>) -> Result<(), String> {
    let message_text = match str::from_utf8(&buf) {
        Ok(text) => {
            text 
        },
        Err(message) => {
            return Result::Err(format!("Failed to parse incoming message: {}", message));
        }
    };

    let raft_message = match serde_json::from_str(message_text.trim_matches(char::from(0))) {
        Ok(message) => {
            message
        },
        Err(message) => {
            return Result::Err(format!("Failed to interpret incoming message: {}", message));
        }
    };
    
    let sender_address = sender.to_string();
    let peer = match peers_by_address.get(&sender_address) {
        Some(peer) =>  {
            peer
        },
        None => {
            return Result::Err(format!("Failed to find name of sender of incoming message: {}", sender_address));
        }
    };

    match inbound_channel.send(messages::DataMessage { raft_message: raft_message, peer: (*peer).clone() }) {
        Ok(_) => {
            Result::Ok(())
        },
        Err(message) => {
            Result::Err(format!("Failed to pass incoming message onto channel: {}", message))
        }
    }
}

