use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::net::UdpSocket;
use std::str;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

enum SystemMessage {
    Close
}

enum RaftMessage {
    HeartBeat(i32)
}

impl Clone for RaftMessage {
    fn clone(&self) -> Self {
        match self {
            RaftMessage::HeartBeat(i) => {
                RaftMessage::HeartBeat(i.clone())
            }
        }
    }
}

impl fmt::Display for RaftMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RaftMessage::HeartBeat(i) => {
                write!(f, "HeartBeat {:?}", i)
            }
        }
    }
}

struct DataMessage {
    raft_message: RaftMessage,
    address: String
}

struct Config {
    peers: Vec<i32>
}

fn usage() {
    println!("Usage: rusty-raft '[NAME]' [OWN_PORT] [KNOWN_PORTS]");
    println!("     NAME is the name of this server.");
    println!("     OWN_PORT is the port of this server.");
    println!("     KNOWN_PORTS is a comma-separated list of other member ports.");
}

fn parse_port(port: &str) -> i32 {
    match port.trim().parse::<i32>() {
        Ok(p) => p,
        Err(_) => {
            println!("Invalid port: {}, must be a number.", port);
            usage();
            panic!("Aborting!");
        }
    }
}

fn read_config() -> Config {
    let contents = fs::read_to_string("config.dat").unwrap();

    let mut peers = Vec::new();
    for line in contents.lines() {
        let segments : Vec<&str> = line.split(":").collect();
        match segments[0] {
            "peers" => {
                for peer in segments[1].split(",") {
                    println!("{}", peer);
                    peers.push(parse_port(peer));
                }
            }
            _ => {
                panic!("Oh, no!");
            }
        }
    };

    Config { peers }
}

fn main() {
    let mut arguments = env::args();

    // Pop the command
    arguments.next();

    let name = match arguments.next() {
        Some(arg) => arg,
        None => {
            println!("Invalid name-parameter.");
            usage();
            panic!("Aborting");
        }
    };

    let port = match arguments.next() {
        Some(arg) => arg,
        None => {
            println!("Invalid own-port-parameter.");
            usage();
            panic!("Aborting!");
        }
    };
    let port = parse_port(&port[..]);

    let config = read_config();
    if config.peers.len() < 3 {
        println!("Invalid list of peer ports. Three are required.");
        usage();
        panic!("Aborting!");
    }

    println!("{} {} {:?}", name, port, config.peers);

    let (inbound_channel_entrance, inbound_channel_exit) = mpsc::channel();
    let (outbound_channel_entrance, outbound_channel_exit) = mpsc::channel();

    let (udp_system_channel, udp_system_channel_reader) = mpsc::channel();
    let (loop_system_channel, loop_system_channel_reader) = mpsc::channel();

    let upd_handle = thread::spawn(move || { udp_loop(udp_system_channel_reader, inbound_channel_entrance, outbound_channel_exit, port); });
    let loop_handle = thread::spawn(move || { 
        main_loop(loop_system_channel_reader, inbound_channel_exit, outbound_channel_entrance, config.peers.iter().filter(|p| **p != port).collect()); 
    });

    let mut buffer = String::new();
    let stdin = io::stdin();
    stdin.read_line(&mut buffer).unwrap();

    println!("Initiating exit.");

    udp_system_channel.send(SystemMessage::Close).unwrap();
    loop_system_channel.send(SystemMessage::Close).unwrap();

    upd_handle.join().unwrap();
    loop_handle.join().unwrap();

    println!("Node exiting!");
}

fn udp_loop(system_channel: mpsc::Receiver<SystemMessage>, inbound_channel: mpsc::Sender<DataMessage>, outbound_channel: mpsc::Receiver<DataMessage>, port: i32) {
    let address = format!("127.0.0.1:{}", port);

    let mut socket = UdpSocket::bind(&address);
    if let Ok(s) = &mut socket {
        s.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
    }

    loop {
        if let Ok(SystemMessage::Close) = system_channel.try_recv() {
            println!("UDP Connection exiting!");
            break;
        }

        match &socket {
            Result::Ok(sock) => {
                if let Ok(msg) = outbound_channel.try_recv() {
                    sock.send_to(serialize(&msg.raft_message).as_bytes(), msg.address).unwrap();
                }

                let mut buf = [0; 100];
                if let Result::Ok((number_of_bytes, source_address)) = sock.recv_from(&mut buf) {
                    if number_of_bytes > 0 {
                        inbound_channel.send(DataMessage { raft_message: parse(str::from_utf8(&buf).unwrap().trim_matches(char::from(0))), address: source_address.to_string() }).unwrap();
                    }
                }
            }
            Result::Err(msg) => {
                println!("Failed to bind socket: {}", msg);
                socket = UdpSocket::bind(&address);
                if let Ok(s) = &mut socket {
                    s.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
                }
            }
        }
    }
}

fn parse(message : &str) -> RaftMessage {
    RaftMessage::HeartBeat(String::from(message).trim_end().parse::<i32>().unwrap())
}

fn serialize(message : &RaftMessage) -> String {
    match message {
        RaftMessage::HeartBeat(i) => {
            format!("{}", i)
        }
    }
}

fn main_loop(system_channel: mpsc::Receiver<SystemMessage>, inbound_channel: mpsc::Receiver<DataMessage>, outbound_channel: mpsc::Sender<DataMessage>, peers: Vec<&i32>) {

    let peers : Vec<String> = peers.iter().map(|p| format!("127.0.0.1:{}", p)).collect();

    let mut i = 1;

    loop {
        if let Ok(_) = system_channel.try_recv() {
            println!("Main loop exiting!");
            break;
        }

        if let Ok(msg) = inbound_channel.try_recv() {
            println!("Main received: {} from {}", msg.raft_message, msg.address);
        }

        let msg = RaftMessage::HeartBeat(i);

        for peer in &peers {
            println!("Main sent: {} to {}", &msg, peer);

            outbound_channel.send(DataMessage { raft_message: msg.clone(), address: peer.clone() }).unwrap();
        }

        i = i + 1;

        thread::sleep(Duration::from_millis(1000));
    }
}