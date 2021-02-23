use rand::Rng;
use std::convert::TryFrom;
use std::env;
use std::fmt;
use std::io;
use std::net::UdpSocket;
use std::str;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
struct Config {
    peers: Vec<i32>,
    candidate_timeout: u64,
    candidate_resend_timeout: u64,
    heartbeat_timeout: u64,

    election_timeout_length: u64,
    idle_timeout_length: u64
}

impl ::std::default::Default for Config {
    fn default() -> Self { Self { peers: vec![], candidate_timeout: 10000, candidate_resend_timeout: 2000, heartbeat_timeout: 2000, election_timeout_length: 10000, idle_timeout_length: 1000 }}
}

enum SystemMessage {
    Close
}

enum RaftMessage {
    RequestVote(RequestVoteData),
    RequestVoteResponse(RequestVoteResponseData),
    HeartBeat(u64)
}

struct RequestVoteData {
    candidate: i32,
    term: u64
}

struct RequestVoteResponseData {
    success: bool,
    acceptor: i32
}

impl Clone for RaftMessage {
    fn clone(&self) -> Self {
        match self {
            RaftMessage::HeartBeat(i) => {
                RaftMessage::HeartBeat(i.clone())
            },
            _ => {
                panic!("Not implemented");
            }
        }
    }
}

impl fmt::Display for RaftMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RaftMessage::HeartBeat(i) => {
                write!(f, "HeartBeat {:?}", i)
            },
            _ => {
                panic!("Not implemented");
            }
        }
    }
}

struct Context {
    random: rand::rngs::ThreadRng,
    term: u64
}

struct DataMessage {
    raft_message: RaftMessage,
    address: String
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

    let mut config : Config = confy::load("rusty_raft").unwrap();
    if config.peers.len() < 3 {
        println!("Invalid list of peer ports. Three are required.");
        usage();
        panic!("Aborting!");
    }
    config.peers = config.peers.into_iter().filter(|&peer| peer != port).collect();

    println!("{} {} {:?}", name, port, config.peers);

    let (inbound_channel_entrance, inbound_channel_exit) = mpsc::channel();
    let (outbound_channel_entrance, outbound_channel_exit) = mpsc::channel();

    let (udp_system_channel, udp_system_channel_reader) = mpsc::channel();
    let (loop_system_channel, loop_system_channel_reader) = mpsc::channel();

    let max_timeout : u64 = 10;

    let upd_handle = thread::spawn(move || { udp_loop(udp_system_channel_reader, inbound_channel_entrance, outbound_channel_exit, port); });
    let loop_handle = thread::spawn(move || { 
        main_loop(loop_system_channel_reader, inbound_channel_exit, outbound_channel_entrance, &port, &config); 
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
    let msg = serde_json::from_str(message).unwrap();
    match msg {
        Value::Object(map) => {
            if let Some(candidate) = map.get("candidacy") {
                RaftMessage::RequestVote(RequestVoteData {
                    candidate: parse_i32(candidate.get("candidate").unwrap()), 
                    term: parse_u64(candidate.get("term").unwrap())
                })
            } else if let Some(accept) = map.get("request_vote_response") {
                RaftMessage::RequestVoteResponse(RequestVoteResponseData { 
                    success: accept.get("success").unwrap() == "true",
                    acceptor: parse_i32(accept.get("acceptor").unwrap()) 
                })
            } else if let Some(heartbeat) = map.get("heartbeat") {
                RaftMessage::HeartBeat(parse_u64(heartbeat.get("term").unwrap()))
            } else {
                panic!("Not handled {}", message);
            }
        },
        _ => {
            panic!("Invalid json!");
        }
    }
}

fn parse_i32(value: &serde_json::Value) -> i32 {
    match value {
        serde_json::Value::Number(i) => {
            i32::try_from(i.as_i64().unwrap()).unwrap()
        },
        _ => {
            panic!("Not a number");
        }
    }
}

fn parse_u64(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Number(i) => {
            i.as_u64().unwrap()
        },
        _ => {
            panic!("Not a number");
        }
    }
}

fn serialize(message : &RaftMessage) -> String {
    match message {
        RaftMessage::HeartBeat(i) => {
            json!({
                "heartbeat": json!({
                    "term": *i
                })
            }).to_string()
        },
        RaftMessage::RequestVote(data) => {
            json!({
                "candidacy": json!({
                    "candidate": data.candidate,
                    "term": data.term
                })
            }).to_string()
        },
        RaftMessage::RequestVoteResponse(data) => {
            json!({
                "request_vote_response": json!({
                    "success": if data.success { "true" } else { "false" },
                    "acceptor": data.acceptor
                })
            }).to_string()
        },
        _ => {
            panic!("Not implemented!");
        }
    }
}

struct FollowerData {
    host_port: i32,
    election_timeout: Instant,
    election_timeout_length: u128,
}

struct CandidateData {
    host_port: i32,
    election_timeout: Instant,
    election_timeout_length: u128,
    last_send_time: Option<Instant>,
    resend_timeout_length: u128,
    peers_approving: Vec<i32>,
    peers_undecided: Vec<i32>
}

struct LeaderData {
    host_port: i32,
    peers: Vec<i32>,
    idle_timeout: Option<Instant>,
    idle_timeout_length: u128
}

enum Role {
    Follower(FollowerData),
    Candidate(CandidateData),
    Leader(LeaderData)
}

fn tick_follower(follower: FollowerData, inbound_channel: &mpsc::Receiver<DataMessage>, outbound_channel: &mpsc::Sender<DataMessage>, config: &Config, context: &mut Context) -> Role {

    if let Ok(msg) = inbound_channel.try_recv() {
        match msg.raft_message {
            RaftMessage::RequestVote(data) => {
                println!("F {}: Accept candidacy of {} with term: {}", context.term, data.candidate, data.term);
                context.term = data.term;
                send_request_vote_response(&follower.host_port, &data.candidate, outbound_channel);
                return Role::Follower(FollowerData{
                    host_port: follower.host_port,
                    election_timeout: Instant::now(),
                    election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random)
                })
            },
            RaftMessage::HeartBeat(term) => {
                println!("F {}: Received heartbeat, term: {}", context.term, term);
                return Role::Follower(FollowerData{
                    host_port: follower.host_port,
                    election_timeout: Instant::now(),
                    election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random)
                })
            },
            _ => {
                panic!("Unknown message");
            }
        }
    }

    if follower.election_timeout.elapsed().as_millis() > follower.election_timeout_length {
        println!("F {}: Timeout!", context.term);
        context.term += 1;
        Role::Candidate(CandidateData{ 
            host_port: follower.host_port,
            election_timeout: Instant::now(), 
            election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random), 
            last_send_time: None, 
            resend_timeout_length: u128::from(config.candidate_resend_timeout),
            peers_approving: Vec::new(),
            peers_undecided: config.peers.clone()
        })
    } else {
        println!("F {}: Timeout in {}", context.term, follower.election_timeout_length - follower.election_timeout.elapsed().as_millis());
        Role::Follower(follower)
    }
}

fn tick_candidate(mut candidate: CandidateData, inbound_channel: &mpsc::Receiver<DataMessage>, outbound_channel: &mpsc::Sender<DataMessage>, config: &Config, context: &mut Context) -> Role {
    if let Ok(message) = inbound_channel.try_recv() {
        match message.raft_message {
            RaftMessage::RequestVote(_) => {
                panic!("Not implemented");
            },
            RaftMessage::RequestVoteResponse(data) => {
                candidate.peers_undecided.retain(|peer| *peer != data.acceptor);
                if data.success {
                    candidate.peers_approving.push(data.acceptor);
                }
            },
            RaftMessage::HeartBeat(_) => {
                panic!("Not implemented");
            }
        }
    }

    if candidate.peers_approving.len() >= config.peers.len() / 2 {
        println!("C {}: Elected!", context.term);
        return Role::Leader(LeaderData{
            host_port: candidate.host_port,
            peers: config.peers.clone(),
            idle_timeout: None,
            idle_timeout_length: u128::from(config.idle_timeout_length)
        })
    }

    if candidate.last_send_time.is_none() || candidate.election_timeout.elapsed().as_millis() > candidate.election_timeout_length {
        println!("C {}: Broadcast candidacy", context.term);
        broadcast_request_vote(&candidate, &context.term, outbound_channel);
        candidate.peers_approving = Vec::new();
        candidate.peers_undecided = config.peers.clone();
        candidate.election_timeout = Instant::now();
        candidate.election_timeout_length = randomize_timeout(&config.election_timeout_length, &mut context.random);
        candidate.last_send_time = Some(Instant::now());
        return Role::Candidate(candidate)
    }

    if candidate.last_send_time.unwrap().elapsed().as_millis() > candidate.resend_timeout_length {
        println!("C {}: Rebroadcast candidacy", context.term);
        rebroadcast_request_vote(&candidate, &context.term, outbound_channel);
        candidate.last_send_time = Some(Instant::now());
    }

    Role::Candidate(candidate)
}

fn tick_leader(mut leader: LeaderData, inbound_channel: &mpsc::Receiver<DataMessage>, outbound_channel: &mpsc::Sender<DataMessage>, config: &Config, context: &mut Context) -> Role {

    if leader.idle_timeout.is_none() || leader.idle_timeout.unwrap().elapsed().as_millis() > leader.idle_timeout_length {
        println!("L {}: Broadcast heartbeat", context.term);
        broadcast_heartbeat(&leader, &context.term, outbound_channel);
        leader.idle_timeout = Some(Instant::now());
        return Role::Leader(leader)
    }

    Role::Leader(leader)
}

fn broadcast_request_vote(candidate: &CandidateData, term: &u64, outbound_channel: &mpsc::Sender<DataMessage>) {
    for peer in candidate.peers_undecided.iter() {
        send_request_vote(&candidate.host_port, term, peer, outbound_channel);
    }
}

fn rebroadcast_request_vote(candidate: &CandidateData, term: &u64, outbound_channel: &mpsc::Sender<DataMessage>) {
    for peer in candidate.peers_undecided.iter() {
        send_request_vote(&candidate.host_port, term, peer, outbound_channel);
    }
}

fn broadcast_heartbeat(leader: &LeaderData, term: &u64, outbound_channel: &mpsc::Sender<DataMessage>) {
    for peer in leader.peers.iter() {
        send_heartbeat(term, peer, outbound_channel);
    }
}

fn send_request_vote(node: &i32, term: &u64, peer: &i32, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { raft_message: RaftMessage::RequestVote(RequestVoteData{ candidate: *node, term: *term }), address: format!("127.0.0.1:{}", peer) }).unwrap();
}

fn send_request_vote_response(node: &i32, candidate: &i32, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { raft_message: RaftMessage::RequestVoteResponse(RequestVoteResponseData { success: true, acceptor: *node }), address: format!("127.0.0.1:{}", candidate) }).unwrap();
}

fn send_heartbeat(term: &u64, peer: &i32, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { raft_message: RaftMessage::HeartBeat(*term), address: format!("127.0.0.1:{}", peer) }).unwrap();
}

fn tick(role: Role, inbound_channel: &mpsc::Receiver<DataMessage>, outbound_channel: &mpsc::Sender<DataMessage>, config: &Config, context: &mut Context) -> Role {
    match role {
        Role::Follower(data) => {
            tick_follower(data, inbound_channel, outbound_channel, config, context)
        }
        Role::Candidate(data) => {
            tick_candidate(data, inbound_channel, outbound_channel, config, context)
        }
        Role::Leader(data) => {
            tick_leader(data, inbound_channel, outbound_channel, config, context)
        }
    }
}

fn randomize_timeout(base: &u64, random: &mut rand::rngs::ThreadRng) -> u128 {
    u128::from(base + random.gen_range(1..*base))
}

fn main_loop(system_channel: mpsc::Receiver<SystemMessage>, inbound_channel: mpsc::Receiver<DataMessage>, outbound_channel: mpsc::Sender<DataMessage>, host_port: &i32, config: &Config) {

    let mut context = Context { 
        random: rand::thread_rng(),
        term: 0
    };

    let election_timeout_length = randomize_timeout(&config.election_timeout_length, &mut context.random);

    let mut role = Role::Follower(FollowerData{ host_port: *host_port, election_timeout: Instant::now(), election_timeout_length });

    loop {
        if let Ok(_) = system_channel.try_recv() {
            println!("Main loop exiting!");
            break;
        }

        role = tick(role, &inbound_channel, &outbound_channel, config, &mut context);

        thread::sleep(Duration::from_millis(1000));
    }
}
