use rand::Rng;
use std::collections::HashMap;
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
    candidate_resend_timeout: u64,

    election_timeout_length: u64,
    idle_timeout_length: u64,

    peers: HashMap<String, String>
}

struct TimeoutConfig {
    candidate_resend_timeout: u64,

    election_timeout_length: u64,
    idle_timeout_length: u64
}

impl ::std::default::Default for Config {
    fn default() -> Self { 
        Self { 
            candidate_resend_timeout: 2000, 
            election_timeout_length: 10000, 
            idle_timeout_length: 1000,
            peers: HashMap::new()
        }
    }
}

enum SystemMessage {
    Close
}

enum RaftMessage {
    AppendEntries(AppendEntriesData),
    AppendEntriesResponse(AppendEntriesResponseData),
    RequestVote(RequestVoteData),
    RequestVoteResponse(RequestVoteResponseData)
}

#[derive(Clone)]
struct AppendEntriesData {
    term: u64,
}

struct AppendEntriesResponseData {
    term: u64,
}

struct RequestVoteData {
    term: u64
}

struct RequestVoteResponseData {
    term: u64,
    success: bool
}

impl Clone for RaftMessage {
    fn clone(&self) -> Self {
        match self {
            RaftMessage::AppendEntries(data) => {
                RaftMessage::AppendEntries(data.clone())
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
            RaftMessage::AppendEntries(data) => {
                write!(f, "AppendEntries {:?}", data.term)
            },
            _ => {
                panic!("Not implemented");
            }
        }
    }
}

struct Context {
    random: rand::rngs::ThreadRng,
    persistent_state: PersistentState
}

struct PersistentState {
    current_term: u64,
    voted_for: Option<String>
}

struct DataMessage {
    raft_message: RaftMessage,
    peer: String
}

fn usage() {
    println!("Usage: rusty-raft '[NAME]'");
    println!("     NAME is the name of this server.");
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

    let mut bad_connection = false;
    if let Some(arg) = arguments.next() {
        if arg == "--simulate-bad-connection" || arg == "-s" {
            bad_connection = true;
        }
    }

    let config : Config = confy::load("rusty_raft").unwrap();

    let timeout_config = TimeoutConfig {
        candidate_resend_timeout: config.candidate_resend_timeout,
        election_timeout_length: config.election_timeout_length,
        idle_timeout_length: config.idle_timeout_length
    };
    let mut peers = config.peers;

    if peers.len() < 3 {
        println!("Invalid list of peers. At least three are required.");
        usage();
        panic!("Aborting!");
    }

    let local_address = peers.remove(&name).unwrap();
    let peer_names : Vec<String> = peers.iter().map(|(peer_name, _)| peer_name.clone()).collect();

    println!("Starting {}", name);

    let (inbound_channel_entrance, inbound_channel_exit) = mpsc::channel();
    let (outbound_channel_entrance, outbound_channel_exit) = mpsc::channel();

    let (udp_system_channel, udp_system_channel_reader) = mpsc::channel();
    let (loop_system_channel, loop_system_channel_reader) = mpsc::channel();

    let upd_handle = thread::spawn(move || { udp_loop(udp_system_channel_reader, inbound_channel_entrance, outbound_channel_exit, local_address, peers, bad_connection); });
    let loop_handle = thread::spawn(move || { 
        main_loop(loop_system_channel_reader, inbound_channel_exit, outbound_channel_entrance, &timeout_config, peer_names); 
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

fn udp_loop(system_channel: mpsc::Receiver<SystemMessage>, 
    inbound_channel: mpsc::Sender<DataMessage>, 
    outbound_channel: mpsc::Receiver<DataMessage>, 
    local_address: String, 
    peers: HashMap<String, String>,
    bad_connection: bool) {

    let value_lookup = peers.iter().map(|(key, value)| (value, key)).collect::<HashMap<&String, &String>>();
    println!("{}", local_address);

    let mut socket = UdpSocket::bind(&local_address);
    if let Ok(s) = &mut socket {
        s.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
    }

    let mut random = rand::thread_rng();
    let bad_connection_chance = 0.025;
    let mut transfers_to_miss = 0;

    loop {
        if let Ok(SystemMessage::Close) = system_channel.try_recv() {
            println!("UDP Connection exiting!");
            break;
        }

        match &socket {
            Result::Ok(sock) => {
                if let Ok(msg) = outbound_channel.try_recv() {
                    if bad_connection && transfers_to_miss > 0 {
                        println!("Dropped outgoing package!");
                        transfers_to_miss -= 1;
                    } else if bad_connection && bad_connection_chance > random.gen_range(0.0..1.0) {
                        println!("Dropped outgoing package!");
                        transfers_to_miss = random.gen_range(1..20);
                    } else {
                        sock.send_to(serialize(&msg.raft_message).as_bytes(), peers.get(&msg.peer).unwrap()).unwrap();
                    }
                }

                let mut buf = [0; 4096];
                if let Result::Ok((number_of_bytes, source_address)) = sock.recv_from(&mut buf) {
                    if number_of_bytes > 0 {
                        if bad_connection && transfers_to_miss > 0 {
                            println!("Dropped incoming package!");
                            transfers_to_miss -= 1;
                        } else if bad_connection && bad_connection_chance > random.gen_range(0.0..1.0) {
                            println!("Dropped incoming package!");
                            transfers_to_miss = random.gen_range(1..20);
                        } else {
                            inbound_channel.send(DataMessage { 
                                raft_message: parse(str::from_utf8(&buf).unwrap().trim_matches(char::from(0))), 
                                peer: (*value_lookup.get(&source_address.to_string()).unwrap()).clone()
                            }).unwrap();
                        }
                    }
                }
            }
            Result::Err(msg) => {
                println!("Failed to bind socket: {}", msg);
                thread::sleep(Duration::from_millis(1000));
                socket = UdpSocket::bind(&local_address);
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
            if let Some(candidate) = map.get("request_vote") {
                RaftMessage::RequestVote(RequestVoteData {
                    term: parse_u64(candidate.get("term").unwrap())
                })
            } else if let Some(request_vote_response) = map.get("request_vote_response") {
                RaftMessage::RequestVoteResponse(RequestVoteResponseData { 
                    term: parse_u64(request_vote_response.get("term").unwrap()),
                    success: request_vote_response.get("success").unwrap() == "true"
                })
            } else if let Some(append_entries) = map.get("append_entries") {
                RaftMessage::AppendEntries(AppendEntriesData {
                    term: parse_u64(append_entries.get("term").unwrap())
                })
            } else if let Some(append_entries_response) = map.get("append_entries_response") {
                RaftMessage::AppendEntriesResponse(AppendEntriesResponseData {
                    term: parse_u64(append_entries_response.get("term").unwrap())
                })
            } else {
                panic!("Not handled {}", message);
            }
        },
        _ => {
            panic!("Invalid json!");
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
        RaftMessage::AppendEntries(data) => {
            json!({
                "append_entries": json!({
                    "term": data.term
                })
            }).to_string()
        },
        RaftMessage::AppendEntriesResponse(data) => {
            json!({
                "append_entries_response": json!({
                    "term": data.term
                })
            }).to_string()
        },
        RaftMessage::RequestVote(data) => {
            json!({
                "request_vote": json!({
                    "term": data.term
                })
            }).to_string()
        },
        RaftMessage::RequestVoteResponse(data) => {
            json!({
                "request_vote_response": json!({
                    "term": data.term,
                    "success": if data.success { "true" } else { "false" }
                })
            }).to_string()
        }
    }
}

struct FollowerData {
    election_timeout: Instant,
    election_timeout_length: u128,
}

struct CandidateData {
    election_timeout: Instant,
    election_timeout_length: u128,
    last_send_time: Option<Instant>,
    resend_timeout_length: u128,
    peers_approving: Vec<String>,
    peers_undecided: Vec<String>
}

struct LeaderData {
    idle_timeout: Option<Instant>,
    idle_timeout_length: u128
}

enum Role {
    Follower(FollowerData),
    Candidate(CandidateData),
    Leader(LeaderData)
}

fn tick_follower(follower: FollowerData, peers: &Vec<String>, inbound_channel: &mpsc::Receiver<DataMessage>, outbound_channel: &mpsc::Sender<DataMessage>, config: &TimeoutConfig, context: &mut Context) -> Role {

    if let Ok(msg) = inbound_channel.try_recv() {
        match msg.raft_message {
            RaftMessage::AppendEntries(data) => {
                println!("F {}: Received append entries, term: {}", context.persistent_state.current_term, data.term);
                context.persistent_state.current_term = data.term;
                send_append_entries_response(&context.persistent_state.current_term, &msg.peer, outbound_channel);
                return Role::Follower(FollowerData{
                    election_timeout: Instant::now(),
                    election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random)
                })
            },
            RaftMessage::AppendEntriesResponse(_) => {
                // Old or misguided message, ignore
            },
            RaftMessage::RequestVote(data) => {
                return handle_request_vote(&data, &msg.peer, Role::Follower(follower), outbound_channel, config, context)
            },
            RaftMessage::RequestVoteResponse(_) => {
                // Old or misguided message, ignore
            }
        }
    }

    if follower.election_timeout.elapsed().as_millis() > follower.election_timeout_length {
        println!("F {}: Timeout!", context.persistent_state.current_term);
        context.persistent_state.current_term += 1;
        Role::Candidate(CandidateData{ 
            election_timeout: Instant::now(), 
            election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random), 
            last_send_time: None, 
            resend_timeout_length: u128::from(config.candidate_resend_timeout),
            peers_approving: Vec::new(),
            peers_undecided: peers.iter().map(|peer| peer.clone()).collect::<Vec<String>>()
        })
    } else {
        println!("F {}: Timeout in {}", context.persistent_state.current_term, follower.election_timeout_length - follower.election_timeout.elapsed().as_millis());
        Role::Follower(follower)
    }
}

fn tick_candidate(mut candidate: CandidateData, peers: &Vec<String>, inbound_channel: &mpsc::Receiver<DataMessage>, outbound_channel: &mpsc::Sender<DataMessage>, config: &TimeoutConfig, context: &mut Context) -> Role {
    if let Ok(message) = inbound_channel.try_recv() {
        match &message.raft_message {
            RaftMessage::RequestVote(data) => {
                return handle_request_vote(&data, &message.peer, Role::Candidate(candidate), outbound_channel, config, context)
            },
            RaftMessage::RequestVoteResponse(data) => {
                candidate.peers_undecided.retain(|peer| *peer != message.peer);
                if data.success {
                    candidate.peers_approving.push(message.peer);
                }
            },
            RaftMessage::AppendEntries(data) => {
                println!("C {}: Received append entries from new leader, term: {}", context.persistent_state.current_term, data.term);
                context.persistent_state.current_term = data.term;
                send_append_entries_response(&context.persistent_state.current_term, &message.peer, outbound_channel);
                return Role::Follower(FollowerData{
                    election_timeout: Instant::now(),
                    election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random)
                })
            },
            RaftMessage::AppendEntriesResponse(_) => {
                panic!("Not implemented");
            }
        }
    }

    if candidate.peers_approving.len() >= peers.len() / 2 {
        println!("C {}: Elected!", context.persistent_state.current_term);
        return Role::Leader(LeaderData{
            idle_timeout: None,
            idle_timeout_length: u128::from(config.idle_timeout_length)
        })
    }

    if candidate.last_send_time.is_none() || candidate.election_timeout.elapsed().as_millis() > candidate.election_timeout_length {
        println!("C {}: Broadcast candidacy", context.persistent_state.current_term);
        candidate.peers_approving = Vec::new();
        candidate.peers_undecided = peers.iter().map(|peer| peer.clone()).collect::<Vec<String>>();
        candidate.election_timeout = Instant::now();
        candidate.election_timeout_length = randomize_timeout(&config.election_timeout_length, &mut context.random);
        candidate.last_send_time = Some(Instant::now());
        broadcast_request_vote(&context.persistent_state.current_term, &candidate.peers_undecided, outbound_channel);
        return Role::Candidate(candidate)
    }

    if candidate.last_send_time.unwrap().elapsed().as_millis() > candidate.resend_timeout_length {
        println!("C {}: Rebroadcast candidacy", context.persistent_state.current_term);
        rebroadcast_request_vote(&context.persistent_state.current_term, &candidate.peers_undecided, outbound_channel);
        candidate.last_send_time = Some(Instant::now());
    }

    Role::Candidate(candidate)
}

fn tick_leader(mut leader: LeaderData, peers: &Vec<String>, inbound_channel: &mpsc::Receiver<DataMessage>, outbound_channel: &mpsc::Sender<DataMessage>, config: &TimeoutConfig, context: &mut Context) -> Role {

    if let Ok(message) = inbound_channel.try_recv() {
        match message.raft_message {
            RaftMessage::AppendEntries(data) => {
                println!("L {}: append entries from {} with term {}", context.persistent_state.current_term, message.peer, data.term);
                context.persistent_state.current_term = data.term;
                return become_follower(config, context)
            },
            RaftMessage::AppendEntriesResponse(data) => {
                println!("L {}: append entries response: {} from {}", context.persistent_state.current_term, data.term, message.peer)
            },
            RaftMessage::RequestVote(data) => {
                println!("L {}: RequestVote from {} with term {}", context.persistent_state.current_term, message.peer, data.term);
                return handle_request_vote(&data, &message.peer, Role::Leader(leader), outbound_channel, config, context)
            },
            RaftMessage::RequestVoteResponse(_) => {
                // Ignore for now
            }
        }
    }

    if leader.idle_timeout.is_none() || leader.idle_timeout.unwrap().elapsed().as_millis() > leader.idle_timeout_length {
        println!("L {}: Broadcast AppendEntries Idle", context.persistent_state.current_term);
        broadcast_append_entries_idle(&context.persistent_state.current_term, peers, outbound_channel);
        leader.idle_timeout = Some(Instant::now());
        return Role::Leader(leader)
    }

    Role::Leader(leader)
}

fn handle_request_vote(data: &RequestVoteData, peer: &String, old_role: Role, outbound_channel: &mpsc::Sender<DataMessage>, config: &TimeoutConfig, context: &mut Context) -> Role {
    if data.term <= context.persistent_state.current_term {
        println!("F {}: Reject candidacy of {} with term: {}", context.persistent_state.current_term, peer, data.term);
        send_request_vote_response(&context.persistent_state.current_term, false, &peer, outbound_channel);
        old_role
    } else {
        println!("F {}: Accept candidacy of {} with term: {}", context.persistent_state.current_term, peer, data.term);
        context.persistent_state.current_term = data.term;
        send_request_vote_response(&context.persistent_state.current_term, true, &peer, outbound_channel);
        become_follower(config, context)
    }
}

fn become_follower(config: &TimeoutConfig, context: &mut Context) -> Role {
    return Role::Follower(FollowerData{
        election_timeout: Instant::now(),
        election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random)
    })
}

fn broadcast_request_vote(term: &u64, peers: &Vec<String>, outbound_channel: &mpsc::Sender<DataMessage>) {
    for peer in peers.iter() {
        send_request_vote(term, peer, outbound_channel);
    }
}

fn rebroadcast_request_vote(term: &u64, peers: &Vec<String>, outbound_channel: &mpsc::Sender<DataMessage>) {
    for peer in peers.iter() {
        send_request_vote(term, peer, outbound_channel);
    }
}

fn broadcast_append_entries_idle(term: &u64, followers: &Vec<String>, outbound_channel: &mpsc::Sender<DataMessage>) {
    for follower in followers.iter() {
        send_append_entries_idle(term, follower, outbound_channel);
    }
}

fn send_request_vote(term: &u64, peer: &String, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { 
        raft_message: RaftMessage::RequestVote(RequestVoteData{ 
            term: *term
        }), 
        peer: (*peer).clone()
    }).unwrap();
}

fn send_request_vote_response(term: &u64, success: bool, candidate: &String, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { 
        raft_message: RaftMessage::RequestVoteResponse(RequestVoteResponseData { 
            term: *term,
            success: success
        }), 
        peer: (*candidate).clone()
    }).unwrap();
}

fn send_append_entries_idle(term: &u64, follower: &String, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { 
        raft_message: RaftMessage::AppendEntries(AppendEntriesData {
            term: *term
        }),
        peer: (*follower).clone()
    }).unwrap();
}

fn send_append_entries_response(term: &u64, leader: &String, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { 
        raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData {
            term: *term
        }), 
        peer: (*leader).clone()
    }).unwrap();
}

fn tick(role: Role, peers: &Vec<String>, inbound_channel: &mpsc::Receiver<DataMessage>, outbound_channel: &mpsc::Sender<DataMessage>, config: &TimeoutConfig, context: &mut Context) -> Role {
    match role {
        Role::Follower(data) => {
            tick_follower(data, peers, inbound_channel, outbound_channel, config, context)
        }
        Role::Candidate(data) => {
            tick_candidate(data, peers, inbound_channel, outbound_channel, config, context)
        }
        Role::Leader(data) => {
            tick_leader(data, peers, inbound_channel, outbound_channel, config, context)
        }
    }
}

fn randomize_timeout(base: &u64, random: &mut rand::rngs::ThreadRng) -> u128 {
    u128::from(base + random.gen_range(1..*base))
}

fn main_loop(system_channel: mpsc::Receiver<SystemMessage>, 
    inbound_channel: mpsc::Receiver<DataMessage>, 
    outbound_channel: mpsc::Sender<DataMessage>, 
    config: &TimeoutConfig, 
    peers: Vec<String>) {

    let mut context = Context { 
        random: rand::thread_rng(),
        persistent_state: PersistentState {
            current_term: 0,
            voted_for: None
        }
    };

    let election_timeout_length = randomize_timeout(&config.election_timeout_length, &mut context.random);

    let mut role = Role::Follower(FollowerData{ 
        election_timeout: Instant::now(), 
        election_timeout_length 
    });

    loop {
        if let Ok(_) = system_channel.try_recv() {
            println!("Main loop exiting!");
            break;
        }

        role = tick(role, &peers, &inbound_channel, &outbound_channel, config, &mut context);

        thread::sleep(Duration::from_millis(1000));
    }
}
