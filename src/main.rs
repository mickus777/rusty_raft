use rand::Rng;
use std::collections::HashMap;
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
    election_timeout_length: u64,
    idle_timeout_length: u64,

    peers: HashMap<String, String>
}

struct TimeoutConfig {
    election_timeout_length: u64,
    idle_timeout_length: u64
}

impl ::std::default::Default for Config {
    fn default() -> Self { 
        Self { 
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
    prev_log_index: usize,
    prev_log_term: Option<u64>,
    entries: Vec<LogPost>,
    leader_commit: usize
}

struct AppendEntriesResponseData {
    term: u64,
    success: bool
}

struct RequestVoteData {
    term: u64,
    last_log_index: usize,
    last_log_term: Option<u64>
}

struct RequestVoteResponseData {
    term: u64,
    vote_granted: bool
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
    name: String,
    random: rand::rngs::ThreadRng,
    persistent_state: PersistentState,
    volatile_state: VolatileState
}

#[derive(Clone)]
struct LogPost {
    term: u64,
    value: i32
}

struct PersistentState {
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<LogPost>
}

struct VolatileState {
    commit_index: usize,
    last_applied: usize
}

struct DataMessage {
    raft_message: RaftMessage,
    peer: String
}

struct LogMessage {
    value: i32
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
    let (log_channel, log_channel_reader) = mpsc::channel();

    let (udp_system_channel, udp_system_channel_reader) = mpsc::channel();
    let (loop_system_channel, loop_system_channel_reader) = mpsc::channel();

    let upd_handle = thread::spawn(move || { udp_loop(udp_system_channel_reader, inbound_channel_entrance, outbound_channel_exit, local_address, peers, bad_connection); });
    let loop_handle = thread::spawn(move || { main_loop(name, loop_system_channel_reader, inbound_channel_exit, outbound_channel_entrance, log_channel_reader, &timeout_config, peer_names); });

    let stdin = io::stdin();
    loop {
        let mut buffer = String::new();
        stdin.read_line(&mut buffer).unwrap();
        if buffer.trim().len() > 0 {
            match buffer.trim().parse::<i32>() {
                Ok(value) => {
                    log_channel.send(LogMessage { value }).unwrap();
                },
                Err(_) => {
                    println!("Invalid log value: {}", buffer);
                    break;
                }
            }
        } else {
            break;
        }
    }

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
                    term: parse_u64(candidate.get("term").unwrap()),
                    last_log_index: parse_usize(candidate.get("last_log_index").unwrap()),
                    last_log_term: parse_option_u64(candidate.get("last_log_term").unwrap())
                })
            } else if let Some(request_vote_response) = map.get("request_vote_response") {
                RaftMessage::RequestVoteResponse(RequestVoteResponseData { 
                    term: parse_u64(request_vote_response.get("term").unwrap()),
                    vote_granted: request_vote_response.get("vote_granted").unwrap() == "true"
                })
            } else if let Some(append_entries) = map.get("append_entries") {
                RaftMessage::AppendEntries(AppendEntriesData {
                    term: parse_u64(append_entries.get("term").unwrap()),
                    prev_log_index: parse_usize(append_entries.get("prev_log_index").unwrap()),
                    prev_log_term: parse_option_u64(append_entries.get("prev_log_term").unwrap()),
                    entries: parse_entries(append_entries.get("entries").unwrap()),
                    leader_commit: parse_usize(append_entries.get("leader_commit").unwrap())
                })
            } else if let Some(append_entries_response) = map.get("append_entries_response") {
                RaftMessage::AppendEntriesResponse(AppendEntriesResponseData {
                    term: parse_u64(append_entries_response.get("term").unwrap()),
                    success: append_entries_response.get("success").unwrap() == "true"
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

fn parse_entry(value: &serde_json::Value) -> LogPost {
    match value {
        serde_json::Value::Object(map) => {
            LogPost {
                term: parse_u64(map.get("term").unwrap()),
                value: parse_i32(map.get("value").unwrap())
            }
        },
        _ => {
            panic!("Not a valid entry");
        }
    }
}

fn parse_entries(value: &serde_json::Value) -> Vec<LogPost> {
    match value {
        serde_json::Value::Array(array) => {
            array.iter().map(|value| parse_entry(value)).collect()
        },
        _ => {
            panic!("Not a valid entry list");
        }
    }
}

fn parse_option_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(_) => Some(parse_u64(value)),
        serde_json::Value::Null => None,
        _ => {
            panic!("Unknown value");
        }
    }
}

fn parse_u64(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Number(i) => {
            i.as_u64().unwrap()
        },
        _ => {
            panic!("Not a u64");
        }
    }
}

fn parse_i32(value: &serde_json::Value) -> i32 {
    match value {
        serde_json::Value::Number(i) => {
            i32::try_from(i.as_u64().unwrap()).unwrap()
        },
        _ => {
            panic!("Not an i32");
        }
    }
}

fn parse_usize(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Number(i) => {
            usize::try_from(i.as_u64().unwrap()).unwrap()
        },
        _ => {
            panic!("Not a usize");
        }
    }
}

fn serialize(message : &RaftMessage) -> String {
    match message {
        RaftMessage::AppendEntries(data) => {
            json!({
                "append_entries": json!({
                    "term": data.term,
                    "prev_log_index": data.prev_log_index,
                    "prev_log_term": match data.prev_log_term {
                        Some(term) => json!(term),
                        None => serde_json::Value::Null,
                    },
                    "entries": data.entries.iter().map(|entry| json!({ "term": entry.term, "value": entry.value })).collect::<serde_json::Value>(),
                    "leader_commit": data.leader_commit
                }) 
            }).to_string()
        },
        RaftMessage::AppendEntriesResponse(data) => {
            json!({
                "append_entries_response": json!({
                    "term": data.term,
                    "success": if data.success { "true" } else { "false" }
                })
            }).to_string()
        },
        RaftMessage::RequestVote(data) => {
            json!({ 
                "request_vote": json!({
                    "term": data.term,
                    "last_log_index": data.last_log_index,
                    "last_log_term": match data.last_log_term {
                        Some(term) => json!(term),
                        None => serde_json::Value::Null
                    }
                })
            }).to_string()
        },
        RaftMessage::RequestVoteResponse(data) => {
            json!({
                "request_vote_response": json!({
                    "term": data.term,
                    "vote_granted": if data.vote_granted { "true" } else { "false" }
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
    peers_approving: Vec<String>,
    peers_undecided: Vec<String>
}

struct LeaderData {
    idle_timeout: Instant,
    idle_timeout_length: u128
}

enum Role {
    Follower(FollowerData),
    Candidate(CandidateData),
    Leader(LeaderData)
}

fn tick_follower(follower: FollowerData, 
    peers: &Vec<String>, 
    inbound_channel: &mpsc::Receiver<DataMessage>, 
    outbound_channel: &mpsc::Sender<DataMessage>, 
    log_channel: &mpsc::Receiver<LogMessage>,
    config: &TimeoutConfig, 
    context: &mut Context) -> Role {

    if let Ok(message) = log_channel.try_recv() {
        println!("Follower can not handle message: {}", message.value)
    }

    if let Ok(msg) = inbound_channel.try_recv() {
        match msg.raft_message {
            RaftMessage::AppendEntries(data) => {
                println!("F {}: Received AppendEntries from {} term: {}", context.persistent_state.current_term, msg.peer, data.term);
                if data.term > context.persistent_state.current_term {
                    context.persistent_state.current_term = data.term;
                    context.persistent_state.voted_for = None;
                }
                send_append_entries_response(&context.persistent_state.current_term, &true, &msg.peer, outbound_channel);
                become_follower(config, context)
            },
            RaftMessage::AppendEntriesResponse(data) => {
                println!("F {}: Received AppendEntriesResponse from {} term: {}", context.persistent_state.current_term, msg.peer, data.term);
                // Old or misguided message, ignore
                Role::Follower(follower)
            },
            RaftMessage::RequestVote(data) => {
                println!("F {}: Received RequestVote from {} term: {}", context.persistent_state.current_term, msg.peer, data.term);
                handle_request_vote(&data, &msg.peer, Role::Follower(follower), outbound_channel, config, context)
            },
            RaftMessage::RequestVoteResponse(data) => {
                println!("F {}: Received RequestVoteResponse from {} term: {}", context.persistent_state.current_term, msg.peer, data.term);
                // Old or misguided message, ignore
                Role::Follower(follower)
            }
        }
    } else {
        if follower.election_timeout.elapsed().as_millis() > follower.election_timeout_length {
            println!("F {}: Timeout!", context.persistent_state.current_term);
            become_candidate(peers, outbound_channel, config, context)
        } else {
            println!("F {}: Timeout in {}", context.persistent_state.current_term, follower.election_timeout_length - follower.election_timeout.elapsed().as_millis());
            Role::Follower(follower)
        }
    }
}

fn tick_candidate(mut candidate: CandidateData, 
    peers: &Vec<String>, 
    inbound_channel: &mpsc::Receiver<DataMessage>, 
    outbound_channel: &mpsc::Sender<DataMessage>, 
    log_channel: &mpsc::Receiver<LogMessage>,
    config: &TimeoutConfig, 
    context: &mut Context) -> Role {

    if let Ok(message) = log_channel.try_recv() {
        println!("Candidate can not handle message: {}", message.value)
    }

    if let Ok(message) = inbound_channel.try_recv() {
        match &message.raft_message {
            RaftMessage::RequestVote(data) => {
                println!("C {}: RequestVote from {} with term {}", context.persistent_state.current_term, message.peer, data.term);
                handle_request_vote(&data, &message.peer, Role::Candidate(candidate), outbound_channel, config, context)
            },
            RaftMessage::RequestVoteResponse(data) => {
                println!("C {}: RequestVoteResponse from {} with term {}", context.persistent_state.current_term, message.peer, data.term);
                if data.term > context.persistent_state.current_term {
                    context.persistent_state.current_term = data.term;
                    context.persistent_state.voted_for = None;
                    become_follower(config, context)
                } else {
                    candidate.peers_undecided.retain(|peer| *peer != message.peer);
                    if data.vote_granted {
                        candidate.peers_approving.push(message.peer);
                    }
                    Role::Candidate(candidate)
                }
            },
            RaftMessage::AppendEntries(data) => {
                println!("C {}: Received append entries from {}, term: {}", context.persistent_state.current_term, message.peer, data.term);
                if data.term >= context.persistent_state.current_term {
                    context.persistent_state.current_term = data.term;
                    context.persistent_state.voted_for = None;
                    send_append_entries_response(&context.persistent_state.current_term, &true, &message.peer, outbound_channel);
                    become_follower(config, context)
                } else {
                    send_append_entries_response(&context.persistent_state.current_term, &false, &message.peer, outbound_channel);
                    Role::Candidate(candidate)
                }
            },
            RaftMessage::AppendEntriesResponse(data) => {
                println!("C {}: Received AppendEntriesResponse from {}, term: {}", context.persistent_state.current_term, message.peer, data.term);
                // Old or misguided message, ignore
                Role::Candidate(candidate)
            }
        }
    } else {
        if candidate.peers_approving.len() >= peers.len() / 2 {
            println!("C {}: Elected!", context.persistent_state.current_term);
            become_leader(peers, outbound_channel, config, context)
        } else if candidate.election_timeout.elapsed().as_millis() > candidate.election_timeout_length {
            println!("C {}: Timeout!", context.persistent_state.current_term);
            become_candidate(peers, outbound_channel, config, context)
        } else {
            println!("C {}: Timeout in {}", context.persistent_state.current_term, candidate.election_timeout_length - candidate.election_timeout.elapsed().as_millis());
            Role::Candidate(candidate)
        }
    }
}

fn tick_leader(mut leader: LeaderData, 
    peers: &Vec<String>, 
    inbound_channel: &mpsc::Receiver<DataMessage>, 
    outbound_channel: &mpsc::Sender<DataMessage>, 
    log_channel: &mpsc::Receiver<LogMessage>,
    config: &TimeoutConfig, 
    context: &mut Context) -> Role {

    if let Ok(message) = log_channel.try_recv() {
        println!("Leader can not handle message: {}", message.value);
        Role::Leader(leader)
    } else if let Ok(message) = inbound_channel.try_recv() {
        match message.raft_message {
            RaftMessage::AppendEntries(data) => {
                println!("L {}: AppendEntries from {} with term {}", context.persistent_state.current_term, message.peer, data.term);
                if data.term > context.persistent_state.current_term {
                    context.persistent_state.current_term = data.term;
                    context.persistent_state.voted_for = None;
                    send_append_entries_response(&context.persistent_state.current_term, &true, &message.peer, outbound_channel);
                    become_follower(config, context)
                } else {
                    send_append_entries_response(&context.persistent_state.current_term, &false, &message.peer, outbound_channel);
                    Role::Leader(leader)
                }
            },
            RaftMessage::AppendEntriesResponse(data) => {
                println!("L {}: AppendEntriesResponse from {} with term {}", context.persistent_state.current_term, message.peer, data.term);
                if data.term > context.persistent_state.current_term {
                    context.persistent_state.current_term = data.term;
                    context.persistent_state.voted_for = None;
                    become_follower(config, context)
                } else {
                    Role::Leader(leader)
                }
            },
            RaftMessage::RequestVote(data) => {
                println!("L {}: RequestVote from {} with term {}", context.persistent_state.current_term, message.peer, data.term);
                if data.term > context.persistent_state.current_term {
                    context.persistent_state.current_term = data.term;
                    context.persistent_state.voted_for = None;
                    become_follower(config, context)
                } else {
                    handle_request_vote(&data, &message.peer, Role::Leader(leader), outbound_channel, config, context)
                }
            },
            RaftMessage::RequestVoteResponse(data) => {
                println!("L {}: RequestVoteResponse from {} with term {}", context.persistent_state.current_term, message.peer, data.term);
                // Ignore for now
                Role::Leader(leader)
            }
        }
    } else {
        if leader.idle_timeout.elapsed().as_millis() > leader.idle_timeout_length {
            println!("L {}: Broadcast AppendEntries Idle", context.persistent_state.current_term);
            let prev_log_term = match top(&context.persistent_state.log) {
                Some(post) => Some(post.term),
                None => None
            };
            broadcast_append_entries(
                &context.persistent_state.current_term, 
                &context.persistent_state.log.len(), 
                &prev_log_term,
                &vec!(),
                &context.volatile_state.commit_index, 
                peers, 
                outbound_channel);
            leader.idle_timeout = Instant::now();
            Role::Leader(leader)
        } else {
            Role::Leader(leader)
        }
    }
}

fn handle_request_vote(data: &RequestVoteData, candidate: &String, old_role: Role, outbound_channel: &mpsc::Sender<DataMessage>, config: &TimeoutConfig, context: &mut Context) -> Role {
    if data.term < context.persistent_state.current_term {
        println!("X {}: Reject candidacy of {} with term: {}", context.persistent_state.current_term, candidate, data.term);
        send_request_vote_response(&context.persistent_state.current_term, false, &candidate, outbound_channel);
        return old_role
    } 
    
    if let None = &context.persistent_state.voted_for {
        println!("X {}: Accept new candidacy of {} with term: {}", context.persistent_state.current_term, candidate, data.term);
        context.persistent_state.current_term = data.term;
        context.persistent_state.voted_for = Some(candidate.clone());
        send_request_vote_response(&context.persistent_state.current_term, true, &candidate, outbound_channel);
        return become_follower(config, context)
    } else if let Some(voted_for) = &context.persistent_state.voted_for {
        if voted_for == candidate {
            println!("X {}: Accept old candidacy of {} with term: {}", context.persistent_state.current_term, candidate, data.term);
            context.persistent_state.current_term = data.term;
            context.persistent_state.voted_for = Some(candidate.clone());
            send_request_vote_response(&context.persistent_state.current_term, true, &candidate, outbound_channel);
            return become_follower(config, context)
        }        
    }

    println!("X {}: Reject candidacy of {} has already voted.", context.persistent_state.current_term, candidate);
    send_request_vote_response(&context.persistent_state.current_term, false, &candidate, outbound_channel);
    old_role
}

fn become_follower(config: &TimeoutConfig, context: &mut Context) -> Role {
    return Role::Follower(FollowerData{
        election_timeout: Instant::now(),
        election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random)
    })
}

fn become_candidate(peers: &Vec<String>, outbound_channel: &mpsc::Sender<DataMessage>, config: &TimeoutConfig, context: &mut Context) -> Role {
    context.persistent_state.current_term += 1;
    context.persistent_state.voted_for = Some(context.name.clone());
    let last_log_term = match top(&context.persistent_state.log) {
        Some(post) => Some(post.term),
        None => None
    };
    broadcast_request_vote(
        &context.persistent_state.current_term, 
        &context.persistent_state.log.len(), 
        &last_log_term,
        &peers, outbound_channel);
    Role::Candidate(CandidateData {
        election_timeout: Instant::now(), 
        election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random), 
        peers_approving: Vec::new(),
        peers_undecided: peers.iter().map(|peer| peer.clone()).collect::<Vec<String>>()
    })
}

fn top(list: &Vec<LogPost>) -> Option<&LogPost> {
    match list.len() {
        0 => None,
        n => Some(&list[n-1])
    }
}

fn become_leader(peers: &Vec<String>, outbound_channel: &mpsc::Sender<DataMessage>, config: &TimeoutConfig, context: &mut Context) -> Role {
    let prev_log_term = match top(&context.persistent_state.log) {
        Some(post) => Some(post.term),
        None => None
    };
    broadcast_append_entries(
        &context.persistent_state.current_term, 
        &context.persistent_state.log.len(),
        &prev_log_term,
        &vec!(),
        &context.volatile_state.commit_index,
        peers, 
        outbound_channel);
    Role::Leader(LeaderData {
        idle_timeout: Instant::now(),
        idle_timeout_length: u128::from(config.idle_timeout_length)
    })
}

fn broadcast_request_vote(term: &u64, last_log_index: &usize, last_log_term: &Option<u64>, peers: &Vec<String>, outbound_channel: &mpsc::Sender<DataMessage>) {
    for peer in peers.iter() {
        send_request_vote(term, last_log_index, last_log_term, peer, outbound_channel);
    }
}

fn broadcast_append_entries(term: &u64, prev_log_index: &usize, prev_log_term: &Option<u64>, entries: &Vec<LogPost>, leader_commit: &usize, followers: &Vec<String>, outbound_channel: &mpsc::Sender<DataMessage>) {
    for follower in followers.iter() {
        send_append_entries(term, prev_log_index, prev_log_term, entries, leader_commit, follower, outbound_channel);
    }
}

fn send_request_vote(term: &u64, last_log_index: &usize, last_log_term: &Option<u64>, peer: &String, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { 
        raft_message: RaftMessage::RequestVote(RequestVoteData{ 
            term: *term,
            last_log_index: *last_log_index,
            last_log_term: *last_log_term
        }), 
        peer: (*peer).clone()
    }).unwrap();
}

fn send_request_vote_response(term: &u64, vote_granted: bool, candidate: &String, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { 
        raft_message: RaftMessage::RequestVoteResponse(RequestVoteResponseData { 
            term: *term,
            vote_granted: vote_granted
        }), 
        peer: (*candidate).clone()
    }).unwrap();
}

fn send_append_entries(term: &u64, prev_log_index: &usize, prev_log_term: &Option<u64>, entries: &Vec<LogPost>, leader_commit: &usize, follower: &String, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { 
        raft_message: RaftMessage::AppendEntries(AppendEntriesData {
            term: *term,
            prev_log_index: *prev_log_index,
            prev_log_term: *prev_log_term,
            entries: entries.clone(),
            leader_commit: *leader_commit
        }),
        peer: (*follower).clone()
    }).unwrap();
}

fn send_append_entries_response(term: &u64, success: &bool, leader: &String, outbound_channel: &mpsc::Sender<DataMessage>) {
    outbound_channel.send(DataMessage { 
        raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData {
            term: *term,
            success: *success
        }), 
        peer: (*leader).clone()
    }).unwrap();
}

fn tick(role: Role, 
    peers: &Vec<String>, 
    inbound_channel: &mpsc::Receiver<DataMessage>, 
    outbound_channel: &mpsc::Sender<DataMessage>, 
    log_channel: &mpsc::Receiver<LogMessage>,
    config: &TimeoutConfig, 
    context: &mut Context) -> Role {
    match role {
        Role::Follower(data) => {
            tick_follower(data, peers, inbound_channel, outbound_channel, log_channel, config, context)
        }
        Role::Candidate(data) => {
            tick_candidate(data, peers, inbound_channel, outbound_channel, log_channel, config, context)
        }
        Role::Leader(data) => {
            tick_leader(data, peers, inbound_channel, outbound_channel, log_channel, config, context)
        }
    }
}

fn randomize_timeout(base: &u64, random: &mut rand::rngs::ThreadRng) -> u128 {
    u128::from(base + random.gen_range(1..*base))
}

fn main_loop(name: String, system_channel: mpsc::Receiver<SystemMessage>, 
    inbound_channel: mpsc::Receiver<DataMessage>, 
    outbound_channel: mpsc::Sender<DataMessage>, 
    log_channel: mpsc::Receiver<LogMessage>,
    config: &TimeoutConfig, 
    peers: Vec<String>) {

    let mut context = Context { 
        name: name,
        random: rand::thread_rng(),
        persistent_state: PersistentState {
            current_term: 0,
            voted_for: None,
            log: vec!()
        },
        volatile_state: VolatileState {
            commit_index: 0,
            last_applied: 0
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

        role = tick(role, &peers, &inbound_channel, &outbound_channel, &log_channel, config, &mut context);

        thread::sleep(Duration::from_millis(1000));
    }
}
