use rand::Rng;
use std::collections::HashMap;
use std::cmp;
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
            idle_timeout_length: 5000,
            peers: HashMap::new()
        }
    }
}

enum SystemMessage {
    Close
}

#[derive(Serialize, Deserialize, Debug)]
enum RaftMessage {
    AppendEntries(AppendEntriesData),
    AppendEntriesResponse(AppendEntriesResponseData),
    RequestVote(RequestVoteData),
    RequestVoteResponse(RequestVoteResponseData)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct AppendEntriesData {
    term: u64,
    prev_log_index: Option<usize>,
    prev_log_term: Option<u64>,
    entries: Vec<LogPost>,
    leader_commit: Option<usize>
}

#[derive(Serialize, Deserialize, Debug)]
struct AppendEntriesResponseData {
    term: u64,
    success: bool,
    last_log_index: Option<usize>
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestVoteData {
    term: u64,
    last_log_index: Option<usize>,
    last_log_term: Option<u64>
}

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Clone, Deserialize, Serialize)]
struct LogPost {
    term: u64,
    value: i32
}

impl cmp::PartialEq for LogPost {
    fn eq(&self, other: &Self) -> bool {
        self.term == other.term && self.value == other.value
    }
}

impl fmt::Debug for LogPost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("")
        .field(&self.term)
        .field(&self.value)
        .finish()
    }
}

struct PersistentState {
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<LogPost>
}

struct VolatileState {
    commit_index: Option<usize>,
    last_applied: Option<usize>
}

#[derive(Debug, Serialize, Deserialize)]
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

    let config : Config = match confy::load("rusty_raft") {
        Ok(config) => config,
        Err(_) => {
            println!("Could not find configuration file, without peers we get no further.");
            usage();
            panic!("Aborting!");
        }
    };

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

    let local_address = match peers.remove(&name) {
        Some(address) => address,
        None => {
            println!("Could not find the address of the node name.");
            usage();
            panic!("Aborting!");
        }
    };
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
        match stdin.read_line(&mut buffer) {
            Ok(_) => {},
            Err(message) =>  {
                println!("Encountered error: {}", message);
                usage();
                println!("Exiting!");
                break;
            }
        };
        if buffer.trim().len() > 0 {
            match buffer.trim().parse::<i32>() {
                Ok(value) => {
                    match log_channel.send(LogMessage { value }) {
                        Ok(_) => {},
                        Err(message) => {
                            println!("Encountered error: {}", message);
                            usage();
                            println!("Exiting!");
                            break;
                        }
                    };
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

    match udp_system_channel.send(SystemMessage::Close) {
        Ok(_) => {},
        Err(message) => {
            println!("Failed to send close-message to the udp-channel: {}", message);
        }
    };
    match loop_system_channel.send(SystemMessage::Close) {
        Ok(_) => {},
        Err(message) => {
            println!("Failed to send close-message to the loop-channel: {}", message);
        }
    };

    match upd_handle.join() {
        Ok(_) => {},
        Err(message) => {
            println!("Failed to join the udp-channel: {:?}", message);
        }
    };
    match loop_handle.join() {
        Ok(_) => {},
        Err(message) => {
            println!("Failed to join the loop-channel: {:?}", message);
        }
    };

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
        match s.set_read_timeout(Some(Duration::from_millis(10))) {
            Ok(_) => {},
            Err(message) => {
                println!("Failed to set read timeout from socket: {}.", message);
                return
            }
        };
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
                        let message = match serde_json::to_string(&msg.raft_message) {
                            Ok(message) => message,
                            Err(message) => {
                                println!("Failed to parse message: {}", message);
                                break
                            }
                        };
                        let peer_address = match peers.get(&msg.peer) {
                            Some(address) => address,
                            None => {
                                println!("Could not find address of peer: {}", msg.peer);
                                break
                            }
                        };
                        match sock.send_to(message.as_bytes(), peer_address) {
                            Ok(_) => {},
                            Err(message) => {
                                println!("Failed to send message to peer: {}", message);
                                break;
                            }
                        };
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
                            let message_text = match str::from_utf8(&buf) {
                                Ok(text) => text,
                                Err(message) => {
                                    println!("Failed to parse incoming message: {}", message);
                                    break;
                                }
                            };
                            let message_text = message_text.trim_matches(char::from(0));
                            let raft_message = match serde_json::from_str(message_text) {
                                Ok(message) => message,
                                Err(message) => {
                                    println!("Failed to interpret incoming message: {}", message);
                                    break;
                                }
                            };
                            let peer_address = source_address.to_string();
                            let peer = match value_lookup.get(&peer_address) {
                                Some(peer) => peer,
                                None => {
                                    println!("Failed to find name of sender of incoming message: {}", peer_address);
                                    break;
                                }
                            };
                            match inbound_channel.send(DataMessage { 
                                raft_message: raft_message,
                                peer: (*peer).clone()
                            }) {
                                Ok(_) => {},
                                Err(message) => {
                                    println!("Failed to pass incoming message onto channel: {}", message);
                                    break;
                                }
                            };
                        }
                    }
                }
            }
            Result::Err(msg) => {
                println!("Failed to bind socket: {}", msg);
                thread::sleep(Duration::from_millis(1000));
                socket = UdpSocket::bind(&local_address);
                if let Ok(s) = &mut socket {
                    match s.set_read_timeout(Some(Duration::from_millis(10))) {
                        Ok(_) => {},
                        Err(message) => {
                            println!("Failed to set read timeout from socket: {}.", message);
                            return
                        }
                    };
                }
            }
        }
    }
}

#[derive(Debug)]
struct FollowerData {
    election_timeout: Instant,
    election_timeout_length: u128,
}

#[derive(Debug)]
struct CandidateData {
    election_timeout: Instant,
    election_timeout_length: u128,
    peers_approving: Vec<String>,
    peers_undecided: Vec<String>
}

#[derive(Debug)]
struct LeaderData {
    idle_timeout: Instant,
    idle_timeout_length: u128,
    next_index: HashMap<String, usize>,
    match_index: HashMap<String, Option<usize>>
}

#[derive(Debug)]
enum Role {
    Follower(FollowerData),
    Candidate(CandidateData),
    Leader(LeaderData)
}

fn get_message_term(message: &RaftMessage) -> u64 {
    match message {
        RaftMessage::AppendEntries(data) => data.term,
        RaftMessage::AppendEntriesResponse(data) => data.term,
        RaftMessage::RequestVote(data) => data.term,
        RaftMessage::RequestVoteResponse(data) => data.term
    }
}

// Append entries to the log starting at start_pos, skipping duplicates and dropping all leftover entries in the log
fn append_entries_from(log: &mut Vec<LogPost>, entries: &Vec<LogPost>, start_pos: &Option<usize>) {
    if entries.len() == 0 {
        return
    } else if log.len() == 0 {
        log.extend(entries.iter().cloned());
        return
    }
    let mut pos_offset = 0;
    let start_pos : usize = start_pos.unwrap_or_default();
    loop {
        if let Some(new_entry) = entries.get(pos_offset) {
            if let Some(old_entry) = log.get(start_pos + pos_offset + 1) {
                if new_entry == old_entry {
                    pos_offset += 1
                } else {
                    log.drain((start_pos + pos_offset + 1)..);
                    log.extend(entries[pos_offset..].iter().cloned());
                    break
                }
            } else {
                log.extend(entries[pos_offset..].iter().cloned());
                break
            }
        } else {
            log.drain((start_pos + pos_offset + 1)..);
            break
        }
    }
}

fn handle_append_entries(role: Role, append_entries: &AppendEntriesData, config: &TimeoutConfig, context: &mut Context, peer: &String) -> (Role, Vec<DataMessage>) {
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Receiver rule 1:
    if append_entries.term < context.persistent_state.current_term {
        return (role, create_append_entries_response(&context.persistent_state.current_term, &false, &None, peer))
    }
    //////////////////////////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Receiver rule 2:
    if let Some(prev_index) = append_entries.prev_log_index {
        if let Some(prev_term) = append_entries.prev_log_term {
            if let Some(post) = context.persistent_state.log.get(prev_index) {
                if post.term != prev_term {
                    println!("Test 1");
                    // Log contains an entry at leader's previous index but its term is not the same as that of the leader
                    return (role, create_append_entries_response(&context.persistent_state.current_term, &false, &None, peer))
                }
            } else {
                println!("Test 2");
                // Log does not contain an entry at leader's previous index
                return (role, create_append_entries_response(&context.persistent_state.current_term, &false, &None, peer))
            }
        } else {
            panic!("If there is a previous index, there must also be a previous term.");
        }
    }
    //////////////////////////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Receiver rule 3 and 4:
    append_entries_from(&mut context.persistent_state.log, &append_entries.entries, &append_entries.prev_log_index);
    //////////////////////////////////////////////////////////////////////////////////////////////

    let mut last_log_index = None;
    if context.persistent_state.log.len() > 0 {
        last_log_index = Some(context.persistent_state.log.len() - 1);
    }
    let messages = create_append_entries_response(&context.persistent_state.current_term, &true, &last_log_index, peer);

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Receiver rule 5:
    if let Some(leader_commit) = append_entries.leader_commit {
        if let Some(commit_index) = context.volatile_state.commit_index {
            if leader_commit > commit_index {
                context.volatile_state.commit_index = Some(cmp::min(leader_commit, context.persistent_state.log.len() - 1));
            }
        } else {
            context.volatile_state.commit_index = Some(leader_commit);
        }
    } else {
        if append_entries.leader_commit.is_some() {
            panic!("It should not be possible to have a commit index higher than that of an elected leader");
        }
    }
    //////////////////////////////////////////////////////////////////////////////////////////////

    (become_follower(config, context), messages)
}

fn handle_request_vote(data: &RequestVoteData, candidate: &String, _config: &TimeoutConfig, context: &mut Context) -> bool {
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Receiver rule 1:
    if data.term < context.persistent_state.current_term {
        return false
    } 
    //////////////////////////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Receiver rule 2:
    if let Some(voted_for) = &context.persistent_state.voted_for {
        if voted_for != candidate {
            return false
        }
    }
    let result = check_last_log_post(&data.last_log_index, &data.last_log_term, &context.persistent_state.log);
    //////////////////////////////////////////////////////////////////////////////////////////////

    if result {
        context.persistent_state.voted_for = Some(candidate.clone());
        result
    } else {
        result
    }
}

fn tick_follower(follower: FollowerData, 
    peers: &Vec<String>, 
    config: &TimeoutConfig, 
    context: &mut Context) -> (Role, Vec<DataMessage>) {

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Rule: Followers 2
    if follower.election_timeout.elapsed().as_millis() > follower.election_timeout_length {
        println!("F {}: Timeout!", context.persistent_state.current_term);
        become_candidate(peers, config, context)
    //////////////////////////////////////////////////////////////////////////////////////////////
    } else {
        println!("F {}: Timeout in {}", context.persistent_state.current_term, follower.election_timeout_length - follower.election_timeout.elapsed().as_millis());
        (Role::Follower(follower), Vec::new())
    }
}

fn tick_candidate(candidate: CandidateData, 
    peers: &Vec<String>, 
    config: &TimeoutConfig, 
    context: &mut Context) -> (Role, Vec<DataMessage>) {

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Rule: Candidate 2
    if candidate.peers_approving.len() >= peers.len() / 2 {
        println!("C {}: Elected!", context.persistent_state.current_term);
        become_leader(peers, config, context)
    //////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Rule: Candidate 4
    } else if candidate.election_timeout.elapsed().as_millis() > candidate.election_timeout_length {
        println!("C {}: Timeout!", context.persistent_state.current_term);
        become_candidate(peers, config, context)
    //////////////////////////////////////////////////////////////////////////////////////////////
    } else {
        println!("C {}: Timeout in {}", context.persistent_state.current_term, candidate.election_timeout_length - candidate.election_timeout.elapsed().as_millis());
        (Role::Candidate(candidate), Vec::new())
    }
}

fn tick_leader(mut leader: LeaderData, 
    peers: &Vec<String>, 
    config: &TimeoutConfig, 
    context: &mut Context) -> (Role, Vec<DataMessage>) {

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Rule: Leader 1 part 2
    if leader.idle_timeout.elapsed().as_millis() > leader.idle_timeout_length {
        let prev_log_term = match top(&context.persistent_state.log) {
            Some(post) => Some(post.term),
            None => None
        };
        let prev_log_index = match context.persistent_state.log.len() {
            0 => None,
            _ => Some(context.persistent_state.log.len() - 1)
        };
        if prev_log_term.is_some() {
            println!("L {}: Broadcast AppendEntries Idle term: {}, index: {}", context.persistent_state.current_term, prev_log_term.unwrap(), prev_log_index.unwrap());
        } else {
            println!("L {}: Broadcast AppendEntries Idle", context.persistent_state.current_term);
        }
        let messages = create_append_entrieses(
            &context.persistent_state.current_term, 
            &prev_log_index, 
            &prev_log_term,
            &vec!(),
            &context.volatile_state.commit_index, 
            peers);
        let mut next_index = HashMap::new();
        let mut match_index : HashMap<String, Option<usize>> = HashMap::new();
        for peer in peers.iter() {
            if context.persistent_state.log.len() > 0 {
                next_index.insert((*peer).clone(), context.persistent_state.log.len() - 1);
            }
            match_index.insert((*peer).clone(), None);
        }
        (Role::Leader(LeaderData {
            idle_timeout: Instant::now(),
            idle_timeout_length: u128::from(config.idle_timeout_length),
            next_index: next_index,
            match_index: match_index
        }), messages)
    //////////////////////////////////////////////////////////////////////////////////////////////
    } else {
        let mut messages = Vec::new();
        //////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Leader 3 part 1
        for peer in peers.iter() {
            if let Some(next_index) = leader.next_index.get(peer) {
                if *next_index < context.persistent_state.log.len() {
                    let mut prev_log_index = None;
                    let mut prev_log_term = None;
                    if *next_index > 0 {
                        prev_log_index = Some(next_index - 1);
                        prev_log_term = match context.persistent_state.log.get(*next_index - 1) {
                            Some(post) => Some(post.term),
                            None => {
                                panic!("Failed to find log entry term of {}", *next_index - 1);
                            }
                        };
                    }
                    println!("L {} Send AppendEntries {:?} with previous index: {:?} and previous term: {:?}", context.persistent_state.current_term, get_log_range(&Some(*next_index), &context.persistent_state.log), prev_log_index, prev_log_term);
                    messages.push(create_append_entries(
                        &context.persistent_state.current_term, 
                        &prev_log_index, 
                        &prev_log_term, 
                        &get_log_range(&Some(*next_index), &context.persistent_state.log), 
                        &context.volatile_state.commit_index, 
                        peer))
                }
            } else {
                leader.next_index.insert(peer.clone(), context.persistent_state.log.len());
            }
        }
        //////////////////////////////////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Leader 4
        if let Some(commit_index) = context.volatile_state.commit_index {
            let mut max_possible_commit_index = context.persistent_state.log.len() - 1;
            while commit_index < max_possible_commit_index && context.persistent_state.log[max_possible_commit_index].term == context.persistent_state.current_term {
                let mut peers_with_match_index = 0;
                for peer in peers.iter() {
                    if let Some(index_option) = leader.match_index.get(peer) {
                        if let Some(index) = index_option {
                            if *index >= max_possible_commit_index {
                                peers_with_match_index += 1;
                            }
                        }
                    }
                }
                if peers_with_match_index >= peers.len() / 2 {
                    context.volatile_state.commit_index = Some(max_possible_commit_index);
                    break;
                }
                max_possible_commit_index -= 1;
            }
        } else if context.persistent_state.log.len() > 0 {
            let mut max_possible_commit_index = context.persistent_state.log.len() - 1;
            while context.persistent_state.log[max_possible_commit_index].term == context.persistent_state.current_term {
                let mut peers_with_match_index = 0;
                for peer in peers.iter() {
                    if let Some(index_option) = leader.match_index.get(peer) {
                        if let Some(index) = index_option {
                            if *index >= max_possible_commit_index {
                                peers_with_match_index += 1;
                            }
                        }
                    }
                }
                if peers_with_match_index >= peers.len() / 2 {
                    context.volatile_state.commit_index = Some(max_possible_commit_index);
                    break;
                }
                if max_possible_commit_index == 0 {
                    break
                } else {
                    max_possible_commit_index -= 1;
                }
            }
        }
        //////////////////////////////////////////////////////////////////////////////////////////

        (Role::Leader(leader), messages)
    }
}

fn get_log_range(from_index: &Option<usize>, log: &Vec<LogPost>) -> Vec<LogPost> {
    if let Some(start_index) = from_index {
        log[*start_index..].iter().cloned().collect()
    } else {
        log.iter().cloned().collect()
    }
}

fn check_last_log_post(last_log_index: &Option<usize>, last_log_term: &Option<u64>, log: &Vec<LogPost>) -> bool {
    if log.len() == 0 {
        last_log_index.is_none() && last_log_term.is_none()
    } else {
        match last_log_index {
            None => false,
            Some(index) => {
                match last_log_term {
                    None => false,
                    Some(term) => {
                        if log.len() - 1 > *index {
                            false
                        } else if log.len() - 1 < *index {
                            true
                        } else {
                            match log.get(*index) {
                                Some(post) => {
                                    post.term == *term
                                },
                                None => {
                                    panic!("We are sure there is a post at index");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn become_follower(config: &TimeoutConfig, context: &mut Context) -> Role {
    return Role::Follower(FollowerData{
        election_timeout: Instant::now(),
        election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random)
    })
}

fn become_candidate(peers: &Vec<String>, config: &TimeoutConfig, context: &mut Context) -> (Role, Vec<DataMessage>) {
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Rule: Candidate 1
    context.persistent_state.current_term += 1;
    context.persistent_state.voted_for = Some(context.name.clone());
    let last_log_term = match top(&context.persistent_state.log) {
        Some(post) => Some(post.term),
        None => None
    };
    let last_log_index = match context.persistent_state.log.len() {
        0 => None,
        _ => Some(context.persistent_state.log.len() - 1)
    };
    println!("X {}: Broadcast request votes to: {:?}", context.persistent_state.current_term, peers);
    (Role::Candidate(CandidateData {
        election_timeout: Instant::now(), 
        election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random), 
        peers_approving: Vec::new(),
        peers_undecided: peers.iter().map(|peer| peer.clone()).collect::<Vec<String>>()
    }), create_request_votes(
        &context.persistent_state.current_term, 
        &last_log_index, 
        &last_log_term,
        &peers))
    //////////////////////////////////////////////////////////////////////////////////////////////
}

fn top(list: &Vec<LogPost>) -> Option<&LogPost> {
    match list.len() {
        0 => None,
        n => Some(&list[n-1])
    }
}

fn become_leader(peers: &Vec<String>, config: &TimeoutConfig, context: &mut Context) -> (Role, Vec<DataMessage>) {
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Rule: Leader 1 part 1
    let prev_log_term = match top(&context.persistent_state.log) {
        Some(post) => Some(post.term),
        None => None
    };
    let prev_log_index = match context.persistent_state.log.len() {
        0 => None,
        _ => Some(context.persistent_state.log.len())
    };
    println!("X {}: Broadcast heartbeat to: {:?}", context.persistent_state.current_term, peers);
    let messages = create_append_entrieses(
        &context.persistent_state.current_term, 
        &prev_log_index,
        &prev_log_term,
        &vec!(),
        &context.volatile_state.commit_index,
        peers, 
        );
    //////////////////////////////////////////////////////////////////////////////////////////////
    let mut next_index = HashMap::new();
    let mut match_index : HashMap<String, Option<usize>> = HashMap::new();
    for peer in peers.iter() {
        if context.persistent_state.log.len() > 0 {
            next_index.insert((*peer).clone(), context.persistent_state.log.len() - 1);
        }
        match_index.insert((*peer).clone(), None);
    }
    (Role::Leader(LeaderData {
        idle_timeout: Instant::now(),
        idle_timeout_length: u128::from(config.idle_timeout_length),
        next_index: next_index,
        match_index: match_index
    }), messages)
}

fn create_request_votes(term: &u64, last_log_index: &Option<usize>, last_log_term: &Option<u64>, peers: &Vec<String>) -> Vec<DataMessage> {
    let mut messages = Vec::new();

    for peer in peers.iter() {
        messages.push(create_request_vote(term, last_log_index, last_log_term, peer));
    }

    messages
}

fn create_append_entrieses(term: &u64, prev_log_index: &Option<usize>, prev_log_term: &Option<u64>, entries: &Vec<LogPost>, leader_commit: &Option<usize>, followers: &Vec<String>) -> Vec<DataMessage> {
    let mut messages = Vec::new();

    for follower in followers.iter() {
        messages.push(create_append_entries(term, prev_log_index, prev_log_term, entries, leader_commit, follower));
    }

    messages
}

fn create_request_vote(term: &u64, last_log_index: &Option<usize>, last_log_term: &Option<u64>, peer: &String) -> DataMessage {
    DataMessage { 
        raft_message: RaftMessage::RequestVote(RequestVoteData{ 
            term: *term,
            last_log_index: *last_log_index,
            last_log_term: *last_log_term
        }), 
        peer: (*peer).clone()
    }
}

fn create_request_vote_response(term: &u64, vote_granted: bool, candidate: &String) -> DataMessage {
    DataMessage { 
        raft_message: RaftMessage::RequestVoteResponse(RequestVoteResponseData { 
            term: *term,
            vote_granted: vote_granted
        }), 
        peer: (*candidate).clone()
    }
}

fn create_append_entries(term: &u64, prev_log_index: &Option<usize>, prev_log_term: &Option<u64>, entries: &Vec<LogPost>, leader_commit: &Option<usize>, follower: &String) -> DataMessage {
    DataMessage { 
        raft_message: RaftMessage::AppendEntries(AppendEntriesData {
            term: *term,
            prev_log_index: *prev_log_index,
            prev_log_term: *prev_log_term,
            entries: entries.clone(),
            leader_commit: *leader_commit
        }),
        peer: (*follower).clone()
    }
}

fn create_append_entries_response(term: &u64, success: &bool, last_log_index: &Option<usize>, leader: &String) -> Vec<DataMessage> {
    vec!(DataMessage { 
        raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData {
            term: *term,
            success: *success,
            last_log_index: *last_log_index
        }), 
        peer: (*leader).clone()
    })
}

fn receive_log(role: Role, message: LogMessage, context: &mut Context) -> (Role, Vec<DataMessage>) {
    match role {
        Role::Follower(_) => {
            println!("Follower can not receive incoming data.");
            (role, Vec::new())
        },
        Role::Candidate(_) => {
            println!("Candidate can not receive incoming data.");
            (role, Vec::new())
        },
        Role::Leader(leader) => {
            //////////////////////////////////////////////////////////////////////////////////
            // Rule: Leader 2
            (leader_receive_log(leader, message.value, context), Vec::new())
            //////////////////////////////////////////////////////////////////////////////////
        }
    }
}

fn tick_role(role: Role, 
    peers: &Vec<String>, 
    config: &TimeoutConfig, 
    context: &mut Context) -> (Role, Vec<DataMessage>) {

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Rule: All servers 1
    if let Some(commit_index) = context.volatile_state.commit_index {
        if let Some(last_applied) = context.volatile_state.last_applied {
            if commit_index > last_applied {
                if let Some(post) = context.persistent_state.log.get(last_applied) {
                    context.volatile_state.last_applied = Some(last_applied + 1);
                    apply_log_post(post);
                } else {
                    panic!("We know that last applied must exist!");
                }
            }
        } else {
            if let Some(post) = context.persistent_state.log.get(0) {
                context.volatile_state.last_applied = Some(0);
                apply_log_post(post);
            } else {
                panic!("Since commit_index has a value there must be something in the log.");
            }
        }
    }
    //////////////////////////////////////////////////////////////////////////////////////////////

    match role {
        Role::Follower(data) => {
            tick_follower(data, peers, config, context)
        }
        Role::Candidate(data) => {
            tick_candidate(data, peers, config, context)
        }
        Role::Leader(data) => {
            tick_leader(data, peers, config, context)
        }
    }
}

fn message_role(role: Role, message: &RaftMessage, peer: &String, config: &TimeoutConfig, context: &mut Context) -> (Role, Vec<DataMessage>) {
    //////////////////////////////////////////////////////////////////////////////////////////////
    // Rule: All servers 2
    let term = get_message_term(&message);
    let mut should_become_follower = false;
    if term > context.persistent_state.current_term {
        context.persistent_state.current_term = term;
        context.persistent_state.voted_for = None;
        should_become_follower = true;
    }
    //////////////////////////////////////////////////////////////////////////////////////
    
    let (role, messages) = match role {
        Role::Follower(data) => {
            message_follower(data, message, peer, config, context)
        },
        Role::Candidate(data) => {
            message_candidate(data, message, peer, config, context)
        },
        Role::Leader(data) => {
            message_leader(data, message, peer, config, context)
        }
    };

    if should_become_follower {
        (become_follower(config, context), messages)
    } else {
        (role, messages)
    }
}

fn leader_receive_log(mut leader: LeaderData, value: i32, context: &mut Context) -> Role {
    context.persistent_state.log.push(LogPost { term: context.persistent_state.current_term, value });
    leader.idle_timeout = Instant::now();
    Role::Leader(leader)
}

fn message_follower(follower: FollowerData, raft_message: &RaftMessage, peer: &String, config: &TimeoutConfig, context: &mut Context) -> (Role, Vec<DataMessage>) {
    match raft_message {
        //////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Followers 1
        RaftMessage::AppendEntries(data) => {
            println!("F {}: Received AppendEntries from {} term: {}", context.persistent_state.current_term, peer, data.term);
            handle_append_entries(Role::Follower(follower), &data, config, context, &peer)
        },
        RaftMessage::RequestVote(data) => {
            println!("F {}: Received RequestVote from {} term: {}", context.persistent_state.current_term, peer, data.term);
            if handle_request_vote(&data, &peer, config, context) {
                (become_follower(config, context), vec!(create_request_vote_response(&context.persistent_state.current_term, true, &peer)))
            } else {
                (Role::Follower(follower), vec!(create_request_vote_response(&context.persistent_state.current_term, false, &peer)))
            }
        },
        //////////////////////////////////////////////////////////////////////////////////////////
        RaftMessage::AppendEntriesResponse(data) => {
            println!("F {}: Received AppendEntriesResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.success, data.term);
            // Old or misguided message, ignore
            (Role::Follower(follower), Vec::new())
        },
        RaftMessage::RequestVoteResponse(data) => {
            println!("F {}: Received RequestVoteResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.vote_granted, data.term);
            // Old or misguided message, ignore
            (Role::Follower(follower), Vec::new())
        }
    }
}

fn message_candidate(mut candidate: CandidateData, raft_message: &RaftMessage, peer: &String, config: &TimeoutConfig, context: &mut Context) -> (Role, Vec<DataMessage>) {
    match raft_message {
        RaftMessage::AppendEntries(data) => {
            println!("C {}: Received append entries from {}, term: {}", context.persistent_state.current_term, peer, data.term);
            //////////////////////////////////////////////////////////////////////////////////////
            // Rule: Candidate 3
            handle_append_entries(Role::Candidate(candidate), &data, config, context, &peer)
            //////////////////////////////////////////////////////////////////////////////////////
        },
        RaftMessage::AppendEntriesResponse(data) => {
            println!("C {}: Received AppendEntriesResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.success, data.term);
            // Old or misguided message, ignore
            (Role::Candidate(candidate), Vec::new())
        },
        RaftMessage::RequestVote(data) => {
            println!("C {}: RequestVote from {} with term {}", context.persistent_state.current_term, peer, data.term);
            let result = handle_request_vote(&data, &peer, config, context);
            (Role::Candidate(candidate), vec!(create_request_vote_response(&context.persistent_state.current_term, result, &peer)))
        },
        RaftMessage::RequestVoteResponse(data) => {
            println!("C {}: RequestVoteResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.vote_granted, data.term);
            if data.term > context.persistent_state.current_term {
                context.persistent_state.current_term = data.term;
                context.persistent_state.voted_for = None;
                (become_follower(config, context), Vec::new())
            } else {
                candidate.peers_undecided.retain(|undecided| *undecided != *peer);
                if data.vote_granted {
                    candidate.peers_approving.push(peer.clone());
                }
                (Role::Candidate(candidate), Vec::new())
            }
        }
    }
}

fn message_leader(mut leader: LeaderData, raft_message: &RaftMessage, peer: &String, config: &TimeoutConfig, context: &mut Context) -> (Role, Vec<DataMessage>) {
    match raft_message {
        RaftMessage::AppendEntries(data) => {
            println!("L {}: AppendEntries from {} with term {}", context.persistent_state.current_term, peer, data.term);
            handle_append_entries(Role::Leader(leader), &data, config, context, &peer)
        },
        RaftMessage::AppendEntriesResponse(data) => {
            println!("L {}: AppendEntriesResponse from {} with {} term {}", context.persistent_state.current_term, peer, data.success, data.term);
            if data.term > context.persistent_state.current_term {
                context.persistent_state.current_term = data.term;
                context.persistent_state.voted_for = None;
                (become_follower(config, context), Vec::new())
            } else {
                //////////////////////////////////////////////////////////////////////////////////
                // Rule: Leader 3 part 2
                let mut messages = Vec::new();
                if data.success {
                    if let Some(index) = data.last_log_index {
                        leader.next_index.insert(peer.clone(), index + 1);
                    }
                    leader.match_index.insert(peer.clone(), data.last_log_index);
                } else {
                    if let Some(next_index) = leader.next_index.get(peer) {
                        if *next_index > 0 {
                            let new_next_index = next_index - 1;
                            leader.next_index.insert(peer.clone(), new_next_index);
                            if new_next_index > 0 {
                                if let Some(post) = context.persistent_state.log.get(new_next_index - 1) {
                                    messages.push(create_append_entries(&context.persistent_state.current_term, &Some(new_next_index - 1), &Some(post.term), &context.persistent_state.log[new_next_index..].iter().cloned().collect(), &context.volatile_state.commit_index, peer));
                                } else {
                                    panic!("Since new_next_index is at least zero there must be something in the log.");
                                }
                            } else {
                                messages.push(create_append_entries(&context.persistent_state.current_term, &None, &None, &context.persistent_state.log, &context.volatile_state.commit_index, peer));
                            }
                        } else {
                            leader.next_index.remove(peer);
                            messages.push(create_append_entries(&context.persistent_state.current_term, &None, &None, &context.persistent_state.log, &context.volatile_state.commit_index, peer));
                        }
                    } else {
                        panic!("We should not fail here, there is no prior data");
                    }
                }
                //////////////////////////////////////////////////////////////////////////////////

                (Role::Leader(leader), messages)
            }
        },
        RaftMessage::RequestVote(data) => {
            println!("L {}: RequestVote from {} with term {}", context.persistent_state.current_term, peer, data.term);
            let result = handle_request_vote(&data, &peer, config, context);
            (Role::Leader(leader), vec!(create_request_vote_response(&context.persistent_state.current_term, result, &peer)))
        },
        RaftMessage::RequestVoteResponse(data) => {
            println!("L {}: RequestVoteResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.vote_granted, data.term);
            // Ignore for now
            (Role::Leader(leader), Vec::new())
        }
    }
}

fn apply_log_post(_log_post: &LogPost) {
    // Here we shall apply commmitted log posts to the state machine
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
            commit_index: None,
            last_applied: None
        }
    };

    let mut role = become_follower(config, &mut context);

    loop {
        if let Ok(_) = system_channel.try_recv() {
            println!("Main loop exiting!");
            break;
        }

        println!("X: {:?}", context.persistent_state.log);

        let (new_role, outbound_messages) = if let Ok(message) = log_channel.try_recv() {
            receive_log(role, message, &mut context)
        } else {
            if let Ok(message) = inbound_channel.try_recv() {
                message_role(role, &message.raft_message, &message.peer, config, &mut context)
            } else {
                tick_role(role, &peers, config, &mut context)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn when_append_entries_from_given_empty_log_then_append_all() {
        let mut a = Vec::new();
        let b = vec!(LogPost { term: 0, value: 0 });

        let expected = vec!(LogPost { term: 0, value: 0 });

        append_entries_from(&mut a, &b, &None);

        assert_eq!(a, expected);
    }

    #[test]
    fn when_append_entries_from_given_heartbeat_then_do_nothing() {
        let mut a = vec!(LogPost { term: 0, value: 0 });
        let b = Vec::new();

        let expected = vec!(LogPost { term: 0, value: 0 });

        append_entries_from(&mut a, &b, &None);

        assert_eq!(a, expected);
    }

    #[test]
    fn when_append_entries_from_given_new_log_post_then_append() {
        let mut a = vec!(LogPost { term: 0, value: 0 });
        let b = vec!(LogPost { term: 1, value: 2 });

        let expected = vec!(LogPost { term: 0, value: 0 }, LogPost { term: 1, value: 2 });

        append_entries_from(&mut a, &b, &Some(0));

        assert_eq!(a, expected);
    }

    #[test]
    fn when_append_entries_from_given_conflicting_posts_then_replace() {
        let mut a = vec!(LogPost { term: 0, value: 0}, LogPost { term: 0, value: 1 });
        let b = vec!(LogPost { term: 1, value: 2});

        let expected = vec!(LogPost { term: 0, value: 0}, LogPost { term: 1, value: 2 });

        append_entries_from(&mut a, &b, &Some(0));

        assert_eq!(a, expected);
    }

    #[test]
    fn when_append_entries_from_given_partly_overlapping_posts_then_append_new_posts() {
        let mut a = vec!(LogPost { term: 0, value: 0}, LogPost { term: 0, value: 1 });
        let b = vec!(LogPost { term: 0, value: 1}, LogPost { term: 1, value: 2 });

        let expected = vec!(LogPost { term: 0, value: 0}, LogPost { term: 0, value: 1}, LogPost { term: 1, value: 2 });

        append_entries_from(&mut a, &b, &Some(0));

        assert_eq!(a, expected);
    }

    #[test]
    fn when_append_entries_from_given_completely_overlapping_posts_then_do_nothing() {
        let mut a = vec!(LogPost { term: 0, value: 0}, LogPost { term: 0, value: 1 }, LogPost { term: 1, value: 2 });
        let b = vec!(LogPost { term: 0, value: 1}, LogPost { term: 1, value: 2 });

        let expected = vec!(LogPost { term: 0, value: 0}, LogPost { term: 0, value: 1}, LogPost { term: 1, value: 2 });

        append_entries_from(&mut a, &b, &Some(0));

        assert_eq!(a, expected);
    }

    #[test]
    fn when_append_entries_from_given_new_leader_heartbeat_then_do_nothing() {
        let mut a = vec!(LogPost { term: 0, value: 0}, LogPost { term: 0, value: 1 }, LogPost { term: 1, value: 2 });
        let b = Vec::new();

        let expected = vec!(LogPost { term: 0, value: 0}, LogPost { term: 0, value: 1}, LogPost { term: 1, value: 2 });

        append_entries_from(&mut a, &b, &None);

        assert_eq!(a, expected);
    }

    #[test]
    fn when_handle_append_entries_given_append_entries_with_old_term_then_signal_failure() {

        let mut context = create_context();

        let config = create_config();

        let mut append_entries = create_append_entries_data();
        append_entries.term = 1;

        let (role, messages) = handle_append_entries(create_follower(), &append_entries, &config, &mut context, &String::from("peer"));

        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. }), .. }));
        assert!(matches!(role, Role::Follower{..}));
    }

    #[test]
    fn when_handle_append_entries_given_append_entries_with_previous_log_conflict_then_signal_failure() {
        let mut context = create_context();
        context.persistent_state.log.push(LogPost { term: 4, value: 1 });

        let config = create_config();

        let mut append_entries = create_append_entries_data();
        append_entries.prev_log_index = Some(1);
        append_entries.prev_log_term = Some(5);

        let (role, messages) = handle_append_entries(create_follower(), &append_entries, &config, &mut context, &String::from("peer"));

        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. }), .. }));
        assert!(matches!(role, Role::Follower{..}));
    }

    #[test]
    fn when_handle_append_entries_given_append_entries_with_nonexisting_previous_log_then_signal_failure() {
        let mut context = create_context();

        let config = create_config();

        let mut append_entries = create_append_entries_data();
        append_entries.prev_log_index = Some(1);
        append_entries.prev_log_term = Some(5);

        let (role, messages) = handle_append_entries(create_follower(), &append_entries, &config, &mut context, &String::from("peer"));

        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. }), .. }));
        assert!(matches!(role, Role::Follower{..}));
    }

    #[test]
    fn when_handle_append_entries_given_append_entries_with_new_posts_then_append_posts() {
        let mut context = create_context();

        let config = create_config();

        let append_entries = create_append_entries_data();

        let (role, messages) = handle_append_entries(create_follower(), &append_entries, &config, &mut context, &String::from("peer"));

        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. }), .. }));
        assert!(matches!(role, Role::Follower{..}));
    }

    #[test]
    fn when_handle_append_entries_given_append_entries_with_conflicting_new_posts_then_replace_conflicting_posts() {
        let mut context = create_context();
        context.persistent_state.log.push(LogPost { term: 2, value: 13 });
        context.persistent_state.log.push(LogPost { term: 3, value: 17 });

        let config = create_config();

        let append_entries = create_append_entries_data();

        let (role, messages) = handle_append_entries(create_follower(), &append_entries, &config, &mut context, &String::from("peer"));

        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. }), .. }));
        assert!(matches!(role, Role::Follower{..}));
    }

    #[test]
    fn when_handle_append_entries_given_append_entries_with_new_leader_commit_then_set_commit_index() {
        let mut context = create_context();

        let config = create_config();

        let append_entries = create_append_entries_data();

        let (role, messages) = handle_append_entries(create_follower(), &append_entries, &config, &mut context, &String::from("peer"));

        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. }), .. }));
        assert!(matches!(role, Role::Follower{..}));
        assert_eq!(context.persistent_state.log.len(), 3);
        assert_eq!(context.volatile_state.commit_index.unwrap(), 2);
    }

    #[test]
    fn when_handle_append_entries_given_append_entries_with_old_leader_commit_then_dont_set_commit_index() {
        let mut context = create_context();

        let config = create_config();

        let mut append_entries = create_append_entries_data();
        append_entries.leader_commit = Some(0);

        let (role, messages) = handle_append_entries(create_follower(), &append_entries, &config, &mut context, &String::from("peer"));

        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. }), .. }));
        assert!(matches!(role, Role::Follower{..}));
        assert_eq!(context.persistent_state.log.len(), 3);
        assert_eq!(context.volatile_state.commit_index.unwrap(), 1);
    }

    #[test]
    fn when_handle_append_entries_given_append_entries_with_unknown_leader_commit_then_set_commit_index_to_new_last_entry() {
        let mut context = create_context();

        let config = create_config();

        let mut append_entries = create_append_entries_data();
        append_entries.leader_commit = Some(7);

        let (role, messages) = handle_append_entries(create_follower(), &append_entries, &config, &mut context, &String::from("peer"));

        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. }), .. }));
        assert!(matches!(role, Role::Follower{..}));
        assert_eq!(context.persistent_state.log.len(), 3);
        assert_eq!(context.volatile_state.commit_index.unwrap(), 2);
    }

    #[test]
    fn when_message_role_given_new_term_then_become_follower() {

        let mut context = create_context();

        let config = create_config();

        let role = create_candidate();

        let message = RaftMessage::RequestVote(create_request_vote_data());

        let result = message_role(role, &message, &String::from("peer 1"), &config, &mut context);

        assert_eq!(context.persistent_state.current_term, 6);
        assert!(matches!(result, (Role::Follower{ .. }, ..)));
    }

    #[test]
    fn when_message_follower_given_reject_request_vote_then_timeout_is_not_reset() {
        let mut context = create_context();

        let config = create_config();

        let mut follower = create_follower_data();
        follower.election_timeout = Instant::now() - Duration::new(500, 0);
        let elapsed = follower.election_timeout.elapsed().as_millis();

        let mut data = create_request_vote_data();
        data.term = 0;
        let message = RaftMessage::RequestVote(data);

        let result = message_follower(follower, &message, &String::from("leader"), &config, &mut context);

        match result {
            (Role::Follower(data), _) => {
                assert!(data.election_timeout.elapsed().as_millis() >= elapsed);
            },
            _ => {
                panic!();
            }
        }
    }
    
    #[test]
    fn when_append_entries_given_old_term_then_reply_false() {

        let mut context = create_context();

        let config = create_config();

        let message = RaftMessage::AppendEntries(AppendEntriesData{
            term: context.persistent_state.current_term - 1,
            entries: Vec::new(),
            leader_commit: Some(0),
            prev_log_index: Some(1),
            prev_log_term: Some(1)
        });

        let (role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
        let (role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
        let (role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Leader(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
    }

    #[test]
    fn when_append_entries_given_nonexisting_previous_index_then_reply_false() {
        let mut context = create_context();

        let config = create_config();

        let message = RaftMessage::AppendEntries(AppendEntriesData{
            term: context.persistent_state.current_term,
            entries: Vec::new(),
            leader_commit: Some(0),
            prev_log_index: Some(3),
            prev_log_term: Some(1)
        });

        let (role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
        let (role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
        let (role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Leader(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
    }

    #[test]
    fn when_append_entries_given_conflicting_previous_term_then_reply_false() {
        let mut context = create_context();

        let config = create_config();

        let message = RaftMessage::AppendEntries(AppendEntriesData{
            term: context.persistent_state.current_term,
            entries: Vec::new(),
            leader_commit: Some(0),
            prev_log_index: Some(1),
            prev_log_term: Some(2)
        });

        let (role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
        let (role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
        let (role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Leader(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: false, .. })));
    }

    #[test]
    fn when_append_entries_given_conflicting_new_terms_then_remove_old_and_append_new() {
        let config = create_config();

        let message = RaftMessage::AppendEntries(AppendEntriesData{
            term: 5,
            entries: vec!(LogPost { term: 2, value: 9 }, LogPost { term: 2, value: 7 }, LogPost { term: 3, value: 8 }),
            leader_commit: Some(0),
            prev_log_index: Some(0),
            prev_log_term: Some(1)
        });

        let expected = vec!(LogPost { term: 1, value: 0}, LogPost { term: 2, value: 9 }, LogPost { term: 2, value: 7 }, LogPost { term: 3, value: 8 });

        let mut context = create_context();
        context.persistent_state.log.push(LogPost { term: 4, value: 17 });
        let (_role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.persistent_state.log, expected);
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));

        let mut context = create_context();
        context.persistent_state.log.push(LogPost { term: 4, value: 17 });
        let (_role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.persistent_state.log, expected);
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));

        let mut context = create_context();
        context.persistent_state.log.push(LogPost { term: 4, value: 17 });
        let (_role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.persistent_state.log, expected);
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));
    }

    #[test]
    fn when_append_entries_given_new_leader_commit_less_than_log_len_then_set_commit_index_to_leader_commit() {
        let config = create_config();

        let message = RaftMessage::AppendEntries(AppendEntriesData{
            term: 5,
            entries: vec!(LogPost { term: 2, value: 9 }),
            leader_commit: Some(1),
            prev_log_index: Some(1),
            prev_log_term: Some(1)
        });

        let mut context = create_context();
        context.volatile_state.commit_index = Some(0);
        let (_role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.volatile_state.commit_index, Some(1));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));

        let mut context = create_context();
        context.volatile_state.commit_index = Some(0);
        let (_role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.volatile_state.commit_index, Some(1));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));

        let mut context = create_context();
        context.volatile_state.commit_index = Some(0);
        let (_role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.volatile_state.commit_index, Some(1));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));
    }

    #[test]
    fn when_append_entries_given_new_leader_commit_greater_than_log_len_then_set_commit_index_to_log_len() {
        let config = create_config();

        let message = RaftMessage::AppendEntries(AppendEntriesData{
            term: 5,
            entries: vec!(LogPost { term: 2, value: 9 }),
            leader_commit: Some(7),
            prev_log_index: Some(1),
            prev_log_term: Some(1)
        });

        let mut context = create_context();
        context.volatile_state.commit_index = Some(0);
        let (_role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.volatile_state.commit_index, Some(2));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));

        let mut context = create_context();
        context.volatile_state.commit_index = Some(0);
        let (_role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.volatile_state.commit_index, Some(2));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));

        let mut context = create_context();
        context.volatile_state.commit_index = Some(0);
        let (_role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert_eq!(context.volatile_state.commit_index, Some(2));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::AppendEntriesResponse(AppendEntriesResponseData{ success: true, .. })));
    }

    #[test]
    fn when_request_vote_given_old_term_then_do_not_grant_vote() {
        let mut context = create_context();

        let config = create_config();

        let message = RaftMessage::RequestVote(RequestVoteData{
            term: context.persistent_state.current_term - 1,
            last_log_index: Some(1),
            last_log_term: Some(1)
        });

        let (role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, term: 5 })));

        let (role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, term: 5 })));

        let (role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Leader(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, term: 5 })));
    }

    #[test]
    fn when_request_vote_given_already_voted_for_someone_else_this_term_then_do_not_grant_vote() {
        let mut context = create_context();
        context.persistent_state.voted_for = Some(String::from("someone"));

        let config = create_config();

        let message = RaftMessage::RequestVote(RequestVoteData{
            term: context.persistent_state.current_term,
            last_log_index: Some(1),
            last_log_term: Some(1)
        });

        let (role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, .. })));

        let (role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, .. })));

        let (role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Leader(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, .. })));
    }

    #[test]
    fn when_request_vote_given_candidates_log_is_old_then_do_not_grant_vote() {
        let mut context = create_context();

        let config = create_config();

        let message = RaftMessage::RequestVote(RequestVoteData{
            term: context.persistent_state.current_term,
            last_log_index: Some(0),
            last_log_term: Some(1)
        });

        let (role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, .. })));

        let (role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, .. })));

        let (role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Leader(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: false, .. })));
    }

    #[test]
    fn when_request_vote_given_new_term_then_grant_vote() {
        let mut context = create_context();

        let config = create_config();

        let message = RaftMessage::RequestVote(RequestVoteData{
            term: context.persistent_state.current_term + 1,
            last_log_index: Some(1),
            last_log_term: Some(1)
        });

        context.persistent_state.current_term = 5;
        let (role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: true, term: 6 })));

        context.persistent_state.current_term = 5;
        let (role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: true, term: 6 })));

        context.persistent_state.current_term = 5;
        let (role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: true, term: 6 })));
    }

    #[test]
    fn when_request_vote_given_rerequest_from_same_candidate_then_grant_vote() {
        let mut context = create_context();
        context.persistent_state.voted_for = Some(String::from("other"));

        let config = create_config();

        let message = RaftMessage::RequestVote(RequestVoteData{
            term: context.persistent_state.current_term,
            last_log_index: Some(1),
            last_log_term: Some(1)
        });

        let (role, messages) = message_role(Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: true, term: 5 })));

        let (role, messages) = message_role(Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: true, term: 5 })));

        let (role, messages) = message_role(Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
        assert!(matches!(role, Role::Leader(..)));
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages.get(0).unwrap().raft_message, RaftMessage::RequestVoteResponse(RequestVoteResponseData{ vote_granted: true, term: 5 })));
    }

    #[test]
    fn when_tick_role_given_commit_index_greater_than_last_applied_then_apply_one() {

        let mut context = create_context();
        context.volatile_state.commit_index = Some(3);

        let config = create_config();

        context.volatile_state.last_applied = Some(0);
        let (role, messages) = tick_role(Role::Follower(create_follower_data()), &create_peers(), &config, &mut context);
        assert!(matches!(role, Role::Follower(..)));
        assert_eq!(messages.len(), 0);
        assert_eq!(context.volatile_state.last_applied, Some(1));

        context.volatile_state.last_applied = Some(0);
        let (role, messages) = tick_role(Role::Candidate(create_candidate_data()), &create_peers(), &config, &mut context);
        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 0);
        assert_eq!(context.volatile_state.last_applied, Some(1));

        context.volatile_state.last_applied = Some(0);
        let (role, messages) = tick_role(Role::Leader(create_leader_data()), &create_peers(), &config, &mut context);
        assert!(matches!(role, Role::Leader(..)));
        assert_eq!(messages.len(), 0);
        assert_eq!(context.volatile_state.last_applied, Some(1));
    }
    
    #[test]
    fn when_message_role_given_new_term_then_accept_term_and_become_follower() {
        let mut context = create_context();

        let config = create_config();

        for message in vec!(create_append_entries(6), create_append_entries_response(6), create_request_vote(6), create_request_vote_response(6)) {
            for role in vec!(Role::Follower(create_follower_data()), Role::Candidate(create_candidate_data()), Role::Leader(create_leader_data())) {
                context.persistent_state.current_term = 5;
                let (new_role, _messages) = message_role(role, &message, &String::from("other"), &config, &mut context);
                assert!(matches!(new_role, Role::Follower(..)));
                assert_eq!(context.persistent_state.current_term, 6);
            }
        }
    }

    #[test]
    fn when_tick_follower_given_election_timer_timeout_then_become_candidate() {
        
        let mut context = create_context();

        let config = create_config();

        let mut follower = create_follower_data();
        follower.election_timeout = Instant::now() - Duration::new(500, 0);

        let (role, messages) = tick_follower(follower, &create_peers(), &config, &mut context);

        assert!(matches!(role, Role::Candidate(..)));
        assert_eq!(messages.len(), 2);
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::RequestVote(..), ..}));
    }

    #[test]
    fn when_message_follower_given_append_entries_then_election_timeout_is_reset() {
        let mut context = create_context();

        let config = create_config();

        let mut follower = create_follower_data();
        follower.election_timeout = Instant::now() - Duration::new(500, 0);
        let elapsed = follower.election_timeout.elapsed().as_millis();

        let message = RaftMessage::AppendEntries(create_append_entries_data());

        let result = message_follower(follower, &message, &String::from("leader"), &config, &mut context);

        match result {
            (Role::Follower(data), _) => {
                assert!(data.election_timeout.elapsed().as_millis() < elapsed);
            },
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn when_message_follower_given_request_vote_then_election_timeout_is_reset() {
        let mut context = create_context();

        let config = create_config();

        let mut follower = create_follower_data();
        follower.election_timeout = Instant::now() - Duration::new(500, 0);
        let elapsed = follower.election_timeout.elapsed().as_millis();

        let message = RaftMessage::RequestVote(create_request_vote_data());

        let result = message_follower(follower, &message, &String::from("leader"), &config, &mut context);

        match result {
            (Role::Follower(data), _) => {
                assert!(data.election_timeout.elapsed().as_millis() < elapsed);
            },
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn when_become_candidate_then_increment_current_turn() {

        let mut context = create_context();
        context.persistent_state.current_term = 2;

        let config = create_config();

        let peers = create_peers();

        become_candidate(&peers, &config, &mut context);

        assert_eq!(context.persistent_state.current_term, 3);
    }

    #[test]
    fn when_become_candidate_then_vote_for_self() {

        let mut context = create_context();
        context.name = String::from("self");
        context.persistent_state.voted_for = None;

        let config = create_config();

        let peers = create_peers();

        become_candidate(&peers, &config, &mut context);

        assert_eq!(context.persistent_state.voted_for.unwrap(), String::from("self"));
    }

    #[test]
    fn when_become_candidate_then_reset_election_timer() {
        let mut context = create_context();

        let config = create_config();

        let peers = create_peers();

        let result = become_candidate(&peers, &config, &mut context);

        match result {
            (Role::Candidate(data), _) => {
                assert!(data.election_timeout.elapsed().as_millis() < u128::from(config.election_timeout_length));
            },
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn when_become_candidate_then_send_request_vote_to_all_peers() {
        let mut context = create_context();

        let config = create_config();

        let peers = create_peers();

        let (_role, messages) = become_candidate(&peers, &config, &mut context);

        assert_eq!(messages.len(), peers.len());
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::RequestVote(..), ..}));
    }

    #[test]
    fn when_tick_candidate_given_majority_approves_then_become_leader() {
        let mut context = create_context();

        let config = create_config();

        let mut candidate = create_candidate_data();
        candidate.peers_approving.push(String::from("peer 1"));

        let peers = create_peers();

        let (role, _messages) = tick_candidate(candidate, &peers, &config, &mut context);

        assert!(matches!(role, Role::Leader{..}));
    }

    #[test]
    fn when_message_candidate_given_append_entries_from_new_leader_then_become_follower() {
        let mut context = create_context();

        let config = create_config();

        let candidate = create_candidate_data();

        let message = RaftMessage::AppendEntries(create_append_entries_data());

        let (role, _messages) = message_candidate(candidate, &message, &String::from("new leader"), &config, &mut context);

        assert!(matches!(role, Role::Follower{..}));
    }

    #[test]
    fn when_tick_candidate_given_elapsed_election_timeout_then_become_candidate() {
        let mut context = create_context();

        let config = create_config();

        let mut candidate = create_candidate_data();
        candidate.election_timeout = Instant::now() - Duration::new(500, 0);

        let peers = create_peers();

        let (role, _messages) = tick_candidate(candidate, &peers, &config, &mut context);

        assert!(matches!(role, Role::Candidate{..}));
    }

    #[test]
    fn when_become_leader_then_inform_all_peers() {

        let mut context = create_context();

        let config = create_config();

        let peers = create_peers();

        let (_role, messages) = become_leader(&peers, &config, &mut context);

        assert_eq!(messages.len(), peers.len());
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntries(..), .. }));
    }

    #[test]
    fn when_tick_leader_given_idle_timeout_then_send_heartbeat_to_all_peers() {

        let mut context = create_context();

        let config = create_config();

        let peers = create_peers();

        let mut leader = create_leader_data();
        leader.idle_timeout = Instant::now() - Duration::new(500, 0);

        let (_role, messages) = tick_leader(leader, &peers, &config, &mut context);

        assert_eq!(messages.len(), peers.len());
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntries(..), .. }));
    }

    #[test]
    fn when_tick_leader_given_follower_next_index_less_than_log_index_then_send_append_entries() {

        let mut context = create_context();

        let config = create_config();

        let peers = create_peers();

        let mut leader = create_leader_data();
        leader.next_index.insert(String::from("peer 1"), 1);
        leader.next_index.insert(String::from("peer 2"), 2);

        let (_role, messages) = tick_leader(leader, &peers, &config, &mut context);

        assert_eq!(messages.len(), 1);
        assert_eq!(messages.get(0).unwrap().peer, String::from("peer 1"));
        assert!(matches!(messages.get(0).unwrap(), DataMessage { raft_message: RaftMessage::AppendEntries(..), .. }));
    }

    #[test]
    fn when_message_leader_given_successful_append_entries_response_then_update_peer_indices() {

        let mut context = create_context();

        let config = create_config();

        let mut leader = create_leader_data();
        leader.next_index.insert(String::from("peer 1"), 1);
        leader.match_index.insert(String::from("peer 1"), Some(0));

        let mut message = create_append_entries_response_data();
        message.last_log_index = Some(1);
        let message = RaftMessage::AppendEntriesResponse(message);

        let result = message_leader(leader, &message, &String::from("peer 1"), &config, &mut context);

        match result {
            (Role::Leader(data), _) => {
                assert_eq!(data.next_index.get(&String::from("peer 1")).unwrap(), &2);
                assert_eq!(data.match_index.get(&String::from("peer 1")).unwrap().unwrap(), 1);
            },
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn when_message_leader_given_failed_append_entries_response_then_decrement_next_peer_index() {

        let mut context = create_context();

        let config = create_config();

        let mut leader = create_leader_data();
        leader.next_index.insert(String::from("peer 1"), 1);

        let mut message = create_append_entries_response_data();
        message.success = false;
        let message = RaftMessage::AppendEntriesResponse(message);

        let result = message_leader(leader, &message, &String::from("peer 1"), &config, &mut context);

        match result {
            (Role::Leader(data), _) => {
                assert_eq!(data.next_index.get(&String::from("peer 1")).unwrap(), &0);
            },
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn when_tick_leader_given_a_match_index_majority_over_commit_index_then_increase_commit_index() {

        let mut context = create_context();
        context.volatile_state.commit_index = Some(0);
        context.persistent_state.log.push(LogPost { term: context.persistent_state.current_term, value: 17 });

        let config = create_config();

        let peers = create_peers();

        let mut leader = create_leader_data();
        leader.next_index.insert(String::from("peer 1"), 3);
        leader.match_index.insert(String::from("peer 1"), Some(2));

        tick_leader(leader, &peers, &config, &mut context);

        assert_eq!(context.volatile_state.commit_index.unwrap(), 2);
    }

    fn create_peers() -> Vec<String> {
        vec!(String::from("peer 1"), String::from("peer 2"))
    }
    
    fn create_follower() -> Role {
        Role::Follower(create_follower_data())
    }

    fn create_follower_data() -> FollowerData {
        FollowerData {
            election_timeout: Instant::now(),
            election_timeout_length: 10000            
        }
    }

    fn create_candidate() -> Role {
        Role::Candidate(create_candidate_data())
    }

    fn create_candidate_data() -> CandidateData {
        CandidateData {
            election_timeout: Instant::now(),
            election_timeout_length: 10000,
            peers_approving: vec!(),
            peers_undecided: vec!()
        }
    }

    fn create_leader_data() -> LeaderData {
        LeaderData {
            idle_timeout: Instant::now(),
            idle_timeout_length: 1000,
            match_index: HashMap::new(),
            next_index: HashMap::new()
        }
    }

    fn create_append_entries(term: u64) -> RaftMessage {
        let mut data = create_append_entries_data();
        data.term = term;
        RaftMessage::AppendEntries(data)
    }

    fn create_append_entries_response(term: u64) -> RaftMessage {
        let mut data = create_append_entries_response_data();
        data.term = term;
        RaftMessage::AppendEntriesResponse(data)
    }

    fn create_request_vote(term: u64) -> RaftMessage {
        let mut data = create_request_vote_data();
        data.term = term;
        RaftMessage::RequestVote(data)
    }

    fn create_request_vote_response(term: u64) -> RaftMessage {
        let mut data = create_request_vote_response_data();
        data.term = term;
        RaftMessage::RequestVoteResponse(data)
    }

    fn create_append_entries_data() -> AppendEntriesData {
        AppendEntriesData {
            term: 5,
            prev_log_index: Some(1),
            prev_log_term: Some(1),
            entries: vec!(LogPost { term: 5, value: 3 }),
            leader_commit: Some(2)
        }
    }

    fn create_append_entries_response_data() -> AppendEntriesResponseData {
        AppendEntriesResponseData {
            term: 5,
            success: true,
            last_log_index: Some(1)
        }
    }

    fn create_request_vote_data() -> RequestVoteData {
        RequestVoteData {
            term: 6,
            last_log_index: Some(1), 
            last_log_term: Some(1)
        }
    }

    fn create_request_vote_response_data() -> RequestVoteResponseData {
        RequestVoteResponseData {
            term: 6,
            vote_granted: true
        }
    }

    fn create_context() -> Context {
        Context {
            name: String::from("test"),
            persistent_state: PersistentState {
                log: vec!(LogPost { term: 1, value: 0 }, LogPost { term: 1, value: 7 }),
                current_term: 5, 
                voted_for: None
            },
            random: rand::thread_rng(),
            volatile_state: VolatileState {
                commit_index: Some(1),
                last_applied: Some(1)
            }
        }
    }

    fn create_config() -> TimeoutConfig {
        TimeoutConfig {
            election_timeout_length: 10000,
            idle_timeout_length: 1000
        }
    }
}
