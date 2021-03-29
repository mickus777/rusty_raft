use crate::data;

pub struct LogMessage {
    pub value: i32
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DataMessage {
    pub raft_message: RaftMessage,
    pub peer: String
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum RaftMessage {
    AppendEntries(AppendEntriesData),
    AppendEntriesResponse(AppendEntriesResponseData),
    RequestVote(RequestVoteData),
    RequestVoteResponse(RequestVoteResponseData)
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub struct AppendEntriesData {
    pub term: u64,
    pub prev_log_index: Option<usize>,
    pub prev_log_term: Option<u64>,
    pub entries: Vec<data::LogPost>,
    pub leader_commit: Option<usize>
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct AppendEntriesResponseData {
    pub term: u64,
    pub success: bool,
    pub last_log_index: Option<usize>
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RequestVoteData {
    pub term: u64,
    pub last_log_index: Option<usize>,
    pub last_log_term: Option<u64>
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RequestVoteResponseData {
    pub term: u64,
    pub vote_granted: bool
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

impl std::fmt::Display for RaftMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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

pub fn get_message_term(message: &RaftMessage) -> u64 {
    match message {
        RaftMessage::AppendEntries(data) => data.term,
        RaftMessage::AppendEntriesResponse(data) => data.term,
        RaftMessage::RequestVote(data) => data.term,
        RaftMessage::RequestVoteResponse(data) => data.term
    }
}

impl DataMessage {
    pub fn new_request_vote(term: &u64, last_log_index: &Option<usize>, last_log_term: &Option<u64>, peer: &String) -> DataMessage {
        DataMessage { 
            raft_message: RaftMessage::RequestVote(RequestVoteData { 
                term: *term,
                last_log_index: *last_log_index,
                last_log_term: *last_log_term
            }), 
            peer: (*peer).clone()
        }
    }

    pub fn new_request_votes(term: &u64, last_log_index: &Option<usize>, last_log_term: &Option<u64>, peers: &Vec<String>) -> Vec<DataMessage> {
        let mut messages = Vec::new();
    
        for peer in peers.iter() {
            messages.push(DataMessage::new_request_vote(term, last_log_index, last_log_term, peer));
        }
    
        messages
    }

    pub fn new_request_vote_response(term: &u64, vote_granted: bool, candidate: &String) -> DataMessage {
        DataMessage { 
            raft_message: RaftMessage::RequestVoteResponse(RequestVoteResponseData { 
                term: *term,
                vote_granted: vote_granted
            }), 
            peer: (*candidate).clone()
        }
    }
    
    pub fn new_append_entries(term: &u64, prev_log_index: &Option<usize>, prev_log_term: &Option<u64>, entries: &Vec<data::LogPost>, leader_commit: &Option<usize>, follower: &String) -> DataMessage {
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

    pub fn new_append_entrieses(term: &u64, prev_log_index: &Option<usize>, prev_log_term: &Option<u64>, entries: &Vec<data::LogPost>, leader_commit: &Option<usize>, followers: &Vec<String>) -> Vec<DataMessage> {
        let mut messages = Vec::new();
    
        for follower in followers.iter() {
            messages.push(DataMessage::new_append_entries(term, prev_log_index, prev_log_term, entries, leader_commit, follower));
        }
    
        messages
    }

    pub fn new_append_entries_response(term: &u64, success: &bool, last_log_index: &Option<usize>, leader: &String) -> DataMessage {
        DataMessage { 
            raft_message: RaftMessage::AppendEntriesResponse(AppendEntriesResponseData {
                term: *term,
                success: *success,
                last_log_index: *last_log_index
            }), 
            peer: (*leader).clone()
        }
    }
}
