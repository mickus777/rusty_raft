use crate::data;

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
