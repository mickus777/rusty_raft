use std::collections::HashMap;
use std::time::Instant;

use log::*;
use rand::Rng;

use crate::config;
use crate::context;
use crate::messages;
use crate::utils;

#[derive(Debug)]
pub enum Role {
    Follower(FollowerData),
    Candidate(CandidateData),
    Leader(LeaderData)
}

#[derive(Debug)]
pub struct FollowerData {
    pub election_timeout: Instant,
    pub election_timeout_length: u128,
}

#[derive(Debug)]
pub struct CandidateData {
    pub election_timeout: Instant,
    pub election_timeout_length: u128,
    pub peers_approving: Vec<String>,
    pub peers_undecided: Vec<String>
}

#[derive(Debug)]
pub struct LeaderData {
    pub idle_timeout: Instant,
    pub idle_timeout_length: u128,
    pub next_index: HashMap<String, usize>,
    pub match_index: HashMap<String, Option<usize>>
}

impl Role {
    pub fn new_follower(config: &config::TimeoutConfig, context: &mut context::Context) -> Role {
        return Role::Follower(FollowerData{
            election_timeout: Instant::now(),
            election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random)
        })
    }

    pub fn new_candidate(peers: &Vec<String>, config: &config::TimeoutConfig, context: &mut context::Context) -> (Role, Vec<messages::DataMessage>) {
        //////////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Candidate 1
        context.persistent_state.current_term += 1;
        context.persistent_state.voted_for = Some(context.name.clone());
        let last_log_term = match utils::top(&context.persistent_state.log) {
            Some(post) => Some(post.term),
            None => None
        };
        let last_log_index = match context.persistent_state.log.len() {
            0 => None,
            _ => Some(context.persistent_state.log.len() - 1)
        };
        debug!("X {}: Broadcast request votes to: {:?}", context.persistent_state.current_term, peers);
        (Role::Candidate(CandidateData {
            election_timeout: Instant::now(), 
            election_timeout_length: randomize_timeout(&config.election_timeout_length, &mut context.random), 
            peers_approving: Vec::new(),
            peers_undecided: peers.iter().map(|peer| peer.clone()).collect::<Vec<String>>()
        }), messages::DataMessage::new_request_votes(
            &context.persistent_state.current_term, 
            &last_log_index, 
            &last_log_term,
            &peers))
        //////////////////////////////////////////////////////////////////////////////////////////////
    }

    pub fn new_leader(peers: &Vec<String>, config: &config::TimeoutConfig, context: &mut context::Context) -> (Role, Vec<messages::DataMessage>) {
        //////////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Leader 1 part 1
        let prev_log_term = match utils::top(&context.persistent_state.log) {
            Some(post) => Some(post.term),
            None => None
        };
        let prev_log_index = match context.persistent_state.log.len() {
            0 => None,
            _ => Some(context.persistent_state.log.len())
        };
        debug!("X {}: Broadcast heartbeat to: {:?}", context.persistent_state.current_term, peers);
        let messages = messages::DataMessage::new_append_entrieses(
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
}

fn randomize_timeout(base: &u64, random: &mut rand::rngs::ThreadRng) -> u128 {
    u128::from(base + random.gen_range(1..*base))
}


#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn when_become_candidate_then_increment_current_turn() {

        let mut context = create_context();
        context.persistent_state.current_term = 2;

        let config = create_config();

        let peers = create_peers();

        roles::Role::new_candidate(&peers, &config, &mut context);

        assert_eq!(context.persistent_state.current_term, 3);
    }

    #[test]
    fn when_become_candidate_then_vote_for_self() {

        let mut context = create_context();
        context.name = String::from("self");
        context.persistent_state.voted_for = None;

        let config = create_config();

        let peers = create_peers();

        roles::Role::new_candidate(&peers, &config, &mut context);

        assert_eq!(context.persistent_state.voted_for.unwrap(), String::from("self"));
    }

    #[test]
    fn when_become_candidate_then_reset_election_timer() {
        let mut context = create_context();

        let config = create_config();

        let peers = create_peers();

        let result = roles::Role::new_candidate(&peers, &config, &mut context);

        match result {
            (roles::Role::Candidate(data), _) => {
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

        let (_role, messages) = roles::Role::new_candidate(&peers, &config, &mut context);

        assert_eq!(messages.len(), peers.len());
        assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::RequestVote(..), ..}));
    }

    #[test]
    fn when_become_leader_then_inform_all_peers() {

        let mut context = create_context();

        let config = create_config();

        let peers = create_peers();

        let (_role, messages) = roles::Role::new_leader(&peers, &config, &mut context);

        assert_eq!(messages.len(), peers.len());
        assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntries(..), .. }));
    }

    fn create_peers() -> Vec<String> {
        vec!(String::from("peer 1"), String::from("peer 2"))
    }
    
    fn create_context() -> crate::context::Context {
        crate::context::Context {
            name: String::from("test"),
            persistent_state: crate::context::PersistentState {
                log: vec!(crate::data::LogPost { term: 1, value: 0 }, crate::data::LogPost { term: 1, value: 7 }),
                current_term: 5, 
                voted_for: None
            },
            random: rand::thread_rng(),
            volatile_state: crate::context::VolatileState {
                commit_index: Some(1),
                last_applied: Some(1)
            }
        }
    }

    fn create_config() -> crate::config::TimeoutConfig {
        crate::config::TimeoutConfig {
            election_timeout_length: 10000,
            idle_timeout_length: 1000
        }
    }
}
