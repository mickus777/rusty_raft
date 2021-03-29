use std::cmp;

use log::*;

use crate::config;
use crate::context;
use crate::data;
use crate::messages;
use crate::roles;
use crate::utils;

pub mod message_handling {
    use super::*;
    
    pub fn message_role(role: roles::Role, message: &messages::RaftMessage, peer: &String, config: &config::TimeoutConfig, context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
        let term = messages::get_message_term(&message);
        let mut should_become_follower = false;
        if term > context.persistent_state.current_term {
            context.persistent_state.current_term = term;
            context.persistent_state.voted_for = None;
            should_become_follower = true;
        }

        let (role, messages) = match role {
            roles::Role::Follower(data) => {
                message_follower(data, message, peer, config, context)
            },
            roles::Role::Candidate(data) => {
                message_candidate(data, message, peer, config, context)
            },
            roles::Role::Leader(data) => {
                message_leader(data, message, peer, config, context)
            }
        };

        if should_become_follower {
            (roles::Role::new_follower(config, context), messages)
        } else {
            (role, messages)
        }
    }

    fn message_follower(follower: roles::FollowerData, raft_message: &messages::RaftMessage, peer: &String, config: &config::TimeoutConfig, context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
        match raft_message {
            messages::RaftMessage::AppendEntries(data) => {
                debug!("F {}: Received AppendEntries from {} term: {}", context.persistent_state.current_term, peer, data.term);
                handle_append_entries(roles::Role::Follower(follower), &data, config, context, &peer)
            },
            messages::RaftMessage::AppendEntriesResponse(data) => {
                debug!("F {}: Received misguided AppendEntriesResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.success, data.term);
                (roles::Role::Follower(follower), Vec::new())
            },
            messages::RaftMessage::RequestVote(data) => {
                debug!("F {}: Received RequestVote from {} term: {}", context.persistent_state.current_term, peer, data.term);
                if handle_request_vote(&data, &peer, config, context) {
                    (roles::Role::new_follower(config, context), vec!(messages::DataMessage::new_request_vote_response(&context.persistent_state.current_term, true, &peer)))
                } else {
                    (roles::Role::Follower(follower), vec!(messages::DataMessage::new_request_vote_response(&context.persistent_state.current_term, false, &peer)))
                }
            },
            messages::RaftMessage::RequestVoteResponse(data) => {
                debug!("F {}: Received misguided RequestVoteResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.vote_granted, data.term);
                (roles::Role::Follower(follower), Vec::new())
            }
        }
    }

    fn message_candidate(mut candidate: roles::CandidateData, raft_message: &messages::RaftMessage, peer: &String, config: &config::TimeoutConfig, context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
        match raft_message {
            messages::RaftMessage::AppendEntries(data) => {
                debug!("C {}: Received append entries from {}, term: {}", context.persistent_state.current_term, peer, data.term);
                handle_append_entries(roles::Role::Candidate(candidate), &data, config, context, &peer)
            },
            messages::RaftMessage::AppendEntriesResponse(data) => {
                debug!("C {}: Received misguided AppendEntriesResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.success, data.term);
                (roles::Role::Candidate(candidate), Vec::new())
            },
            messages::RaftMessage::RequestVote(data) => {
                debug!("C {}: RequestVote from {} with term {}", context.persistent_state.current_term, peer, data.term);
                let result = handle_request_vote(&data, &peer, config, context);
                (roles::Role::Candidate(candidate), vec!(messages::DataMessage::new_request_vote_response(&context.persistent_state.current_term, result, &peer)))
            },
            messages::RaftMessage::RequestVoteResponse(data) => {
                debug!("C {}: RequestVoteResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.vote_granted, data.term);
                if data.term > context.persistent_state.current_term {
                    context.persistent_state.current_term = data.term;
                    context.persistent_state.voted_for = None;
                    (roles::Role::new_follower(config, context), Vec::new())
                } else {
                    candidate.peers_undecided.retain(|undecided| *undecided != *peer);
                    if data.vote_granted {
                        candidate.peers_approving.push(peer.clone());
                    }
                    (roles::Role::Candidate(candidate), Vec::new())
                }
            }
        }
    }

    fn message_leader(mut leader: roles::LeaderData, raft_message: &messages::RaftMessage, peer: &String, config: &config::TimeoutConfig, context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
        match raft_message {
            messages::RaftMessage::AppendEntries(data) => {
                debug!("L {}: AppendEntries from {} with term {}", context.persistent_state.current_term, peer, data.term);
                handle_append_entries(roles::Role::Leader(leader), &data, config, context, &peer)
            },
            messages::RaftMessage::AppendEntriesResponse(data) => {
                debug!("L {}: AppendEntriesResponse from {} with {} term {}", context.persistent_state.current_term, peer, data.success, data.term);
                if data.term > context.persistent_state.current_term {
                    context.persistent_state.current_term = data.term;
                    context.persistent_state.voted_for = None;
                    (roles::Role::new_follower(config, context), Vec::new())
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
                                        messages.push(messages::DataMessage::new_append_entries(&context.persistent_state.current_term, &Some(new_next_index - 1), &Some(post.term), &context.persistent_state.log[new_next_index..].iter().cloned().collect(), &context.volatile_state.commit_index, peer));
                                    } else {
                                        panic!("Since new_next_index is at least zero there must be something in the log.");
                                    }
                                } else {
                                    messages.push(messages::DataMessage::new_append_entries(&context.persistent_state.current_term, &None, &None, &context.persistent_state.log, &context.volatile_state.commit_index, peer));
                                }
                            } else {
                                leader.next_index.remove(peer);
                                messages.push(messages::DataMessage::new_append_entries(&context.persistent_state.current_term, &None, &None, &context.persistent_state.log, &context.volatile_state.commit_index, peer));
                            }
                        } else {
                            panic!("We should not fail here, there is no prior data");
                        }
                    }
                    //////////////////////////////////////////////////////////////////////////////////

                    (roles::Role::Leader(leader), messages)
                }
            },
            messages::RaftMessage::RequestVote(data) => {
                debug!("L {}: RequestVote from {} with term {}", context.persistent_state.current_term, peer, data.term);
                let result = handle_request_vote(&data, &peer, config, context);
                (roles::Role::Leader(leader), vec!(messages::DataMessage::new_request_vote_response(&context.persistent_state.current_term, result, &peer)))
            },
            messages::RaftMessage::RequestVoteResponse(data) => {
                debug!("L {}: RequestVoteResponse from {} with {} term: {}", context.persistent_state.current_term, peer, data.vote_granted, data.term);
                // Ignore for now
                (roles::Role::Leader(leader), Vec::new())
            }
        }
    }

    fn handle_append_entries(role: roles::Role, append_entries: &messages::AppendEntriesData, config: &config::TimeoutConfig, context: &mut context::Context, peer: &String) -> (roles::Role, Vec<messages::DataMessage>) {
        //////////////////////////////////////////////////////////////////////////////////////////////
        // Receiver rule 1:
        if append_entries.term < context.persistent_state.current_term {
            return (role, vec!(messages::DataMessage::new_append_entries_response(&context.persistent_state.current_term, &false, &None, peer)))
        }
        //////////////////////////////////////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Receiver rule 2:
        if let Some(prev_index) = append_entries.prev_log_index {
            if let Some(prev_term) = append_entries.prev_log_term {
                if let Some(post) = context.persistent_state.log.get(prev_index) {
                    if post.term != prev_term {
                        // Log contains an entry at leader's previous index but its term is not the same as that of the leader
                        return (role, vec!(messages::DataMessage::new_append_entries_response(&context.persistent_state.current_term, &false, &None, peer)))
                    }
                } else {
                    // Log does not contain an entry at leader's previous index
                    return (role, vec!(messages::DataMessage::new_append_entries_response(&context.persistent_state.current_term, &false, &None, peer)))
                }
            } else {
                panic!("If there is a previous index, there must also be a previous term.");
            }
        }
        //////////////////////////////////////////////////////////////////////////////////////////////

        //////////////////////////////////////////////////////////////////////////////////////////////
        // Receiver rule 3 and 4:
        data::append_entries_from(&mut context.persistent_state.log, &append_entries.entries, &append_entries.prev_log_index);
        //////////////////////////////////////////////////////////////////////////////////////////////

        let mut last_log_index = None;
        if context.persistent_state.log.len() > 0 {
            last_log_index = Some(context.persistent_state.log.len() - 1);
        }
        let messages = vec!(messages::DataMessage::new_append_entries_response(&context.persistent_state.current_term, &true, &last_log_index, peer));

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

        (roles::Role::new_follower(config, context), messages)
    }

    fn handle_request_vote(data: &messages::RequestVoteData, candidate: &String, _config: &config::TimeoutConfig, context: &mut context::Context) -> bool {
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
        let result = data::check_last_log_post(&data.last_log_index, &data.last_log_term, &context.persistent_state.log);
        //////////////////////////////////////////////////////////////////////////////////////////////

        if result {
            context.persistent_state.voted_for = Some(candidate.clone());
            result
        } else {
            result
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use std::collections::HashMap;
        use std::time::Duration;
        use std::time::Instant;
        
        #[test]
        fn when_message_role_given_new_term_then_become_follower() {

            let mut context = create_context();

            let config = create_config();

            let role = roles::Role::Candidate(create_candidate_data());

            let message = messages::RaftMessage::RequestVote(create_request_vote_data());

            let result = message_role(role, &message, &String::from("peer 1"), &config, &mut context);

            assert_eq!(context.persistent_state.current_term, 6);
            assert!(matches!(result, (roles::Role::Follower{ .. }, ..)));
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
            let message = messages::RaftMessage::RequestVote(data);

            let result = message_follower(follower, &message, &String::from("leader"), &config, &mut context);

            match result {
                (roles::Role::Follower(data), _) => {
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

            let message = messages::RaftMessage::AppendEntries(messages::AppendEntriesData{
                term: context.persistent_state.current_term - 1,
                entries: Vec::new(),
                leader_commit: Some(0),
                prev_log_index: Some(1),
                prev_log_term: Some(1)
            });

            let (role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
            let (role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
            let (role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Leader(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
        }

        #[test]
        fn when_append_entries_given_nonexisting_previous_index_then_reply_false() {
            let mut context = create_context();

            let config = create_config();

            let message = messages::RaftMessage::AppendEntries(messages::AppendEntriesData{
                term: context.persistent_state.current_term,
                entries: Vec::new(),
                leader_commit: Some(0),
                prev_log_index: Some(3),
                prev_log_term: Some(1)
            });

            let (role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
            let (role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
            let (role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Leader(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
        }

        #[test]
        fn when_append_entries_given_conflicting_previous_term_then_reply_false() {
            let mut context = create_context();

            let config = create_config();

            let message = messages::RaftMessage::AppendEntries(messages::AppendEntriesData{
                term: context.persistent_state.current_term,
                entries: Vec::new(),
                leader_commit: Some(0),
                prev_log_index: Some(1),
                prev_log_term: Some(2)
            });

            let (role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
            let (role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
            let (role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Leader(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. })));
        }

        #[test]
        fn when_append_entries_given_conflicting_new_terms_then_remove_old_and_append_new() {
            let config = create_config();

            let message = messages::RaftMessage::AppendEntries(messages::AppendEntriesData{
                term: 5,
                entries: vec!(data::LogPost { term: 2, value: 9 }, data::LogPost { term: 2, value: 7 }, data::LogPost { term: 3, value: 8 }),
                leader_commit: Some(0),
                prev_log_index: Some(0),
                prev_log_term: Some(1)
            });

            let expected = vec!(data::LogPost { term: 1, value: 0}, data::LogPost { term: 2, value: 9 }, data::LogPost { term: 2, value: 7 }, data::LogPost { term: 3, value: 8 });

            let mut context = create_context();
            context.persistent_state.log.push(data::LogPost { term: 4, value: 17 });
            let (_role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.persistent_state.log, expected);
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));

            let mut context = create_context();
            context.persistent_state.log.push(data::LogPost { term: 4, value: 17 });
            let (_role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.persistent_state.log, expected);
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));

            let mut context = create_context();
            context.persistent_state.log.push(data::LogPost { term: 4, value: 17 });
            let (_role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.persistent_state.log, expected);
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));
        }

        #[test]
        fn when_append_entries_given_new_leader_commit_less_than_log_len_then_set_commit_index_to_leader_commit() {
            let config = create_config();

            let message = messages::RaftMessage::AppendEntries(messages::AppendEntriesData{
                term: 5,
                entries: vec!(data::LogPost { term: 2, value: 9 }),
                leader_commit: Some(1),
                prev_log_index: Some(1),
                prev_log_term: Some(1)
            });

            let mut context = create_context();
            context.volatile_state.commit_index = Some(0);
            let (_role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.volatile_state.commit_index, Some(1));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));

            let mut context = create_context();
            context.volatile_state.commit_index = Some(0);
            let (_role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.volatile_state.commit_index, Some(1));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));

            let mut context = create_context();
            context.volatile_state.commit_index = Some(0);
            let (_role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.volatile_state.commit_index, Some(1));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));
        }

        #[test]
        fn when_append_entries_given_new_leader_commit_greater_than_log_len_then_set_commit_index_to_log_len() {
            let config = create_config();

            let message = messages::RaftMessage::AppendEntries(messages::AppendEntriesData{
                term: 5,
                entries: vec!(data::LogPost { term: 2, value: 9 }),
                leader_commit: Some(7),
                prev_log_index: Some(1),
                prev_log_term: Some(1)
            });

            let mut context = create_context();
            context.volatile_state.commit_index = Some(0);
            let (_role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.volatile_state.commit_index, Some(2));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));

            let mut context = create_context();
            context.volatile_state.commit_index = Some(0);
            let (_role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.volatile_state.commit_index, Some(2));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));

            let mut context = create_context();
            context.volatile_state.commit_index = Some(0);
            let (_role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert_eq!(context.volatile_state.commit_index, Some(2));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. })));
        }

        #[test]
        fn when_request_vote_given_old_term_then_do_not_grant_vote() {
            let mut context = create_context();

            let config = create_config();

            let message = messages::RaftMessage::RequestVote(messages::RequestVoteData{
                term: context.persistent_state.current_term - 1,
                last_log_index: Some(1),
                last_log_term: Some(1)
            });

            let (role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, term: 5 })));

            let (role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, term: 5 })));

            let (role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Leader(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, term: 5 })));
        }

        #[test]
        fn when_request_vote_given_already_voted_for_someone_else_this_term_then_do_not_grant_vote() {
            let mut context = create_context();
            context.persistent_state.voted_for = Some(String::from("someone"));

            let config = create_config();

            let message = messages::RaftMessage::RequestVote(messages::RequestVoteData{
                term: context.persistent_state.current_term,
                last_log_index: Some(1),
                last_log_term: Some(1)
            });

            let (role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, .. })));

            let (role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, .. })));

            let (role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Leader(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, .. })));
        }

        #[test]
        fn when_request_vote_given_candidates_log_is_old_then_do_not_grant_vote() {
            let mut context = create_context();

            let config = create_config();

            let message = messages::RaftMessage::RequestVote(messages::RequestVoteData{
                term: context.persistent_state.current_term,
                last_log_index: Some(0),
                last_log_term: Some(1)
            });

            let (role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, .. })));

            let (role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, .. })));

            let (role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Leader(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: false, .. })));
        }

        #[test]
        fn when_request_vote_given_new_term_then_grant_vote() {
            let mut context = create_context();

            let config = create_config();

            let message = messages::RaftMessage::RequestVote(messages::RequestVoteData{
                term: context.persistent_state.current_term + 1,
                last_log_index: Some(1),
                last_log_term: Some(1)
            });

            context.persistent_state.current_term = 5;
            let (role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: true, term: 6 })));

            context.persistent_state.current_term = 5;
            let (role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: true, term: 6 })));

            context.persistent_state.current_term = 5;
            let (role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: true, term: 6 })));
        }

        #[test]
        fn when_request_vote_given_rerequest_from_same_candidate_then_grant_vote() {
            let mut context = create_context();
            context.persistent_state.voted_for = Some(String::from("other"));

            let config = create_config();

            let message = messages::RaftMessage::RequestVote(messages::RequestVoteData{
                term: context.persistent_state.current_term,
                last_log_index: Some(1),
                last_log_term: Some(1)
            });

            let (role, messages) = message_role(roles::Role::Follower(create_follower_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: true, term: 5 })));

            let (role, messages) = message_role(roles::Role::Candidate(create_candidate_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: true, term: 5 })));

            let (role, messages) = message_role(roles::Role::Leader(create_leader_data()), &message, &String::from("other"), &config, &mut context);
            assert!(matches!(role, roles::Role::Leader(..)));
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap().raft_message, messages::RaftMessage::RequestVoteResponse(messages::RequestVoteResponseData{ vote_granted: true, term: 5 })));
        }

        #[test]
        fn when_message_role_given_new_term_then_accept_term_and_become_follower() {
            let mut context = create_context();

            let config = create_config();

            for message in vec!(create_append_entries(6), create_append_entries_response(6), create_request_vote(6), create_request_vote_response(6)) {
                for role in vec!(roles::Role::Follower(create_follower_data()), roles::Role::Candidate(create_candidate_data()), roles::Role::Leader(create_leader_data())) {
                    context.persistent_state.current_term = 5;
                    let (new_role, _messages) = message_role(role, &message, &String::from("other"), &config, &mut context);
                    assert!(matches!(new_role, roles::Role::Follower(..)));
                    assert_eq!(context.persistent_state.current_term, 6);
                }
            }
        }

        #[test]
        fn when_message_follower_given_append_entries_then_election_timeout_is_reset() {
            let mut context = create_context();

            let config = create_config();

            let mut follower = create_follower_data();
            follower.election_timeout = Instant::now() - Duration::new(500, 0);
            let elapsed = follower.election_timeout.elapsed().as_millis();

            let message = messages::RaftMessage::AppendEntries(create_append_entries_data());

            let result = message_follower(follower, &message, &String::from("leader"), &config, &mut context);

            match result {
                (roles::Role::Follower(data), _) => {
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

            let message = messages::RaftMessage::RequestVote(create_request_vote_data());

            let result = message_follower(follower, &message, &String::from("leader"), &config, &mut context);

            match result {
                (roles::Role::Follower(data), _) => {
                    assert!(data.election_timeout.elapsed().as_millis() < elapsed);
                },
                _ => {
                    panic!();
                }
            }
        }

        #[test]
        fn when_message_candidate_given_append_entries_from_new_leader_then_become_follower() {
            let mut context = create_context();

            let config = create_config();

            let candidate = create_candidate_data();

            let message = messages::RaftMessage::AppendEntries(create_append_entries_data());

            let (role, _messages) = message_candidate(candidate, &message, &String::from("new leader"), &config, &mut context);

            assert!(matches!(role, roles::Role::Follower{..}));
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
            let message = messages::RaftMessage::AppendEntriesResponse(message);

            let result = message_leader(leader, &message, &String::from("peer 1"), &config, &mut context);

            match result {
                (roles::Role::Leader(data), _) => {
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
            let message = messages::RaftMessage::AppendEntriesResponse(message);

            let result = message_leader(leader, &message, &String::from("peer 1"), &config, &mut context);

            match result {
                (roles::Role::Leader(data), _) => {
                    assert_eq!(data.next_index.get(&String::from("peer 1")).unwrap(), &0);
                },
                _ => {
                    panic!();
                }
            }
        }

        #[test]
        fn when_handle_append_entries_given_append_entries_with_old_term_then_signal_failure() {

            let mut context = create_context();

            let config = create_config();

            let mut append_entries = create_append_entries_data();
            append_entries.term = 1;

            let (role, messages) = handle_append_entries(roles::Role::Follower(create_follower_data()), &append_entries, &config, &mut context, &String::from("peer"));

            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. }), .. }));
            assert!(matches!(role, roles::Role::Follower{..}));
        }

        #[test]
        fn when_handle_append_entries_given_append_entries_with_previous_log_conflict_then_signal_failure() {
            let mut context = create_context();
            context.persistent_state.log.push(data::LogPost { term: 4, value: 1 });

            let config = create_config();

            let mut append_entries = create_append_entries_data();
            append_entries.prev_log_index = Some(1);
            append_entries.prev_log_term = Some(5);

            let (role, messages) = handle_append_entries(roles::Role::Follower(create_follower_data()), &append_entries, &config, &mut context, &String::from("peer"));

            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. }), .. }));
            assert!(matches!(role, roles::Role::Follower{..}));
        }

        #[test]
        fn when_handle_append_entries_given_append_entries_with_nonexisting_previous_log_then_signal_failure() {
            let mut context = create_context();

            let config = create_config();

            let mut append_entries = create_append_entries_data();
            append_entries.prev_log_index = Some(1);
            append_entries.prev_log_term = Some(5);

            let (role, messages) = handle_append_entries(roles::Role::Follower(create_follower_data()), &append_entries, &config, &mut context, &String::from("peer"));

            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: false, .. }), .. }));
            assert!(matches!(role, roles::Role::Follower{..}));
        }

        #[test]
        fn when_handle_append_entries_given_append_entries_with_new_posts_then_append_posts() {
            let mut context = create_context();

            let config = create_config();

            let append_entries = create_append_entries_data();

            let (role, messages) = handle_append_entries(roles::Role::Follower(create_follower_data()), &append_entries, &config, &mut context, &String::from("peer"));

            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. }), .. }));
            assert!(matches!(role, roles::Role::Follower{..}));
        }

        #[test]
        fn when_handle_append_entries_given_append_entries_with_conflicting_new_posts_then_replace_conflicting_posts() {
            let mut context = create_context();
            context.persistent_state.log.push(data::LogPost { term: 2, value: 13 });
            context.persistent_state.log.push(data::LogPost { term: 3, value: 17 });

            let config = create_config();

            let append_entries = create_append_entries_data();

            let (role, messages) = handle_append_entries(roles::Role::Follower(create_follower_data()), &append_entries, &config, &mut context, &String::from("peer"));

            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. }), .. }));
            assert!(matches!(role, roles::Role::Follower{..}));
        }

        #[test]
        fn when_handle_append_entries_given_append_entries_with_new_leader_commit_then_set_commit_index() {
            let mut context = create_context();

            let config = create_config();

            let append_entries = create_append_entries_data();

            let (role, messages) = handle_append_entries(roles::Role::Follower(create_follower_data()), &append_entries, &config, &mut context, &String::from("peer"));

            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. }), .. }));
            assert!(matches!(role, roles::Role::Follower{..}));
            assert_eq!(context.persistent_state.log.len(), 3);
            assert_eq!(context.volatile_state.commit_index.unwrap(), 2);
        }

        #[test]
        fn when_handle_append_entries_given_append_entries_with_old_leader_commit_then_dont_set_commit_index() {
            let mut context = create_context();

            let config = create_config();

            let mut append_entries = create_append_entries_data();
            append_entries.leader_commit = Some(0);

            let (role, messages) = handle_append_entries(roles::Role::Follower(create_follower_data()), &append_entries, &config, &mut context, &String::from("peer"));

            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. }), .. }));
            assert!(matches!(role, roles::Role::Follower{..}));
            assert_eq!(context.persistent_state.log.len(), 3);
            assert_eq!(context.volatile_state.commit_index.unwrap(), 1);
        }

        #[test]
        fn when_handle_append_entries_given_append_entries_with_unknown_leader_commit_then_set_commit_index_to_new_last_entry() {
            let mut context = create_context();

            let config = create_config();

            let mut append_entries = create_append_entries_data();
            append_entries.leader_commit = Some(7);

            let (role, messages) = handle_append_entries(roles::Role::Follower(create_follower_data()), &append_entries, &config, &mut context, &String::from("peer"));

            assert_eq!(messages.len(), 1);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntriesResponse(messages::AppendEntriesResponseData{ success: true, .. }), .. }));
            assert!(matches!(role, roles::Role::Follower{..}));
            assert_eq!(context.persistent_state.log.len(), 3);
            assert_eq!(context.volatile_state.commit_index.unwrap(), 2);
        }

        fn create_follower_data() -> roles::FollowerData {
            roles::FollowerData {
                election_timeout: Instant::now(),
                election_timeout_length: 10000            
            }
        }

        fn create_candidate_data() -> roles::CandidateData {
            roles::CandidateData {
                election_timeout: Instant::now(),
                election_timeout_length: 10000,
                peers_approving: vec!(),
                peers_undecided: vec!()
            }
        }

        fn create_leader_data() -> roles::LeaderData {
            roles::LeaderData {
                idle_timeout: Instant::now(),
                idle_timeout_length: 1000,
                match_index: HashMap::new(),
                next_index: HashMap::new()
            }
        }

        fn create_append_entries(term: u64) -> messages::RaftMessage {
            let mut data = create_append_entries_data();
            data.term = term;
            messages::RaftMessage::AppendEntries(data)
        }

        fn create_append_entries_response(term: u64) -> messages::RaftMessage {
            let mut data = create_append_entries_response_data();
            data.term = term;
            messages::RaftMessage::AppendEntriesResponse(data)
        }

        fn create_request_vote(term: u64) -> messages::RaftMessage {
            let mut data = create_request_vote_data();
            data.term = term;
            messages::RaftMessage::RequestVote(data)
        }

        fn create_request_vote_response(term: u64) -> messages::RaftMessage {
            let mut data = create_request_vote_response_data();
            data.term = term;
            messages::RaftMessage::RequestVoteResponse(data)
        }

        fn create_append_entries_data() -> messages::AppendEntriesData {
            messages::AppendEntriesData {
                term: 5,
                prev_log_index: Some(1),
                prev_log_term: Some(1),
                entries: vec!(data::LogPost { term: 5, value: 3 }),
                leader_commit: Some(2)
            }
        }

        fn create_append_entries_response_data() -> messages::AppendEntriesResponseData {
            messages::AppendEntriesResponseData {
                term: 5,
                success: true,
                last_log_index: Some(1)
            }
        }

        fn create_request_vote_data() -> messages::RequestVoteData {
            messages::RequestVoteData {
                term: 6,
                last_log_index: Some(1), 
                last_log_term: Some(1)
            }
        }

        fn create_request_vote_response_data() -> messages::RequestVoteResponseData {
            messages::RequestVoteResponseData {
                term: 6,
                vote_granted: true
            }
        }

        fn create_context() -> context::Context {
            context::Context {
                name: String::from("test"),
                persistent_state: context::PersistentState {
                    log: vec!(data::LogPost { term: 1, value: 0 }, data::LogPost { term: 1, value: 7 }),
                    current_term: 5, 
                    voted_for: None
                },
                random: rand::thread_rng(),
                volatile_state: context::VolatileState {
                    commit_index: Some(1),
                    last_applied: Some(1)
                }
            }
        }

        fn create_config() -> config::TimeoutConfig {
            config::TimeoutConfig {
                election_timeout_length: 10000,
                idle_timeout_length: 1000
            }
        }
    }
}

pub mod tick_handling {
    use super::*;

    use std::collections::HashMap;
    use std::time::Instant;

    pub fn tick_role(role: roles::Role, 
        peers: &Vec<String>, 
        config: &config::TimeoutConfig, 
        context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
    
        //////////////////////////////////////////////////////////////////////////////////////////////
        // Rule: All servers 1
        if let Some(commit_index) = context.volatile_state.commit_index {
            if let Some(last_applied) = context.volatile_state.last_applied {
                if commit_index > last_applied {
                    if let Some(post) = context.persistent_state.log.get(last_applied) {
                        context.volatile_state.last_applied = Some(last_applied + 1);
                        data::apply_log_post(post);
                    } else {
                        panic!("We know that last applied must exist!");
                    }
                }
            } else {
                if let Some(post) = context.persistent_state.log.get(0) {
                    context.volatile_state.last_applied = Some(0);
                    data::apply_log_post(post);
                } else {
                    panic!("Since commit_index has a value there must be something in the log.");
                }
            }
        }
        //////////////////////////////////////////////////////////////////////////////////////////////
    
        match role {
            roles::Role::Follower(data) => {
                tick_follower(data, peers, config, context)
            }
            roles::Role::Candidate(data) => {
                tick_candidate(data, peers, config, context)
            }
            roles::Role::Leader(data) => {
                tick_leader(data, peers, config, context)
            }
        }
    }

    fn tick_follower(follower: roles::FollowerData, 
        peers: &Vec<String>, 
        config: &config::TimeoutConfig, 
        context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
    
        //////////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Followers 2
        if follower.election_timeout.elapsed().as_millis() > follower.election_timeout_length {
            debug!("F {}: Timeout!", context.persistent_state.current_term);
            roles::Role::new_candidate(peers, config, context)
        //////////////////////////////////////////////////////////////////////////////////////////////
        } else {
            debug!("F {}: Timeout in {}", context.persistent_state.current_term, follower.election_timeout_length - follower.election_timeout.elapsed().as_millis());
            (roles::Role::Follower(follower), Vec::new())
        }
    }
    
    fn tick_candidate(candidate: roles::CandidateData, 
        peers: &Vec<String>, 
        config: &config::TimeoutConfig, 
        context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
    
        //////////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Candidate 2
        if candidate.peers_approving.len() >= peers.len() / 2 {
            info!("C {}: Elected!", context.persistent_state.current_term);
            roles::Role::new_leader(peers, config, context)
        //////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Candidate 4
        } else if candidate.election_timeout.elapsed().as_millis() > candidate.election_timeout_length {
            debug!("C {}: Timeout!", context.persistent_state.current_term);
            roles::Role::new_candidate(peers, config, context)
        //////////////////////////////////////////////////////////////////////////////////////////////
        } else {
            debug!("C {}: Timeout in {}", context.persistent_state.current_term, candidate.election_timeout_length - candidate.election_timeout.elapsed().as_millis());
            (roles::Role::Candidate(candidate), Vec::new())
        }
    }
    
    fn tick_leader(mut leader: roles::LeaderData, 
        peers: &Vec<String>, 
        config: &config::TimeoutConfig, 
        context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
    
        //////////////////////////////////////////////////////////////////////////////////////////////
        // Rule: Leader 1 part 2
        if leader.idle_timeout.elapsed().as_millis() > leader.idle_timeout_length {
            let prev_log_term = match utils::top(&context.persistent_state.log) {
                Some(post) => Some(post.term),
                None => None
            };
            let prev_log_index = match context.persistent_state.log.len() {
                0 => None,
                _ => Some(context.persistent_state.log.len() - 1)
            };
            if prev_log_term.is_some() {
                debug!("L {}: Broadcast AppendEntries Idle term: {}, index: {}", context.persistent_state.current_term, prev_log_term.unwrap(), prev_log_index.unwrap());
            } else {
                debug!("L {}: Broadcast AppendEntries Idle", context.persistent_state.current_term);
            }
            let messages = messages::DataMessage::new_append_entrieses(
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
            (roles::Role::Leader(roles::LeaderData {
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
                        debug!("L {} Send AppendEntries {:?} with previous index: {:?} and previous term: {:?}", context.persistent_state.current_term, data::get_log_range(&Some(*next_index), &context.persistent_state.log), prev_log_index, prev_log_term);
                        messages.push(messages::DataMessage::new_append_entries(
                            &context.persistent_state.current_term, 
                            &prev_log_index, 
                            &prev_log_term, 
                            &data::get_log_range(&Some(*next_index), &context.persistent_state.log), 
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
    
            (roles::Role::Leader(leader), messages)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use std::collections::HashMap;
        use std::time::Duration;
        use std::time::Instant;


        #[test]
        fn when_tick_role_given_commit_index_greater_than_last_applied_then_apply_one() {
    
            let mut context = create_context();
            context.volatile_state.commit_index = Some(3);
    
            let config = create_config();
    
            context.volatile_state.last_applied = Some(0);
            let (role, messages) = tick_role(roles::Role::Follower(create_follower_data()), &create_peers(), &config, &mut context);
            assert!(matches!(role, roles::Role::Follower(..)));
            assert_eq!(messages.len(), 0);
            assert_eq!(context.volatile_state.last_applied, Some(1));
    
            context.volatile_state.last_applied = Some(0);
            let (role, messages) = tick_role(roles::Role::Candidate(create_candidate_data()), &create_peers(), &config, &mut context);
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 0);
            assert_eq!(context.volatile_state.last_applied, Some(1));
    
            context.volatile_state.last_applied = Some(0);
            let (role, messages) = tick_role(roles::Role::Leader(create_leader_data()), &create_peers(), &config, &mut context);
            assert!(matches!(role, roles::Role::Leader(..)));
            assert_eq!(messages.len(), 0);
            assert_eq!(context.volatile_state.last_applied, Some(1));
        }
        
        #[test]
        fn when_tick_follower_given_election_timer_timeout_then_become_candidate() {
            
            let mut context = create_context();
    
            let config = create_config();
    
            let mut follower = create_follower_data();
            follower.election_timeout = Instant::now() - Duration::new(500, 0);
    
            let (role, messages) = tick_follower(follower, &create_peers(), &config, &mut context);
    
            assert!(matches!(role, roles::Role::Candidate(..)));
            assert_eq!(messages.len(), 2);
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::RequestVote(..), ..}));
        }
    
        #[test]
        fn when_tick_candidate_given_majority_approves_then_become_leader() {
            let mut context = create_context();
    
            let config = create_config();
    
            let mut candidate = create_candidate_data();
            candidate.peers_approving.push(String::from("peer 1"));
    
            let peers = create_peers();
    
            let (role, _messages) = tick_candidate(candidate, &peers, &config, &mut context);
    
            assert!(matches!(role, roles::Role::Leader{..}));
        }
    
        #[test]
        fn when_tick_candidate_given_elapsed_election_timeout_then_become_candidate() {
            let mut context = create_context();
    
            let config = create_config();
    
            let mut candidate = create_candidate_data();
            candidate.election_timeout = Instant::now() - Duration::new(500, 0);
    
            let peers = create_peers();
    
            let (role, _messages) = tick_candidate(candidate, &peers, &config, &mut context);
    
            assert!(matches!(role, roles::Role::Candidate{..}));
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
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntries(..), .. }));
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
            assert!(matches!(messages.get(0).unwrap(), messages::DataMessage { raft_message: messages::RaftMessage::AppendEntries(..), .. }));
        }
    
        #[test]
        fn when_tick_leader_given_a_match_index_majority_over_commit_index_then_increase_commit_index() {
    
            let mut context = create_context();
            context.volatile_state.commit_index = Some(0);
            context.persistent_state.log.push(data::LogPost { term: context.persistent_state.current_term, value: 17 });
    
            let config = create_config();
    
            let peers = create_peers();
    
            let mut leader = create_leader_data();
            leader.next_index.insert(String::from("peer 1"), 3);
            leader.match_index.insert(String::from("peer 1"), Some(2));
    
            tick_leader(leader, &peers, &config, &mut context);
    
            assert_eq!(context.volatile_state.commit_index.unwrap(), 2);
        }

        fn create_follower_data() -> roles::FollowerData {
            roles::FollowerData {
                election_timeout: Instant::now(),
                election_timeout_length: 10000            
            }
        }
    
        fn create_candidate_data() -> roles::CandidateData {
            roles::CandidateData {
                election_timeout: Instant::now(),
                election_timeout_length: 10000,
                peers_approving: vec!(),
                peers_undecided: vec!()
            }
        }
    
            fn create_leader_data() -> roles::LeaderData {
            roles::LeaderData {
                idle_timeout: Instant::now(),
                idle_timeout_length: 1000,
                match_index: HashMap::new(),
                next_index: HashMap::new()
            }
        }
    
        fn create_peers() -> Vec<String> {
            vec!(String::from("peer 1"), String::from("peer 2"))
        }
        
        fn create_context() -> context::Context {
            context::Context {
                name: String::from("test"),
                persistent_state: context::PersistentState {
                    log: vec!(data::LogPost { term: 1, value: 0 }, data::LogPost { term: 1, value: 7 }),
                    current_term: 5, 
                    voted_for: None
                },
                random: rand::thread_rng(),
                volatile_state: context::VolatileState {
                    commit_index: Some(1),
                    last_applied: Some(1)
                }
            }
        }
    
        fn create_config() -> config::TimeoutConfig {
            config::TimeoutConfig {
                election_timeout_length: 10000,
                idle_timeout_length: 1000
            }
        }
    }
}

pub mod log_handling {
    use super::*;

    use std::time::Instant;

    pub fn role_receive_log(role: roles::Role, message: messages::LogMessage, context: &mut context::Context) -> (roles::Role, Vec<messages::DataMessage>) {
        match role {
            roles::Role::Follower(_) => {
                warn!("Follower can not receive incoming data.");
                (role, Vec::new())
            },
            roles::Role::Candidate(_) => {
                warn!("Candidate can not receive incoming data.");
                (role, Vec::new())
            },
            roles::Role::Leader(leader) => {
                //////////////////////////////////////////////////////////////////////////////////
                // Rule: Leader 2
                (leader_receive_log(leader, message.value, context), Vec::new())
                //////////////////////////////////////////////////////////////////////////////////
            }
        }
    }
    
    fn leader_receive_log(mut leader: roles::LeaderData, value: i32, context: &mut context::Context) -> roles::Role {
        context.persistent_state.log.push(data::LogPost { term: context.persistent_state.current_term, value });
        leader.idle_timeout = Instant::now();
        roles::Role::Leader(leader)
    }    
}
