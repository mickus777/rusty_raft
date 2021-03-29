use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use log::*;

use crate::config;
use crate::context;
use crate::engine;
use crate::messages;
use crate::roles;

pub struct RaftLoop {
    handle: Option<std::thread::JoinHandle<()>>,
    close_flag: Arc<Mutex<bool>>
}

impl RaftLoop {
    pub fn new(name: String,
        inbound_channel: mpsc::Receiver<messages::DataMessage>, 
        outbound_channel: mpsc::Sender<messages::DataMessage>, 
        log_channel: mpsc::Receiver<messages::LogMessage>,
        config: config::TimeoutConfig, 
        peers: Vec<String>) -> RaftLoop {
        
        let close_flag = Arc::new(Mutex::new(false));
        let close_flag_copy = close_flag.clone();

        RaftLoop {
            handle: Some(thread::spawn(move || { raft_loop(name, close_flag_copy, inbound_channel, outbound_channel, log_channel, config, peers); })),
            close_flag: close_flag
        }
    }
}

impl Drop for RaftLoop {
    fn drop(&mut self) {

        match self.close_flag.lock() {
            Ok(mut guard) => {
                *guard = true;
            },
            Err(message) => {
                error!("Failed to set close-flag of the raft loop: {}", message);
            }
        }

        match self.handle.take().unwrap().join() {
            Ok(_) => {},
            Err(message) => {
                error!("Failed to join the raft loop channel: {:?}", message);
            }
        }
    }
}

fn raft_loop(name: String, 
    close_flag: Arc<Mutex<bool>>,
    inbound_channel: mpsc::Receiver<messages::DataMessage>, 
    outbound_channel: mpsc::Sender<messages::DataMessage>, 
    log_channel: mpsc::Receiver<messages::LogMessage>,
    config: config::TimeoutConfig, 
    peers: Vec<String>) {

    let mut context = context::Context::new(name);

    let mut role = roles::Role::new_follower(&config, &mut context);

    loop {
        if *close_flag.lock().unwrap() {
            info!("Raft loop exiting!");
            break;
        }

        debug!("X: {:?}", context.persistent_state.log);

        loop {
            if let Ok(message) = log_channel.try_recv() {
                let (new_role, outbound_messages) = engine::log_handling::role_receive_log(role, message, &mut context);
                send_all_messages(outbound_messages, &outbound_channel);
                role = new_role;
            } else {
                break;
            }
        }

        loop {
            if let Ok(message) = inbound_channel.try_recv() {
                let (new_role, outbound_messages) = engine::message_handling::message_role(role, &message.raft_message, &message.peer, &config, &mut context);
                send_all_messages(outbound_messages, &outbound_channel);
                role = new_role;
            } else {
                break;
            }
        }

        let (new_role, outbound_messages) = engine::tick_handling::tick_role(role, &peers, &config, &mut context);
        send_all_messages(outbound_messages, &outbound_channel);
        role = new_role;

        thread::sleep(Duration::from_millis(1000));
    }
}

fn send_all_messages(messages: Vec<messages::DataMessage>, channel: &mpsc::Sender<messages::DataMessage>) {
    for message in messages {
        match channel.send(message) {
            Ok(_) => {},
            Err(message) => {
                panic!("Failed to send message: {:?}", message);
            }
        }
    }
}