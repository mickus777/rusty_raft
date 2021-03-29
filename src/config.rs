use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub election_timeout_length: u64,
    pub idle_timeout_length: u64,

    pub peers: HashMap<String, String>
}

pub struct TimeoutConfig {
    pub election_timeout_length: u64,
    pub idle_timeout_length: u64
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

impl Config {
    pub fn new() -> Result<(TimeoutConfig, HashMap<String, String>), String> {
        let conf : Result<(Config, HashMap<String, String>), confy::ConfyError> = confy::load("rusty_raft");

        match conf {
            Ok((config, peers)) => {
                Ok((TimeoutConfig {
                    election_timeout_length: config.election_timeout_length,
                    idle_timeout_length: config.idle_timeout_length
                }, peers))
            },
            Err(err) => {
                Err(err.to_string())
            }
        }
    }
}