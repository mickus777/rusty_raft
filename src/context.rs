use crate::data;

pub struct Context {
    pub name: String,
    pub random: rand::rngs::ThreadRng,
    pub persistent_state: PersistentState,
    pub volatile_state: VolatileState
}

pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: Vec<data::LogPost>
}

pub struct VolatileState {
    pub commit_index: Option<usize>,
    pub last_applied: Option<usize>
}

impl Context {
    pub fn new(name: String) -> Context {
        Context { 
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
        }
    }
}
