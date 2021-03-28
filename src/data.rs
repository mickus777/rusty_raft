#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct LogPost {
    pub term: u64,
    pub value: i32
}

impl std::cmp::PartialEq for LogPost {
    fn eq(&self, other: &Self) -> bool {
        self.term == other.term && self.value == other.value
    }
}

impl std::fmt::Debug for LogPost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
        .field(&self.term)
        .field(&self.value)
        .finish()
    }
}

pub fn check_last_log_post(last_log_index: &Option<usize>, last_log_term: &Option<u64>, log: &Vec<LogPost>) -> bool {
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

// Append entries to the log starting at start_pos, skipping duplicates and dropping all leftover entries in the log
pub fn append_entries_from(log: &mut Vec<LogPost>, entries: &Vec<LogPost>, start_pos: &Option<usize>) {
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

pub fn get_log_range(from_index: &Option<usize>, log: &Vec<LogPost>) -> Vec<LogPost> {
    if let Some(start_index) = from_index {
        log[*start_index..].iter().cloned().collect()
    } else {
        log.iter().cloned().collect()
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
}
