use crate::data;

pub fn top(list: &Vec<data::LogPost>) -> Option<&data::LogPost> {
    match list.len() {
        0 => None,
        n => Some(&list[n-1])
    }
}

