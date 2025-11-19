use crate::metadata::{Entry, List};
use anyhow::Result;
use dlv_list::Index;
use std::collections::HashMap;

pub struct Lru {
    pub list: List<u64>, // id is 1
}

impl Lru {
    pub fn new(maxsize: usize) -> Lru {
        Lru {
            list: List::new(maxsize),
        }
    }

    pub fn insert(&mut self, key: u64, entry: &mut Entry) {
        let index = self.list.insert_front(key);
        entry.policy_list_index = Some(index);
        entry.policy_list_id = 1;
    }

    pub fn access(&mut self, index: Index<u64>) {
        self.list.touch(index)
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn remove(&mut self, entry: &Entry) -> Result<()> {
        if let Some(index) = entry.policy_list_index {
            self.list.remove(index);
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "LRU remove: missing policy_list_index for entry, this indicates a bug"
            ))
        }
    }
}

pub struct Slru {
    pub probation: List<u64>,
    pub protected: List<u64>,
}

impl Slru {
    pub fn new(maxsize: usize) -> Slru {
        let protected_cap = (maxsize as f64 * 0.8) as usize;
        Slru {
            probation: List::new(maxsize),
            protected: List::new(protected_cap),
        }
    }

    pub fn insert(&mut self, key: u64, entry: &mut Entry) {
        let index = self.probation.insert_front(key);
        entry.policy_list_index = Some(index);
        entry.policy_list_id = 2;
    }

    pub fn access(&mut self, key: u64, entries: &mut HashMap<u64, Entry>) -> Result<()> {
        if let Some(entry) = entries.get_mut(&key) {
            match entry.policy_list_id {
                2 => {
                    if let Some(index) = entry.policy_list_index {
                        self.probation.remove(index);
                        let new_index = self.protected.insert_front(key);
                        entry.policy_list_index = Some(new_index);
                        entry.policy_list_id = 3;
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!(
                            "SLRU access: missing policy_list_index for probation entry {}, this indicates a bug",
                            key
                        ))
                    }
                }
                3 => {
                    if let Some(index) = entry.policy_list_index {
                        self.protected.touch(index);
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!(
                            "SLRU access: missing policy_list_index for protected entry {}, this indicates a bug",
                            key
                        ))
                    }
                }
                _ => unreachable!(),
            }
        } else {
            Ok(()) // Entry not found is not an error in this context
        }
    }

    pub fn remove(&mut self, entry: &Entry) -> Result<()> {
        if let Some(list_index) = entry.policy_list_index {
            match entry.policy_list_id {
                2 => self.probation.remove(list_index),
                3 => self.protected.remove(list_index),
                _ => unreachable!(),
            };
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "SLRU remove: missing policy_list_index for entry with policy_list_id {}, this indicates a bug",
                entry.policy_list_id
            ))
        }
    }
}
