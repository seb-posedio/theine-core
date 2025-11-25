use dlv_list::{Index, Iter, VecList};
use log;

/// Entry represents a cached item with metadata about its position in various data structures.
///
/// Fields:
/// - policy_list_id: Which list the entry belongs to (0=not in policy, 1=window/lru, 2=probation, 3=protected)
/// - policy_list_index: Position in the policy list (window, probation, or protected)
/// - wheel_list_index: Position in the timer wheel for TTL expiration
/// - wheel_index: Which bucket in the timer wheel (level, slot)
/// - expire: Expiration time in nanoseconds (0 = no expiration)
#[derive(Debug, Clone)]
pub struct Entry {
    pub policy_list_id: u8,
    pub policy_list_index: Option<Index<u64>>,
    pub wheel_list_index: Option<Index<u64>>,
    pub wheel_index: (u8, u8),
    pub expire: u64,
}

impl Default for Entry {
    fn default() -> Self {
        Entry::new()
    }
}

impl Entry {
    pub fn new() -> Self {
        Self {
            policy_list_index: None,
            wheel_list_index: None,
            wheel_index: (0, 0),
            expire: 0,
            policy_list_id: 0,
        }
    }

    /// Validate that the entry's metadata is consistent
    pub fn validate(&self) -> Result<(), String> {
        // Policy list ID must be in valid range [0-3]
        if self.policy_list_id > 3 {
            return Err(format!(
                "Invalid policy_list_id: {}, must be in range [0-3]",
                self.policy_list_id
            ));
        }

        // If policy_list_id is 0, indices should be None
        if self.policy_list_id == 0 {
            if self.policy_list_index.is_some() {
                return Err(
                    "Entry with policy_list_id=0 should not have policy_list_index set".to_string(),
                );
            }
        } else {
            // If policy_list_id is 1-3, index should be Some
            if self.policy_list_index.is_none() {
                return Err(format!(
                    "Entry with policy_list_id={} should have policy_list_index set",
                    self.policy_list_id
                ));
            }
        }

        // Timer wheel indices should be valid ranges
        if self.wheel_index.0 > 4 {
            return Err(format!(
                "Invalid wheel level: {}, must be in range [0-4]",
                self.wheel_index.0
            ));
        }

        // If expire is set, wheel_list_index should be Some
        if self.expire > 0 && self.wheel_list_index.is_none() {
            return Err("Entry with expire time should have wheel_list_index set".to_string());
        }

        Ok(())
    }

    /// Check if entry is expired given current time
    pub fn is_expired(&self, now_ns: u64) -> bool {
        self.expire > 0 && self.expire <= now_ns
    }
}

#[derive(Debug)]
pub struct List<T> {
    pub list: VecList<T>,
    pub capacity: usize,
}

impl<T> List<T> {
    pub fn new(capacity: usize) -> Self {
        let capacity = if capacity == 0 { 1 } else { capacity };
        log::debug!("List created with capacity={}", capacity);
        Self {
            capacity,
            list: VecList::with_capacity(capacity),
        }
    }

    /// Remove entry at index from list
    ///
    /// # Note
    /// This operation is safe - dlv_list handles invalid indices gracefully
    pub fn remove(&mut self, index: Index<T>) {
        self.list.remove(index);
    }

    /// Insert entry to list front and return its index
    ///
    /// Maintains the invariant that newly inserted items are at the front
    pub fn insert_front(&mut self, entry: T) -> Index<T> {
        if let Some(index) = self.list.front_index() {
            self.list.insert_before(index, entry)
        } else {
            // no front entry, list is empty
            self.list.push_front(entry)
        }
    }

    /// Get tail entry, return None if empty
    pub fn tail(&self) -> Option<&T> {
        self.list.back()
    }

    /// Returns the value previous to the value at the given index
    pub fn prev(&self, index: Index<T>) -> Option<&T> {
        if let Some(prev) = self.list.get_previous_index(index) {
            self.list.get(prev)
        } else {
            None
        }
    }

    /// Remove tail entry from list
    pub fn pop_tail(&mut self) -> Option<T> {
        self.list.pop_back()
    }

    /// Move entry to front of list
    ///
    /// Only moves if the entry is not already at front to avoid unnecessary operations
    pub fn touch(&mut self, index: Index<T>) {
        if let Some(front) = self.list.front_index()
            && front != index
        {
            self.list.move_before(index, front);
        }
    }

    /// Iterate over list entries
    pub fn iter(&self) -> Iter<'_, T> {
        self.list.iter()
    }

    /// Get current number of entries in list
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Check if list is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Clear all entries from the list
    pub fn clear(&mut self) {
        self.list.clear();
        log::debug!("List cleared");
    }
}
