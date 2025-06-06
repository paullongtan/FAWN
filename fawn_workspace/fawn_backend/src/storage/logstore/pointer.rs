#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct RecordPtr {
    pub seg_id: u64,
    pub offset: u32,
}

impl RecordPtr {
    pub fn zero() -> Self {
        Self { seg_id: 0, offset: 0 }
    }
}

/// define a partial ordering for RecordPtr
/// first by segment ID, then by offset within that segment
impl PartialOrd for RecordPtr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// define a total ordering for RecordPtr
/// first by segment ID, then by offset within that segment
impl Ord for RecordPtr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.seg_id.cmp(&other.seg_id) {
            std::cmp::Ordering::Equal => self.offset.cmp(&other.offset),
            ord => ord,
        }
    }
}