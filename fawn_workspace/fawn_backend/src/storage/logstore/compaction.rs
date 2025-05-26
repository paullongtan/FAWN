use crate::segment::{SegmentReader};
use std::io;
use std::path::PathBuf;

pub fn run(
    dir: &PathBuf,
    sealed: &mut Vec<SegmentReader>,
) -> io::Result<()> {
    // copy live records into new segment(s), update sealed list
    todo!()
}