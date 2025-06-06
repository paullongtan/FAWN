use std::{fs, io::{Read, Write}, path::Path};
use crate::storage::logstore::RecordPtr;

#[derive(Clone, Copy, Default)]
pub struct Meta { // 24 bytes on disk
    pub last_acked_ptr: RecordPtr, // pointer to last acked record
    pub last_sent_ptr: RecordPtr, // pointer to last sent record (for pre-copy)
}

impl Meta {
    pub fn load<P: AsRef<Path>>(p: P) -> std::io::Result<Self> {
        match fs::read(&p) {
            Ok(buf) if buf.len() == 24 => Ok(Self {
                last_acked_ptr: RecordPtr {
                    seg_id: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
                    offset: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
                },
                last_sent_ptr: RecordPtr {
                    seg_id: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
                    offset: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
                },
            }),
            _ => Ok(Self::default()),    // first boot → zeroes
        }
    }
    pub fn store<P: AsRef<Path>>(&self, p: P) -> std::io::Result<()> {
        let mut buf = [0u8; 24];
        buf[0..8].copy_from_slice(&self.last_acked_ptr.seg_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.last_acked_ptr.offset.to_le_bytes());
        buf[12..20].copy_from_slice(&self.last_sent_ptr.seg_id.to_le_bytes());
        buf[20..24].copy_from_slice(&self.last_sent_ptr.offset.to_le_bytes());
        let mut f = fs::OpenOptions::new()
            .create(true).write(true).truncate(true).open(p)?;
        f.write_all(&buf)?;
        f.sync_all()
    }
}