use std::{fs, io::{Read, Write}, path::Path};

#[derive(Clone, Copy, Default)]
pub struct Meta {                // 16 bytes on disk
    pub last_ts:       u64,      // highest timestamp ever assigned
    pub last_acked_ts: u64,      // highest timestamp ACK-ed by the tail
}

impl Meta {
    pub fn load<P: AsRef<Path>>(p: P) -> std::io::Result<Self> {
        match fs::read(&p) {
            Ok(buf) if buf.len() == 16 => Ok(Self {
                last_ts:       u64::from_le_bytes(buf[0..8].try_into().unwrap()),
                last_acked_ts: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            }),
            _ => Ok(Self::default()),    // first boot → zeroes
        }
    }
    pub fn store<P: AsRef<Path>>(&self, p: P) -> std::io::Result<()> {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.last_ts.to_le_bytes());
        buf[8..16].copy_from_slice(&self.last_acked_ts.to_le_bytes());
        let mut f = fs::OpenOptions::new()
            .create(true).write(true).truncate(true).open(p)?;
        f.write_all(&buf)?;
        f.sync_all()                     // fsync metadata
    }
}