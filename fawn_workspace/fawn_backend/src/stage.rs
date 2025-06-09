use std::{fs, io::{Read, Write}, path::Path};

#[derive(Clone, Copy, Debug)]
pub enum Stage {
    TempMember,
    Normal,
}

impl Stage {
    pub fn load<P: AsRef<Path>>(p: P) -> std::io::Result<Self> {
        let mut buf = [0u8; 1];
        match fs::File::open(&p).and_then(|mut f| f.read_exact(&mut buf)) {
            Ok(_) => match buf[0] {
                0 => Ok(Stage::Normal),
                1 => Ok(Stage::TempMember),
                _ => Ok(Stage::Normal), // fallback/default
            },
            Err(_) => Ok(Stage::Normal), // default on first boot or missing file
        }
    }

    pub fn store<P: AsRef<Path>>(&self, p: P) -> std::io::Result<()> {
        let val = match self {
            Stage::Normal => 0u8,
            Stage::TempMember => 1u8,
        };
        let mut f = fs::OpenOptions::new()
            .create(true).write(true).truncate(true).open(p)?;
        f.write_all(&[val])?;
        f.sync_all()
    }
}