//! Encode / decode individual on-disk records.

use std::io::{self, Read, Write};

pub const FIXED_HDR: usize = 4 + 2 + 1 + 1 + 4; // rec_len + key_len + flags + reserved + key_hash32

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum RecordFlags {
    Put     = 0x01,  // mark the record as a key-value pair (an insertion or update)
    Delete  = 0x02,  // mark the record as a tombstone (a deletion)
    // COMPRESSED = 0x04,
}

pub struct Record<'a> {
    pub flags:  RecordFlags, // mark the operation type of this record
    pub key:    &'a [u8],    // key for this record (actual key used for lookup)
    pub value:  &'a [u8],    // value for this record
    pub hash32: u32,         // upper 32 bits of SHA-256 hash of the key, big-endian (hash value for human-readable key) (e.g., fileA -> 0x12345678)
}

impl<'a> Record<'a> {
    /// Return total encoded length.
    pub fn encoded_len(&self) -> usize {
        FIXED_HDR + self.key.len() + self.value.len() + 4 /*crc32*/
    }

    /// Write encoded bytes to `w`.
    /// a record is serialized as:
    ///  - [rec_len][key_len][flags][reserved][hash32][key][value][crc32] in little-endian
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        use byteorder::WriteBytesExt;

        let rec_len = (self.encoded_len() - 4) as u32; // exclude rec_len itself
        // write the fixed-length header
        w.write_u32::<byteorder::LittleEndian>(rec_len)?;
        w.write_u16::<byteorder::LittleEndian>(self.key.len() as u16)?;
        w.write_all(&[self.flags as u8, 0])?; // reserved
        w.write_u32::<byteorder::LittleEndian>(self.hash32)?;
        // write the variable-length parts
        w.write_all(self.key)?;
        w.write_all(self.value)?;

        // compute a CRC32 checksum over the entire record except the first 4 bytes (rec_len)
        // to ensure any corruption is detected
        let mut hasher = crc32fast::Hasher::new();
        {
            // build once in a scratch vec; cheaper than updating per field
            let mut tmp = Vec::with_capacity(rec_len as usize);
            tmp.write_u16::<byteorder::LittleEndian>(self.key.len() as u16)?;
            tmp.extend_from_slice(&[self.flags as u8, 0]);
            tmp.write_u32::<byteorder::LittleEndian>(self.hash32)?;
            tmp.extend_from_slice(self.key);
            tmp.extend_from_slice(self.value);
            hasher.update(&tmp);
        }
        w.write_u32::<byteorder::LittleEndian>(hasher.finalize())?;
        Ok(())
    }

    /// Read a record from `r` at the current cursor.
    pub fn decode<R: Read>(r: &mut R) -> io::Result<Self> {
        use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

        let rec_len = r.read_u32::<LittleEndian>()? as usize;
        let key_len = r.read_u16::<LittleEndian>()? as usize;
        let flags_u8  = r.read_u8()?;
        let _reserved = r.read_u8()?;
        let hash32    = r.read_u32::<LittleEndian>()?;

        // read the variable parts
        let mut key = vec![0; key_len];
        r.read_exact(&mut key)?;
        let value_len = rec_len as usize - (key_len + FIXED_HDR - 4 /*rec_len*/ + 4 /*crc32*/);
        let mut value = vec![0; value_len];
        r.read_exact(&mut value)?;

        // recompute the CRC32 checksum over the entire record except the first 4 bytes (rec_len)
        let expected_crc = r.read_u32::<LittleEndian>()?;
        let mut hasher = crc32fast::Hasher::new();
        {
            // build once in a scratch vec; cheaper than updating per field
            let mut tmp = Vec::with_capacity(rec_len);
            tmp.write_u16::<LittleEndian>(key_len as u16)?;
            tmp.extend_from_slice(&[flags_u8, 0]);
            tmp.write_u32::<LittleEndian>(hash32)?;
            tmp.extend_from_slice(&key);
            tmp.extend_from_slice(&value);
            hasher.update(&tmp);
        }
        if hasher.finalize() != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CRC32 mismatch",
            ));
        }
        
        Ok(Record {
            flags:  match flags_u8 {
                0x01 => RecordFlags::Put,
                0x02 => RecordFlags::Delete,
                _     => panic!("Invalid record flag"),
            },
            key: Box::leak(key.into_boxed_slice()),
            value: Box::leak(value.into_boxed_slice()),
            hash32,
        })
    }
}

/// Hash helper (upper 32 bits of SHA-256 by default).
/// compute the SHA-256 hash of the input key and return the first 4 bytes as a u32
/// (the hash is in big-endian order)
pub fn hash32(key: &[u8]) -> u32 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(key);
    u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]])
}