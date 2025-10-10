use crate::{CHUNK_LENGTH, ChunkNumber};
use blake3::Hash;
use byteorder::{ByteOrder, LE};
use std::cmp::Ordering;
use crc::Crc;

#[derive(Default)]
pub struct Checksum {
    chunks_hash_list: Vec<(ChunkNumber, Hash)>,
}

impl Checksum {
    pub fn new() -> Self {
        Default::default()
    }

    #[inline(always)]
    pub fn add_chunk(&mut self, n: ChunkNumber, data: &[u8]) {
        assert_eq!(data.len(), CHUNK_LENGTH);
        let hash = blake3::hash(data);
        self.chunks_hash_list.push((n, hash));
    }

    pub fn compute(mut self) -> Hash {
        self.chunks_hash_list
            .sort_by(|(a, _), (b, _)| match a.0.cmp(&b.0) {
                Ordering::Equal => a.1.cmp(&b.1),
                o => o,
            });
        let mut hasher = blake3::Hasher::new();
        let mut chunk_num_buf = [0_u8; 4];
        for (n, hash) in self.chunks_hash_list {
            LE::write_u16(&mut chunk_num_buf[..2], n.0);
            LE::write_u16(&mut chunk_num_buf[2..], n.1);
            hasher.update(&chunk_num_buf);
            hasher.update(hash.as_bytes());
        }
        hasher.finalize()
    }
}

pub static CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_CKSUM);
