use crc_fast::CrcAlgorithm;

#[inline(always)]
pub fn chunk_checksum(data: &[u8]) -> u32 {
    crc_fast::checksum(CrcAlgorithm::Crc32Cksum, data) as _
}
