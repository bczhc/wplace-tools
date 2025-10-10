//! Diff format 2

use crate::ChunkNumber;
use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use yeet_ops::yeet;
use crate::checksum::CRC32;

pub const MAGIC: [u8; 11] = crate::diff_file::MAGIC;

#[derive(Debug)]
pub struct IndexEntry {
    n: ChunkNumber,
    diff_data_range: DiffDataRange,
    /// CRC checksum: [`crate::checksum::CRC32`]
    checksum: u32,
}

macro placeholder() {
    Default::default()
}

#[derive(Debug, Default)]
pub enum DiffDataRange {
    #[default]
    Unchanged,
    Changed { pos: u64, len: u64 },
}

impl DiffDataRange {
    fn to_flag(&self) -> ChunkFlag {
        match self {
            DiffDataRange::Unchanged => ChunkFlag::Unchanged,
            DiffDataRange::Changed { .. } => ChunkFlag::Changed,
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq)]
enum ChunkFlag {
    Unchanged = 0b00,
    Changed = 0b01,
}

pub struct DiffFile<R: Read + Seek> {
    reader: R,
    pub index_pos: u64,
    pub entry_count: u32,
}

impl<R: Read + Seek> DiffFile<R> {
    pub fn open(mut reader: R) -> anyhow::Result<Self> {
        let mut magic = [0_u8; MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if magic != MAGIC {
            yeet!(anyhow::anyhow!("Magic error"));
        }

        let index_pos = reader.read_u64::<LE>()?;
        let entry_count = reader.read_u32::<LE>()?;
        Ok(Self {
            reader,
            index_pos,
            entry_count,
        })
    }

    pub fn read_index(&mut self) -> anyhow::Result<HashMap<ChunkNumber, DiffDataRange>> {
        let mut map = HashMap::new();
        self.reader.seek(SeekFrom::Start(self.index_pos))?;
        let mut buf = [0_u8; 3];
        for _ in 0..self.entry_count {
            self.reader.read_exact(&mut buf)?;
            let unpack = IndexEntry::unpack(buf);
            let diff_data_offset = if unpack.1 == ChunkFlag::Changed {
                let pos = self.reader.read_u64::<LE>()?;
                let len = self.reader.read_u64::<LE>()?;
                DiffDataRange::Changed { pos, len }
            } else {
                DiffDataRange::Unchanged
            };
            map.insert(unpack.0, diff_data_offset);
        }
        Ok(map)
    }
}

pub struct DiffFileWriter<W: Write + Seek> {
    writer: W,
    current_diff_data_pos: u64,
    index_entries: HashMap<ChunkNumber, IndexEntry>,
}

const INDEX_OFFSET_POS: u64 = MAGIC.len() as u64;

impl<W: Write + Seek> DiffFileWriter<W> {
    pub fn create(mut writer: W, chunks: Vec<ChunkNumber>) -> anyhow::Result<Self> {
        writer.write_all(&MAGIC)?;
        writer.write_u64::<LE>(0 /* placeholder: index offset */)?;
        writer.write_u32::<LE>(0 /* placeholder: entry count */)?;
        let diff_data_pos = writer.stream_position()?;

        let mut index_entries = HashMap::new();
        for x in chunks {
            index_entries.insert(
                x,
                IndexEntry {
                    n: x,
                    diff_data_range: placeholder!(),
                    checksum: placeholder!(),
                },
            );
        }
        Ok(Self {
            writer,
            current_diff_data_pos: diff_data_pos,
            index_entries,
        })
    }

    #[inline(always)]
    pub fn add_diff(&mut self, n: ChunkNumber, compressed_diff_data: &[u8]) -> anyhow::Result<()> {
        let checksum = CRC32.checksum(compressed_diff_data);

        let v = self.index_entries.get_mut(&n).unwrap();
        v.diff_data_range = DiffDataRange::Changed {
            pos: self.current_diff_data_pos,
            len: compressed_diff_data.len() as u64,
        };
        self.current_diff_data_pos += compressed_diff_data.len() as u64;
        self.writer.write_all(compressed_diff_data)?;
        Ok(())
    }

    fn write_final_index(&mut self) -> anyhow::Result<()> {
        let index_offset = self.writer.stream_position()?;
        self.writer.seek(SeekFrom::Start(INDEX_OFFSET_POS))?;
        self.writer.write_u64::<LE>(index_offset)?;
        self.writer
            .write_u32::<LE>(self.index_entries.len().try_into().unwrap())?;
        self.writer.seek(SeekFrom::Start(index_offset))?;
        // now write archive index
        for x in self.index_entries.values() {
            self.writer.write_all(&x.pack())?;
            if let DiffDataRange::Changed { pos, len } = x.diff_data_range {
                self.writer.write_u64::<LE>(pos)?;
                self.writer.write_u64::<LE>(len)?;
            }
        }
        Ok(())
    }

    pub fn finalize(mut self) -> anyhow::Result<()> {
        self.write_final_index()?;
        Ok(())
    }
}

impl IndexEntry {
    #[inline(always)]
    fn pack(&self) -> [u8; 3] {
        let n = self.n;
        assert!(n.0 < 2048 && n.1 < 2048);

        let flag = self.diff_data_range.to_flag() as u32;
        let x = n.0 as u32;
        let y = n.1 as u32;

        let packed: u32 = (flag << 22) | (x << 11) | y;

        [
            ((packed >> 16) & 0xFF) as u8,
            ((packed >> 8) & 0xFF) as u8,
            (packed & 0xFF) as u8,
        ]
    }

    #[inline(always)]
    fn unpack(bytes: [u8; 3]) -> (ChunkNumber, ChunkFlag) {
        let packed = ((bytes[0] as u32) << 16) | ((bytes[1] as u32) << 8) | (bytes[2] as u32);

        let flag = ((packed >> 22) & 0b11) as u8;
        let x = ((packed >> 11) & 0x7FF) as u16;
        let y = (packed & 0x7FF) as u16;

        (
            (x, y),
            match flag {
                0b00 => ChunkFlag::Unchanged,
                0b01 => ChunkFlag::Changed,
                _ => unreachable!(),
            },
        )
    }
}
