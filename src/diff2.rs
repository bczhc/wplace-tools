//! Diff format 2.
//!
//! This removes any interior compression
//! and stores a full index of chunks, thus it enables chunk indexing.
//!
//! ## Format
//!
//! Magic (\[u8; 11\]) | IndexPos (u64) | EntryCount (u32) | [`Metadata`] | diff data... (\[u8\]) | [`IndexEntry`]...

use crate::ChunkNumber;
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use yeet_ops::yeet;

pub const MAGIC: [u8; 11] = *b"wplace-diff";

trait WriteTo {
    fn write_to(&self, w: impl Write) -> io::Result<()>;
}

trait ReadFrom
where
    Self: Sized,
{
    fn read_from(r: impl Read) -> io::Result<Self>;
}

/// ## Format
///
/// len(json([`Metadata`])) (u32) | json([`Metadata`])
#[derive(Default, Serialize, Deserialize)]
pub struct Metadata {}

/// ## Format
///
/// - if `diff_data_range` is [`DiffDataRange::Unchanged`]
///
///    [`ChunkFlag`] (u8) | n.x (u16) | n.y (u16) | checksum (u32)
///
/// - if `diff_data_range` is [`DiffDataRange::Changed`]
///
///    [`ChunkFlag`] (u8) | n.x (u16) | n.y (u16) | checksum (u32) | `diff_data_range.pos` (u64) | `diff_data_range.len` (u64)
#[derive(Debug)]
pub struct IndexEntry {
    pub n: ChunkNumber,
    pub diff_data_range: DiffDataRange,
    /// CRC checksum: [`CHUNK_CRC32`]
    pub checksum: u32,
}

#[derive(Debug, Default)]
pub enum DiffDataRange {
    #[default]
    Unchanged,
    Changed {
        pos: u64,
        len: u64,
    },
}

impl DiffDataRange {
    pub const fn is_changed(&self) -> bool {
        matches!(self, Self::Changed { .. })
    }
}

impl DiffDataRange {
    const fn to_flag(&self) -> ChunkFlag {
        match self {
            Self::Unchanged => ChunkFlag::Unchanged,
            Self::Changed { .. } => ChunkFlag::Changed,
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq, TryFromPrimitive)]
enum ChunkFlag {
    Unchanged = 0b00,
    Changed = 0b01,
}

pub struct DiffFile<R> {
    reader: R,
    pub index_pos: u64,
    pub entry_count: u32,
    pub metadata: Metadata,
}

impl DiffFile<()> {
    pub fn open_path(path: impl AsRef<Path>) -> anyhow::Result<DiffFile<BufReader<File>>> {
        DiffFile::open(File::open_buffered(path)?)
    }
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
        let metadata = Metadata::read_from(&mut reader)?;
        Ok(Self {
            reader,
            index_pos,
            entry_count,
            metadata,
        })
    }

    pub fn read_index(&mut self) -> anyhow::Result<HashMap<ChunkNumber, IndexEntry>> {
        let mut map = HashMap::new();
        self.reader.seek(SeekFrom::Start(self.index_pos))?;
        for _ in 0..self.entry_count {
            let entry = IndexEntry::read_from(&mut self.reader)?;
            map.insert(entry.n, entry);
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
    pub fn create(mut writer: W, metadata: Metadata) -> anyhow::Result<Self> {
        writer.write_all(&MAGIC)?;
        writer.write_u64::<LE>(0 /* placeholder: index offset */)?;
        writer.write_u32::<LE>(0 /* placeholder: entry count */)?;
        metadata.write_to(&mut writer)?;
        let diff_data_pos = writer.stream_position()?;

        let index_entries = HashMap::new();
        Ok(Self {
            writer,
            current_diff_data_pos: diff_data_pos,
            index_entries,
        })
    }

    #[inline(always)]
    pub fn add_entry(
        &mut self,
        n: ChunkNumber,
        compressed_diff_data: Option<&[u8]>,
        chunk_checksum: u32,
    ) -> anyhow::Result<()> {
        match compressed_diff_data {
            None => {
                self.index_entries.insert(
                    n,
                    IndexEntry {
                        n,
                        checksum: chunk_checksum,
                        diff_data_range: DiffDataRange::Unchanged,
                    },
                );
            }
            Some(data) => {
                self.index_entries.insert(
                    n,
                    IndexEntry {
                        n,
                        checksum: chunk_checksum,
                        diff_data_range: DiffDataRange::Changed {
                            pos: self.current_diff_data_pos,
                            len: data.len() as u64,
                        },
                    },
                );
                self.current_diff_data_pos += data.len() as u64;
                self.writer.write_all(data)?;
            }
        }
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
            x.write_to(&mut self.writer)?;
        }
        Ok(())
    }

    pub fn finalize(mut self) -> anyhow::Result<()> {
        self.write_final_index()?;
        Ok(())
    }
}

impl WriteTo for Metadata {
    fn write_to(&self, mut w: impl Write) -> io::Result<()> {
        let json = serde_json::to_string(self).unwrap();
        w.write_u32::<LE>(json.len().try_into().unwrap())?;
        w.write_all(json.as_bytes())?;
        Ok(())
    }
}

impl ReadFrom for Metadata {
    fn read_from(mut r: impl Read) -> std::io::Result<Self> {
        let json_len = r.read_u32::<LE>()? as usize;
        let mut buf = vec![0_u8; json_len];
        r.read_exact(&mut buf)?;
        serde_json::from_str(std::str::from_utf8(&buf).map_err(io::Error::other)?)
            .map_err(io::Error::from)
    }
}

impl WriteTo for IndexEntry {
    #[inline(always)]
    fn write_to(&self, mut w: impl Write) -> io::Result<()> {
        w.write_u8(self.diff_data_range.to_flag() as u8)?;
        w.write_u16::<LE>(self.n.0)?;
        w.write_u16::<LE>(self.n.1)?;
        w.write_u32::<LE>(self.checksum)?;
        if let DiffDataRange::Changed { pos, len } = self.diff_data_range {
            w.write_u64::<LE>(pos)?;
            w.write_u64::<LE>(len)?;
        }
        Ok(())
    }
}

impl ReadFrom for IndexEntry {
    #[inline(always)]
    fn read_from(mut r: impl Read) -> io::Result<Self> {
        let flag = r.read_u8()?;
        let flag = ChunkFlag::try_from(flag).map_err(io::Error::other)?;
        let cx = r.read_u16::<LE>()?;
        let cy = r.read_u16::<LE>()?;
        let checksum = r.read_u32::<LE>()?;
        match flag {
            ChunkFlag::Unchanged => Ok(Self {
                n: (cx, cy),
                checksum,
                diff_data_range: DiffDataRange::Unchanged,
            }),
            ChunkFlag::Changed => {
                let pos = r.read_u64::<LE>()?;
                let len = r.read_u64::<LE>()?;
                Ok(Self {
                    n: (cx, cy),
                    checksum,
                    diff_data_range: DiffDataRange::Changed { pos, len },
                })
            }
        }
    }
}
