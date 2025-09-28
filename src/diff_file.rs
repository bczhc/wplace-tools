//! Standalone diff file that contains all the chunk changes.
//!
//! ## File Format
//!
//! \[ [`MAGIC`] | [`Metadata`] | [`ArchiveIndex`] | Compressed diff stream \]
//!
//! **diff stream:**
//!
//!   \[ chunk0_x (u16) | chunk1_y (u16) | diff_data_length (u32) | diff_data (\[u8; diff_data_length\])
//!   | chunk1_x (u16) | chunk2_y (u16) | diff_data_length (u32) | diff_data (\[u8; diff_data_length\])
//!   | ...
//!   | chunkN_x (u16) | chunkN_y (u16) | diff_data_length (u32) | diff_data (\[u8; diff_data_length\]) \]
//!
//! `diff_data` then is also compressed. It expands to: `[0_u8; 1_000_000]`.
//!
//! ## Synopsis
//!
//! ```text
//! File Format
//! └── [ Magic ]
//! └── [ Metadata ]
//!     ├── diff_count : u32
//!     ├── name_length : u32
//!     ├── name : [u8; name_length]
//!     ├── parent_length : u32
//!     ├── parent : [u8; parent_length]
//!     └── creation_time : u64
//! └── [ ArchiveIndex ]
//!     ├── entry_count : u32
//!     ├── compressed_data_length : u32
//!     └── compressed_data : [u8; compressed_data_length]
//!         ├── chunk0_x : u16
//!         ├── chunk0_y : u16
//!         ├── chunk1_x : u16
//!         ├── chunk1_y : u16
//!         ├── ...
//!         ├── chunkN_x : u16
//!         └── chunkN_y : u16
//! └── [ Compressed diff stream ]
//!     ├── chunk0_x : u16
//!     ├── chunk1_y : u16
//!     ├── diff_data_length : u32
//!     ├── diff_data : [u8; diff_daa_length]
//!     ├── chunk1_x : u16
//!     ├── chunk2_y : u16
//!     ├── diff_data_length : u32
//!     ├── diff_data : [u8; diff_data_length]
//!     ├── ...
//!     ├── chunkN_x : u16
//!     ├── chunkN_y : u16
//!     ├── diff_data_length : u32
//!     └── diff_data : [u8; diff_data_length]
//! ```
//!
//! All integer serializations are in little-endian. All compressions are using `flate2::*::Deflate(Encoder|Decoder)`.

use crate::ChunkNumber;
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use flate2::{Compression, read, write};
use static_assertions::const_assert_eq;
use std::io;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread::spawn;
use yeet_ops::yeet;

pub const MAGIC: [u8; 11] = *b"wplace-diff";

const_assert_eq!(blake3::OUT_LEN, 32);
pub type ChecksumHash = [u8; blake3::OUT_LEN];

/// Metadata of a diff file.
///
/// ## Serialization format
///
/// \[ diff_count (u32) | name_length (u32) | name (var-length) | parent_length (u32) | name (var-length) | creation_time (u64) \]
#[derive(Clone,Debug)]
pub struct Metadata {
    /// Number of chunks changed
    pub diff_count: u32,
    /// Checksum of the original archive.
    pub checksum: ChecksumHash,
    pub name: String,
    pub parent: String,
    pub creation_time: u64,
}

const DIFF_COUNT_OFFSET: u64 = MAGIC.len() as u64;

/// An assembled diff file that saves all the chunk changes.
pub struct DiffFileWriter<W: Write + Seek> {
    compressor: write::DeflateEncoder<W>,
}

impl<W> DiffFileWriter<W>
where
    W: Write + Seek,
{
    pub fn new(
        mut writer: W,
        metadata: Metadata,
        archive_index: impl Into<Vec<ChunkNumber>>,
    ) -> anyhow::Result<Self> {
        writer.write_all(&MAGIC)?;
        metadata.write_to(&mut writer)?;
        ArchiveIndex(archive_index.into()).write_to(&mut writer)?;

        let compressor = write::DeflateEncoder::new(writer, Compression::default());
        Ok(Self { compressor })
    }

    #[inline(always)]
    /// This is only safe in a single thread.
    pub fn add_chunk_diff(&mut self, n: ChunkNumber, data: &[u8]) -> anyhow::Result<()> {
        self.compressor.write_u16::<LE>(n.0)?;
        self.compressor.write_u16::<LE>(n.1)?;
        self.compressor.write_u32::<LE>(data.len() as u32)?;
        self.compressor.write_all(data)?;
        Ok(())
    }

    pub fn finish(self, diff_count: u32, checksum: ChecksumHash) -> io::Result<()> {
        let mut w = self.compressor.finish()?;
        w.seek(SeekFrom::Start(DIFF_COUNT_OFFSET))?;
        w.write_u32::<LE>(diff_count)?;
        w.write_all(&checksum)?;
        Ok(())
    }
}

pub struct DiffFileReader<R: Read> {
    decompressor: read::DeflateDecoder<R>,
    pub index: Vec<ChunkNumber>,
    pub metadata: Metadata,
}

impl<R> DiffFileReader<R>
where
    R: Read + Send + 'static,
{
    pub fn new(mut reader: R) -> anyhow::Result<Self> {
        let mut magic_buf = [0_u8; MAGIC.len()];
        reader.read_exact(&mut magic_buf)?;
        if magic_buf != MAGIC {
            yeet!(anyhow::anyhow!("Invalid magic number"));
        }

        let metadata = Metadata::read_from(&mut reader)?;
        let index: Vec<ChunkNumber> = ArchiveIndex::read_from(&mut reader)?.0;

        let reader = read::DeflateDecoder::new(reader);
        Ok(Self {
            decompressor: reader,
            metadata,
            index,
        })
    }

    pub fn chunk_diff_iter(self) -> Receiver<io::Result<(ChunkNumber, Vec<u8>)>> {
        let (tx, rx) = sync_channel(1024);

        spawn(move || {
            let mut reader = self.decompressor;
            for _ in 0..self.metadata.diff_count {
                let result: io::Result<_> = try {
                    let x = reader.read_u16::<LE>()?;
                    let y = reader.read_u16::<LE>()?;
                    let data_len = reader.read_u32::<LE>()?;
                    let mut buf = vec![0_u8; data_len as usize];
                    reader.read_exact(&mut buf)?;
                    ((x, y), buf)
                };
                match result {
                    Err(e) => tx.send(Err(e)).unwrap(),
                    Ok(x) => {
                        tx.send(Ok(x)).unwrap();
                    }
                }
            }
        });

        rx
    }
}

trait WriteTo {
    fn write_to(&self, w: impl Write) -> io::Result<()>;
}

trait ReadFrom
where
    Self: Sized,
{
    fn read_from(r: impl Read) -> io::Result<Self>;
}

impl WriteTo for Metadata {
    fn write_to(&self, mut w: impl Write) -> io::Result<()> {
        w.write_u32::<LE>(self.diff_count)?;
        w.write_all(&self.checksum)?;
        w.write_u64::<LE>(self.creation_time)?;
        self.parent.write_to(&mut w)?;
        self.name.write_to(&mut w)?;
        Ok(())
    }
}

impl ReadFrom for Metadata {
    fn read_from(mut r: impl Read) -> io::Result<Self> {
        let diff_count = r.read_u32::<LE>()?;
        let mut checksum = [0_u8; blake3::OUT_LEN];
        r.read_exact(&mut checksum)?;
        let creation_time = r.read_u64::<LE>()?;
        let parent = String::read_from(&mut r)?;
        let name = String::read_from(&mut r)?;
        Ok(Self {
            diff_count,
            checksum,
            creation_time,
            parent,
            name,
        })
    }
}

impl WriteTo for String {
    fn write_to(&self, mut w: impl Write) -> io::Result<()> {
        w.write_u16::<LE>(self.len().try_into().expect("too long"))?;
        w.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl ReadFrom for String {
    fn read_from(mut r: impl Read) -> io::Result<Self> {
        let len = r.read_u16::<LE>()?;
        let mut buf = vec![0_u8; len as usize];
        r.read_exact(&mut buf)?;
        Ok(String::from_utf8(buf).expect("Invalid UTF-8 string"))
    }
}

/// ## Serialization format
///
/// \[ entry count (u32) | compressed data length (u32) | compressed data (var-length) \]
///
/// **Compressed data expands to:**
///
/// \[ chunk0_x (u16) | chunk0_y (u16) | chunk1_x (u16) | chunk1_y (u16) | ... | chunkN_x (u16) | chunkN_y (u16) \]
#[repr(transparent)]
struct ArchiveIndex(Vec<ChunkNumber>);

impl WriteTo for ArchiveIndex {
    fn write_to(&self, mut w: impl Write) -> io::Result<()> {
        let mut compressed = Cursor::new(Vec::new());
        let mut compressor = write::DeflateEncoder::new(&mut compressed, Compression::default());
        for x in &self.0 {
            compressor.write_u16::<LE>(x.0)?;
            compressor.write_u16::<LE>(x.1)?;
        }
        drop(compressor);

        w.write_u32::<LE>(self.0.len() as u32)?;
        w.write_u32::<LE>(compressed.get_ref().len() as u32)?;
        w.write_all(compressed.get_ref())?;
        Ok(())
    }
}

impl ReadFrom for ArchiveIndex {
    fn read_from(mut r: impl Read) -> io::Result<Self> {
        let length = r.read_u32::<LE>()?;
        let compressed_data_length = r.read_u32::<LE>()?;
        let mut buf = vec![0_u8; compressed_data_length as usize];
        r.read_exact(&mut buf)?;

        let mut de = read::DeflateDecoder::new(Cursor::new(buf));
        let mut list = vec![Default::default(); length as usize];
        for e in list.iter_mut() {
            let x = de.read_u16::<LE>()?;
            let y = de.read_u16::<LE>()?;
            *e = (x, y);
        }
        Ok(Self(list))
    }
}
