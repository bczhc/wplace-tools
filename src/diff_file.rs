//! Diff file format:
//!
//! \[Magic | Metadata | ArchiveIndex | ChunkDiff 1 | ChunkDiff 2 | ... | ChunkDiff N \]

use crate::{CHUNK_LENGTH, ChunkNumber};
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
use flate2::{Compression, read, write};
use std::io;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread::spawn;
use yeet_ops::yeet;

pub const MAGIC: [u8; 11] = *b"wplace-diff";

pub struct Metadata {
    /// Number of chunks changed
    pub diff_count: u32,
    pub name: String,
    pub parent: String,
    pub creation_time: u64,
}

const DIFF_COUNT_OFFSET: u64 = MAGIC.len() as u64;

/// An assembled diff file that saves all the chunk changes.
pub struct DiffFileWriter<W: Write + Seek> {
    compressor: DoubleCompressor<W>,
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
        println!("{:?}", writer.stream_position());
        metadata.write_to(&mut writer)?;
        println!("{:?}", writer.stream_position());
        ChunkNumbers(archive_index.into()).write_to(&mut writer)?;
        println!("{:?}", writer.stream_position());

        // all chunk diffs are compressed
        let compressor = DoubleCompressor::new(writer);
        Ok(Self { compressor })
    }

    #[inline(always)]
    /// This is only safe in a single thread.
    pub fn add_chunk_diff(&mut self, n: ChunkNumber, data: &[u8]) -> anyhow::Result<()> {
        assert_eq!(data.len(), CHUNK_LENGTH);
        self.compressor.write_u16::<LE>(n.0)?;
        self.compressor.write_u16::<LE>(n.1)?;
        self.compressor.write_all(data)?;
        Ok(())
    }

    pub fn finish(self, diff_count: u32) -> io::Result<()> {
        let mut w = self.compressor.finish()?;
        w.seek(SeekFrom::Start(DIFF_COUNT_OFFSET))?;
        w.write_u32::<LE>(diff_count)?;
        Ok(())
    }
}

const CHUNK_DIFF_SIZE: usize = 2 /* size of ChunkNumber a.k.a. u16 */ + 2 + CHUNK_LENGTH;

pub struct DiffFileReader<R: Read> {
    decompressor: DoubleDecompressor<R>,
    pub index: Vec<ChunkNumber>,
    pub metadata: Metadata,
}

impl<R> DiffFileReader<R>
where
    R: Read + Send + 'static + Seek,
{
    pub fn new(mut reader: R) -> anyhow::Result<Self> {
        let mut magic_buf = [0_u8; MAGIC.len()];
        reader.read_exact(&mut magic_buf)?;
        if magic_buf != MAGIC {
            yeet!(anyhow::anyhow!("Invalid magic number"));
        }

        let metadata = Metadata::read_from(&mut reader)?;
        let index: Vec<ChunkNumber> = ChunkNumbers::read_from(&mut reader)?.0;

        println!("{:?}", reader.stream_position());

        let reader = DoubleDecompressor::new(reader);
        Ok(Self {
            decompressor: reader,
            metadata,
            index,
        })
    }

    pub fn chunk_diff_iter(self) -> anyhow::Result<Receiver<io::Result<Vec<u8>>>> {
        let (tx, rx) = sync_channel(8192);

        spawn(move || {
            let mut reader = self.decompressor;
            for _ in 0..self.metadata.diff_count {
                let mut buf = vec![0_u8; CHUNK_DIFF_SIZE];
                let result = reader.read_exact(&mut buf);
                match result {
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(e) => tx.send(Err(e)).unwrap(),
                    Ok(()) => {
                        tx.send(Ok(buf)).unwrap();
                    }
                }
            }
        });

        Ok(rx)
    }
}

struct DoubleCompressor<W: Write> {
    inner: write::DeflateEncoder<write::DeflateEncoder<W>>,
}

impl<W: Write> DoubleCompressor<W> {
    #[inline(always)]
    pub fn new(writer: W) -> Self {
        let compressor = write::DeflateEncoder::new(writer, Compression::default());
        let compressor = write::DeflateEncoder::new(compressor, Compression::default());
        Self { inner: compressor }
    }

    pub fn finish(self) -> io::Result<W> {
        self.inner.finish()?.finish()
    }
}

impl<W: Write> Write for DoubleCompressor<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct DoubleDecompressor<R: Read> {
    inner: read::DeflateDecoder<read::DeflateDecoder<R>>,
}

impl<R: Read> DoubleDecompressor<R> {
    #[inline(always)]
    pub fn new(reader: R) -> Self {
        let decompressor = read::DeflateDecoder::new(reader);
        let decompressor = read::DeflateDecoder::new(decompressor);
        Self {
            inner: decompressor,
        }
    }
}

impl<R: Read> Read for DoubleDecompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
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
        w.write_u64::<LE>(self.creation_time)?;
        self.parent.write_to(&mut w)?;
        self.name.write_to(&mut w)?;
        Ok(())
    }
}

impl ReadFrom for Metadata {
    fn read_from(mut r: impl Read) -> io::Result<Self> {
        let diff_count = r.read_u32::<LE>()?;
        let creation_time = r.read_u64::<LE>()?;
        let parent = String::read_from(&mut r)?;
        let name = String::read_from(&mut r)?;
        Ok(Self {
            diff_count,
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

struct ChunkNumbers(Vec<ChunkNumber>);

impl WriteTo for ChunkNumbers {
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

impl ReadFrom for ChunkNumbers {
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
