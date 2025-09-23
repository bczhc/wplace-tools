use crate::{bincode_config, ChunkNumber, CHUNK_LENGTH};
use bincode::{Decode, Encode};
use byteorder::{WriteBytesExt, LE};
use flate2::{read, write, Compression};
use std::io;
use std::io::{Read, Write};
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread::spawn;
use yeet_ops::yeet;

pub const MAGIC: [u8; 11] = *b"wplace-diff";

#[derive(Encode, Decode, Default)]
pub struct Metadata {
    pub name: String,
    pub parent: String,
    pub creation_time: u64,
}

impl Metadata {
    fn write_to(&self, mut w: impl Write) -> anyhow::Result<()> {
        bincode::encode_into_std_write(self, &mut w, bincode_config())?;
        Ok(())
    }
}

/// An assembled diff file that saves all the chunk changes.
pub struct DiffFileWriter<W: Write> {
    writer: DoubleCompressor<W>,
}

impl<W> DiffFileWriter<W>
where
    W: Write,
{
    pub fn new(
        mut writer: W,
        metadata: Metadata,
        archive_index: &[ChunkNumber],
    ) -> anyhow::Result<Self> {
        writer.write_all(&MAGIC)?;
        let mut compressor = DoubleCompressor::new(writer);
        metadata.write_to(&mut compressor)?;
        bincode::encode_into_std_write(archive_index, &mut compressor, bincode_config())?;

        Ok(Self { writer: compressor })
    }

    #[inline(always)]
    /// This is only single-threading safe.
    pub fn add_chunk_diff(&mut self, n: ChunkNumber, data: &[u8]) -> anyhow::Result<()> {
        assert_eq!(data.len(), CHUNK_LENGTH);
        self.writer.write_u16::<LE>(n.0)?;
        self.writer.write_u16::<LE>(n.1)?;
        self.writer.write_all(data)?;
        Ok(())
    }
}

const CHUNK_DIFF_SIZE: usize = 2 /* size of ChunkNumber a.k.a. u16 */ + 2 + CHUNK_LENGTH;

pub struct DiffFileReader<R: Read> {
    reader: DoubleDecompressor<R>,
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
        let mut reader = DoubleDecompressor::new(reader);
        let metadata: Metadata = bincode::decode_from_std_read(&mut reader, bincode_config())?;
        let index: Vec<ChunkNumber> = bincode::decode_from_std_read(&mut reader, bincode_config())?;
        Ok(Self {
            reader,
            metadata,
            index,
        })
    }

    pub fn chunk_diff_iter(self) -> anyhow::Result<Receiver<io::Result<Vec<u8>>>> {
        let (tx, rx) = sync_channel(8192);

        spawn(move || {
            let mut reader = self.reader;
            // TODO: record something like "number of changed chunks" to prevent unexpected premature EOF.
            loop {
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
