#![feature(file_buffered)]

use wplace_tools::CHUNK_LENGTH;
use wplace_tools::indexed_png::{read_png_reader, write_chunk_png};
use wplace_tools::tar::ChunksTarReader;

fn main() ->anyhow::Result<()> {
    let path = "/mnt/nvme/wplace-archives/1/2025-09-21T09-35-13.789Z+2h49m.tar";
    let mut reader = ChunksTarReader::open_with_index(path)?;

    let reader = reader.open_chunk((1717, 837)).unwrap()?;
    let mut buf = vec![0_u8; CHUNK_LENGTH];
    read_png_reader(reader, &mut buf)?;

    write_chunk_png("/home/bczhc/a.png", &buf)?;
    Ok(())
}
