#![feature(file_buffered)]

use std::fs::File;
use wplace_tools::diff2;
use wplace_tools::diff_file::DiffFileReader;

fn main() -> anyhow::Result<()> {
    let path = "/mnt/nvme/wplace-archives/mine/diffs/2025-10-09T19-21-23.109Z.diff";
    let r = DiffFileReader::new(File::open_buffered(path)?)?;

    let new_path = "/mnt/nvme/wplace-archives/new.bin";
    let mut writer = diff2::DiffFileWriter::create(
        File::create_buffered(new_path)?,
        r.index.clone(),
    )?;
    for x in r.chunk_diff_iter() {
        let x = x?;
        writer.add_diff(x.0, &x.1)?;
    }
    writer.finalize()?;

    let mut reader = diff2::DiffFile::open(File::open_buffered(new_path)?)?;
    let index = reader.read_index()?;
    println!("{}", index.len());
    println!("{:?}", index[&(1234, 1234)]);

    Ok(())
}
