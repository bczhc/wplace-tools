#![feature(file_buffered)]

use bincode::config::standard;
use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use wplace_tools::diff2::Metadata;
use wplace_tools::diff_file::DiffFileReader;
use wplace_tools::{diff2, extract_datetime, ChunkNumber};

#[derive(clap::Parser)]
struct Args {
    old: PathBuf,
    new: PathBuf,
    checksum_ser_dir: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cs_ser_dir = &args.checksum_ser_dir;

    let datetime = extract_datetime(format!("{}", args.new.display()).as_str());
    let map: HashMap<ChunkNumber, u32> = bincode::decode_from_std_read(
        &mut File::open_buffered(cs_ser_dir.join(format!("{datetime}.ser")))?,
        standard(),
    )?;

    println!("Converting {}...", args.old.display());
    convert(args.old, args.new, &map)?;

    Ok(())
}

fn convert(old: impl AsRef<Path>, new: impl AsRef<Path>, cs_map: &HashMap<ChunkNumber, u32>) -> anyhow::Result<()> {
    let r = DiffFileReader::new(File::open_buffered(old)?)?;

    let mut writer = diff2::DiffFileWriter::create(
        File::create_buffered(new)?,
        Metadata::default(),
    )?;

    let mut unchanged = r.index.iter().copied().collect::<HashSet<_>>();
    
    for x in r.chunk_diff_iter() {
        let x = x?;
        writer.add_entry(x.0, Some(&x.1), cs_map[&x.0])?;
        unchanged.remove(&x.0);
    }

    for n in unchanged {
        writer.add_entry(n, None, cs_map[&n])?;
    }
    writer.finalize()?;
    Ok(())
}
