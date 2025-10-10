#![feature(file_buffered)]

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use clap::Parser;
use wplace_tools::{diff2, extract_datetime};
use wplace_tools::diff_file::DiffFileReader;
use rayon::prelude::*;

#[derive(clap::Parser)]
struct Args {
    old: PathBuf,
    new: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    println!("Converting {}...", args.old.display());
    transform(args.old, args.new)?;

    Ok(())
}

fn transform(old: impl AsRef<Path>, new: impl AsRef<Path>) -> anyhow::Result<()> {
    let r = DiffFileReader::new(File::open_buffered(old)?)?;

    let mut writer = diff2::DiffFileWriter::create(
        File::create_buffered(new)?,
        r.index.clone(),
    )?;
    for x in r.chunk_diff_iter() {
        let x = x?;
        writer.add_diff(x.0, &x.1)?;
    }
    writer.finalize()?;
    Ok(())
}