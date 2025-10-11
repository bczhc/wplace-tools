#![feature(file_buffered)]

use bincode::config::standard;
use clap::Parser;
use log::info;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::stdout;
use std::path::{Path, PathBuf};
use wplace_tools::checksum::{chunk_checksum};
use wplace_tools::indexed_png::read_png;
use wplace_tools::{
    collect_chunks, set_up_logger, stylized_progress_bar, ChunkNumber, CHUNK_LENGTH,
};

#[derive(clap::Parser)]
struct Args {
    snapshot_dir: PathBuf,
    crc_out: PathBuf,
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = Args::parse();
    let path = &args.snapshot_dir;
    info!("Collecting...");
    let collected = collect_chunks(path, None)?;

    let out_path = args.crc_out;

    info!("Computing checksum...");
    let pb = stylized_progress_bar(collected.len() as u64);

    let computed = collected
        .into_par_iter()
        .map(|x| {
            let mut buf = [0_u8; CHUNK_LENGTH];
            let file = path.join(format!("{}/{}.png", x.0, x.1));
            read_png(file, &mut buf).unwrap();
            pb.inc(1);
            (x, chunk_checksum(&buf))
        })
        .collect::<HashMap<_, _>>();

    pb.finish();

    bincode::encode_into_std_write(
        computed,
        &mut File::create_buffered(out_path)?,
        standard(),
    )?;
    Ok(())
}
