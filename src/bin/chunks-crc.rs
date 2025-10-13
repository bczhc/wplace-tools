#![feature(file_buffered)]

use bincode::config::standard;
use clap::Parser;
use log::info;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use wplace_tools::checksum::chunk_checksum;
use wplace_tools::indexed_png::{read_png, read_png_reader};
use wplace_tools::tar::ChunksTarReader;
use wplace_tools::{collect_chunks, set_up_logger, stylized_progress_bar, CHUNK_LENGTH};

#[derive(clap::Parser)]
struct Args {
    snapshot: PathBuf,
    crc_out: PathBuf,
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = Args::parse();
    let path = &args.snapshot;
    if path.is_file() {
        process_snapshot_tar(args.snapshot, args.crc_out)?;
    } else {
        process_snapshot_dir(args.snapshot, args.crc_out)?;
    }
    Ok(())
}

fn process_snapshot_tar(
    snapshot: impl AsRef<Path>,
    crc_out: impl AsRef<Path>,
) -> anyhow::Result<()> {
    info!("Reading tar...");
    let snapshot = snapshot.as_ref();
    let tar = ChunksTarReader::open_with_index(snapshot)?;
    let map = &tar.map;

    let out_path = crc_out.as_ref();
    info!("Computing checksum...");
    let pb = stylized_progress_bar(map.len() as u64);

    let computed = map
        .into_par_iter()
        .map(|x| {
            let mut buf = [0_u8; CHUNK_LENGTH];
            let reader = tar.open_chunk(*x.0).unwrap().unwrap();
            read_png_reader(reader, &mut buf).unwrap();
            pb.inc(1);
            (x.0, chunk_checksum(&buf))
        })
        .collect::<HashMap<_, _>>();
    pb.finish();
    bincode::encode_into_std_write(computed, &mut File::create_buffered(out_path)?, standard())?;
    Ok(())
}

fn process_snapshot_dir(
    snapshot: impl AsRef<Path>,
    crc_out: impl AsRef<Path>,
) -> anyhow::Result<()> {
    info!("Collecting...");
    let snapshot = snapshot.as_ref();
    let collected = collect_chunks(snapshot, None)?;

    let out_path = crc_out;

    info!("Computing checksum...");
    let pb = stylized_progress_bar(collected.len() as u64);

    let computed = collected
        .into_par_iter()
        .map(|x| {
            let mut buf = [0_u8; CHUNK_LENGTH];
            let file = snapshot.join(format!("{}/{}.png", x.0, x.1));
            read_png(file, &mut buf).unwrap();
            pb.inc(1);
            (x, chunk_checksum(&buf))
        })
        .collect::<HashMap<_, _>>();

    pb.finish();

    bincode::encode_into_std_write(computed, &mut File::create_buffered(out_path)?, standard())?;
    Ok(())
}
