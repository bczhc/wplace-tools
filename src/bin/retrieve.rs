#![feature(file_buffered)]
#![feature(yeet_expr)]
#![feature(try_blocks)]
#![warn(clippy::all, clippy::nursery)]

use anyhow::anyhow;
use clap::Parser;
use log::{error, info};
use rayon::prelude::*;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::process::{abort, exit};
use lazy_regex::regex;
use wplace_tools::diff2::{DiffDataRange, IndexEntry};
use wplace_tools::indexed_png::{read_png, read_png_reader, write_chunk_png};
use wplace_tools::tar::ChunksTarReader;
use wplace_tools::{CHUNK_LENGTH, ChunkNumber, apply_chunk, diff2, extract_datetime, flate2_decompress, open_file_range, set_up_logger, stylized_progress_bar, validate_chunk_checksum, quick_capture};
use yeet_ops::yeet;

#[derive(clap::Parser)]
struct Args {
    /// Chunk(s) to retrieve. Format: x1-y1,x2-y2,x3-y3,... or x1-y1..x2-y2
    #[arg(short, long)]
    chunk: String,

    /// Directory storing .diff files.
    #[arg(short, long)]
    diff_dir: PathBuf,

    /// Path of the initial snapshot.
    #[arg(short, long)]
    base_snapshot: PathBuf,

    /// Output path.
    #[arg(short, long)]
    out: PathBuf,

    /// Name of the goal snapshot. If not present, use the latest in `diff_dir`.
    #[arg(short = 't', long)]
    at: Option<String>,

    /// If enabled, instead of retrieving only the goal one, also retrieve all chunks prior to it.
    #[arg(short, long)]
    all: bool,
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = Args::parse();
    let chunks = parse_chunk_string(&args.chunk)?;

    let diff_path = Path::new(&args.diff_dir);

    info!("Collecting diff files...");
    let mut diff_list = Vec::new();
    for x in walkdir::WalkDir::new(diff_path) {
        let x = x?;
        if x.path().is_file() {
            let filename = x
                .file_name()
                .to_str()
                .ok_or_else(|| anyhow!("Invalid filename"))?;
            diff_list.push(
                extract_datetime(filename).ok_or_else(|| anyhow!("Malformed diff filename"))?,
            );
        }
    }
    diff_list.sort();
    let last_diff_list = diff_list.last().ok_or_else(|| anyhow::anyhow!("Empty diff list!"))?;
    let goal_snapshot = match &args.at {
        None => {
            last_diff_list
        }
        Some(x) => {
            x
        }
    };

    let Some(dest_snap_pos) = diff_list.iter().position(|x| x == goal_snapshot) else {
        yeet!(anyhow::anyhow!(
            "Cannot find the destination snapshot in the diff list"
        ));
    };
    let base_snapshot_name = extract_datetime(
        format!(
            "{}",
            args.base_snapshot
                .file_name()
                .expect("No filename")
                .display()
        )
        .as_str(),
    )
    .ok_or_else(|| anyhow!("Malformed base snapshot name"))?;
    let base_start = diff_list
        .iter()
        .position(|x| x == &base_snapshot_name)
        .map(|x| x + 1)
        .unwrap_or(0);
    let apply_list = &diff_list[base_start..=dest_snap_pos];

    info!("Collecting index...");
    let progress = stylized_progress_bar(diff_list.len() as u64);
    let map = diff_list
        .iter()
        .par_bridge()
        .map(|x| {
            let mut reader = diff2::DiffFile::open(
                File::open_buffered(diff_path.join(format!("{x}.diff"))).unwrap(),
            )
            .unwrap();
            let index = reader.read_index().unwrap();
            progress.inc(1);
            (extract_datetime(x).unwrap(), index)
        })
        .collect::<HashMap<_, _>>();
    progress.finish();

    info!("Retrieving...");
    let pb = stylized_progress_bar((apply_list.len() * chunks.len()) as u64);
    chunks.into_iter().par_bridge().for_each(|n| {
        let result: anyhow::Result<()> = try {
            let chunk_out = args.out.join(format!("{}-{}", n.0, n.1));
            fs::create_dir_all(&chunk_out)?;

            let mut diff_data = vec![0_u8; CHUNK_LENGTH];

            let mut base = retrieve_chunk(&args.base_snapshot, n, true)?;
            for (idx, name) in apply_list.iter().enumerate() {
                pb.inc(1);
                let entry = map[name].get(&n);
                let entry = match entry {
                    None => {
                        // chunk had not been created in this snapshot
                        info!("Chunk not present in this snapshot '{}', skipping...", name);
                        continue
                    }
                    Some(e) => {e}
                };

                match entry.diff_data_range {
                    DiffDataRange::Unchanged => {
                        // just pass
                    }
                    DiffDataRange::Changed { pos, len } => {
                        let reader =
                            open_file_range(diff_path.join(format!("{name}.diff")), pos, len)?;
                        flate2_decompress(reader, &mut diff_data)?;
                        apply_chunk(&mut base, <&[_; _]>::try_from(&diff_data[..]).unwrap());
                        validate_chunk_checksum(&base, entry.checksum)?;
                    }
                }

                let img_path = chunk_out.join(format!("{name}.png"));
                match args.all {
                    true => {
                        write_chunk_png(&img_path, &base)?;
                    }
                    false if idx == apply_list.len() - 1 => {
                        write_chunk_png(&img_path, &base)?;
                    }
                    _ => {}
                }
            }
        };
        if let Err(e) = result {
            error!("Error occurred: {e}");
            exit(1);
        }
    });
    pb.finish();

    Ok(())
}

fn parse_chunk_string(s: &str) -> anyhow::Result<Vec<ChunkNumber>> {
    let mut chunks: Vec<ChunkNumber> = Vec::new();
    let s = s.chars().filter(|x| !x.is_whitespace()).collect::<String>();
    let split = s.split(',');
    for x in split {
        let p1 = regex!(r"^(\d+)\-(\d+)\.\.(\d+)\-(\d+)$");
        let p2 = regex!(r"^(\d+)\-(\d+)$");
        if p1.is_match(x) {
            let group = quick_capture(x, p1).unwrap();
            let start: ChunkNumber = (group[0].parse()?, group[1].parse()?);
            let end: ChunkNumber = (group[2].parse()?, group[3].parse()?);
            expand_chunks_range(start, end).iter().for_each(|&x| chunks.push(x));
        } else if p2.is_match(x) {
            let group = quick_capture(x, p2).unwrap();
            chunks.push((group[0].parse()?, group[1].parse()?));
        } else {
            yeet!(anyhow::anyhow!("Malformed chunk string: {}", s))
        }
    }
    Ok(chunks)
}

/// `start` and `end` represent the two diagonal points.
fn expand_chunks_range(start: ChunkNumber, end: ChunkNumber) -> Vec<(u16, u16)> {
    fn range(n1: u16, n2: u16) -> RangeInclusive<u16> {
        if n1 < n2 { n1..=n2 } else { n2..=n1 }
    }

    let x_range = range(start.0, end.0);
    let y_range = range(start.1, end.1);
    let mut collected = x_range
        .map(|x| y_range.clone().map(move |y| (x, y)))
        .flatten()
        .collect::<Vec<_>>();
    collected.sort();
    collected
}

fn retrieve_chunk(snapshot: impl AsRef<Path>, n: ChunkNumber, allow_non_exist: bool) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![0_u8; CHUNK_LENGTH];
    let path = snapshot.as_ref();
    if path.is_dir() {
        let png_file = path.join(format!("{}/{}.png", n.0, n.1));
        if png_file.exists() || !allow_non_exist {
            read_png(png_file, &mut buf)?;
        }
    } else if path
        .extension()
        .map(|x| x.eq_ignore_ascii_case(OsStr::new("tar")))
        .unwrap_or(false)
    {
        let tar = ChunksTarReader::open_with_index(path)?;
        let chunk_reader = tar
            .open_chunk(n)
            .ok_or_else(|| anyhow::anyhow!("No such chunk"))??;
        read_png_reader(chunk_reader, &mut buf)?;
    } else {
        yeet!(anyhow::anyhow!("Unknown snapshot type"));
    }
    Ok(buf)
}
