#![feature(decl_macro)]
#![feature(file_buffered)]
#![feature(likely_unlikely)]

use crate::cli::Commands;
use chrono::{Local, TimeZone};
use clap::Parser;
use flate2::{write, Compression};
use log::info;
use rayon::prelude::*;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, hint};
use wplace_tools::checksum::Checksum;
use wplace_tools::diff_file::{DiffFileReader, DiffFileWriter, Metadata};
use wplace_tools::indexed_png::{read_png, write_chunk_png};
use wplace_tools::{
    collect_chunks, new_chunk_file, set_up_logger, stylized_progress_bar, unwrap_os_str, CHUNK_LENGTH,
    MUTATION_MASK, PALETTE_INDEX_MASK,
};

mod cli {
    use clap::{Args, Parser, Subcommand, ValueHint};
    use std::path::PathBuf;
    use wplace_tools::TilesRange;

    #[derive(Debug, Parser)]
    pub struct Cli {
        #[command(subcommand)]
        pub command: Commands,
    }

    #[derive(Debug, Subcommand)]
    pub enum Commands {
        /// Create diff from `base` to `new`.
        Diff {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "NEW", value_hint = ValueHint::FilePath)]
            new: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,
        },

        /// Apply diff on `base`.
        Apply {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "DIFF", value_hint = ValueHint::FilePath)]
            diff: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,
        },

        /// Compare two archives. This is used to verify if a diff-apply pipeline works correctly.
        Compare {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "NEW", value_hint = ValueHint::FilePath)]
            new: PathBuf,
        },

        /// Merely copy the chunks. This is useful when used with `tiles_range`.
        Copy {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,

            #[command(flatten)]
            tiles_range_arg: TilesRangeArg,
        },

        /// Compute the checksum of an archive.
        Checksum {
            #[arg(value_hint = ValueHint::FilePath)]
            archive: PathBuf,
        },
    }

    #[derive(Args, Debug)]
    pub struct TilesRangeArg {
        /// Range of tiles. Format: <x-min>,<x-max>,<y-min>,<y-max>
        #[arg(short = 'r', long)]
        pub tiles_range: Option<String>,
    }

    impl TilesRangeArg {
        pub fn parse(&self) -> Option<TilesRange> {
            self.tiles_range
                .as_ref()
                .and_then(|x| TilesRange::parse_str(x))
        }
    }
}

#[inline(always)]
fn compare_png(base: impl AsRef<Path>, new: impl AsRef<Path>) -> anyhow::Result<bool> {
    let mut img1 = vec![0_u8; CHUNK_LENGTH];
    let mut img2 = vec![0_u8; CHUNK_LENGTH];
    read_png(base, &mut img1)?;
    read_png(new, &mut img2)?;
    Ok(img1 == img2)
}

/// Returns compressed diff between two images. None is for identical images.
#[inline(always)]
fn diff_png(base: impl AsRef<Path>, new: impl AsRef<Path>) -> anyhow::Result<Option<Vec<u8>>> {
    let base = base.as_ref();
    let new = new.as_ref();

    let mut buf = vec![0_u8; CHUNK_LENGTH * 2];
    let (buf1, buf2) = buf.split_at_mut(CHUNK_LENGTH);

    if base.exists() {
        read_png(base, buf1)?;
    }
    read_png(new, buf2)?;

    // It's expecting that a large percent of the chunks are not mutated.
    // Thus in this case, disabling further diff creation can reduce the process time.
    if base.exists() && buf1 == buf2 {
        return Ok(None);
    }

    for x in buf1.iter_mut().zip(buf2) {
        let i1 = *x.0 & PALETTE_INDEX_MASK;
        let i2 = *x.1 & PALETTE_INDEX_MASK;
        if hint::likely(i1 == i2) {
            *x.0 = 0;
        } else {
            *x.0 = i2 | MUTATION_MASK;
        }
    }

    let mut compressor =
        write::DeflateEncoder::new(Cursor::new(Vec::new()), Compression::default());
    compressor.write_all(buf1)?;
    let data = compressor.finish()?.into_inner();
    Ok(Some(data))
}

fn apply_png(
    base: impl AsRef<Path>,
    output: impl AsRef<Path>,
    diff_data: &[u8; CHUNK_LENGTH],
) -> anyhow::Result<()> {
    let mut base_buf = vec![0_u8; CHUNK_LENGTH];

    if base.as_ref().exists() {
        read_png(base, &mut base_buf)?;
    }

    for i in 0..CHUNK_LENGTH {
        // has mutation flag - apply the pixel
        if diff_data[i] & MUTATION_MASK == MUTATION_MASK {
            base_buf[i] = diff_data[i] & PALETTE_INDEX_MASK;
        }
    }

    write_chunk_png(output, &base_buf)?;

    Ok(())
}

thread_local! {
    static COMPRESSOR_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = cli::Cli::parse();
    match args.command {
        Commands::Diff { base, new, output } => {
            info!("Collecting files...");
            let collected = collect_chunks(&new, None)?;

            info!("Creating diff file...");
            let parent_name = unwrap_os_str!(base.file_name().expect("No filename"));
            let this_name = unwrap_os_str!(new.file_name().expect("No filename"));
            let output_file = File::create_buffered(output)?;
            let mut diff_file = DiffFileWriter::new(
                output_file,
                Metadata {
                    diff_count: 0, /* placeholder */
                    name: this_name.into(),
                    parent: parent_name.into(),
                    creation_time: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                },
                collected.clone(),
            )?;

            let (tx, rx) = sync_channel(1024);

            info!("Processing {} files...", collected.len());
            let progress = stylized_progress_bar(collected.len() as u64);
            spawn(move || {
                collected.into_par_iter().for_each_with(tx, |tx, (x, y)| {
                    let base_file = base.join(format!("{x}/{y}.png"));
                    let new_file = new.join(format!("{x}/{y}.png"));

                    let diff_buffer = diff_png(base_file, new_file).unwrap();
                    if let Some(b) = diff_buffer {
                        tx.send((x, y, b)).unwrap();
                    }
                    progress.inc(1);
                });
                progress.finish();
            });

            let mut diff_counter = 0_u32;
            for (x, y, diff) in rx {
                diff_file.add_chunk_diff((x, y), &diff)?;
                diff_counter += 1;
            }
            diff_file.finish(diff_counter)?;
        }
        Commands::Apply { base, diff, output } => {
            info!("Opening diff file...");
            let diff_file = DiffFileReader::new(File::open_buffered(&diff)?)?;
            let index = diff_file.index.clone();
            let index_length = index.len();
            let metadata = &diff_file.metadata;
            info!(
                "Total chunks: {}, parent: {}, name: {}, creation time: {}",
                index_length,
                metadata.parent,
                metadata.name,
                Local
                    .timestamp_millis_opt(metadata.creation_time as i64)
                    .unwrap()
            );
            let changed_chunks = Arc::new(Mutex::new(HashSet::new()));

            info!("Applying diff to {} chunks...", metadata.diff_count);
            let progress = stylized_progress_bar(metadata.diff_count as u64);

            let iter = diff_file.chunk_diff_iter()?;
            iter.into_iter().par_bridge().for_each(|x| {
                let x = x.unwrap();
                let chunk_x = x.0;
                let chunk_y = x.1;
                let mut raw_diff: Vec<u8> = Vec::with_capacity(CHUNK_LENGTH);
                let mut decompressor = write::DeflateDecoder::new(&mut raw_diff);
                decompressor.write_all(&x.2).unwrap();
                decompressor.finish().unwrap();

                let base_file = base.join(format!("{chunk_x}/{chunk_y}.png"));
                let output_file = new_chunk_file(&output, (chunk_x, chunk_y), "png");
                apply_png(
                    base_file,
                    output_file,
                    &raw_diff
                        .try_into()
                        .expect("Raw diff data length is expected to be 1_000_000"),
                )
                    .unwrap();
                changed_chunks.lock().unwrap().insert((chunk_x, chunk_y));
                progress.inc(1);
            });
            progress.finish();

            info!("Copying unchanged chunks...");
            let changed_chunks = Arc::try_unwrap(changed_chunks)
                .unwrap()
                .into_inner()
                .unwrap();
            let progress = stylized_progress_bar(index_length as u64 - changed_chunks.len() as u64);

            index.into_par_iter().for_each(|(chunk_x, chunk_y)| {
                if !changed_chunks.contains(&(chunk_x, chunk_y)) {
                    // chunks that are in the archive index but don't have their diff data
                    // are unchanged
                    fs::copy(
                        base.join(format!("{chunk_x}/{chunk_y}.png")),
                        new_chunk_file(&output, (chunk_x, chunk_y), "png"),
                    )
                        .unwrap();
                    progress.inc(1);
                }
            });
            progress.finish();

            /*index.into_par_iter().for_each(|(x, y)| {
                let base_file = base.join(format!("{x}/{y}.png"));
                let diff_file = diff.join(format!("{x}/{y}.bin"));
                let output_file = new_chunk_file(&output, (x, y), "png");

                if diff_file.exists() {
                    // chunk changed
                    apply_png(base_file, diff_file, output_file).unwrap();
                } else {
                    // chunk not changed; just copy one from `base`
                    fs::copy(&base_file, &output_file).unwrap();
                }
                progress.inc(1);
            });*/

            progress.finish();
        }
        Commands::Compare { base, new } => {
            info!("Collecting files 'base'...");
            let mut base_collected = collect_chunks(&base, None)?;
            info!("Collecting files 'new'...");
            let mut new_collected = collect_chunks(&new, None)?;

            base_collected.sort();
            new_collected.sort();
            if base_collected != new_collected {
                return Err(anyhow::anyhow!("File lists differ."));
            }

            let length = base_collected.len();
            info!("Processing {} files...", length);
            let progress = stylized_progress_bar(length as u64);

            // job-stealing parallelization is enough here
            base_collected.into_iter().par_bridge().for_each(|(x, y)| {
                let base_file = base.join(format!("{x}/{y}.png"));
                let new_file = new.join(format!("{x}/{y}.png"));
                let result = compare_png(&base_file, &new_file).unwrap();
                if !result {
                    info!("{} and {} differ", base_file.display(), new_file.display());
                }
                progress.inc(1);
            });
            progress.finish();
        }
        Commands::Copy {
            base,
            output,
            tiles_range_arg,
        } => {
            fs::create_dir_all(&output)?;
            info!("Collecting files...");
            let collected = collect_chunks(&base, tiles_range_arg.parse())?;
            info!("Processing {} files...", collected.len());
            let progress = stylized_progress_bar(collected.len() as u64);

            collected.into_par_iter().for_each(|(x, y)| {
                let base_file = base.join(format!("{x}/{y}.png"));
                let output_file = new_chunk_file(&output, (x, y), "png");
                fs::copy(base_file, output_file).unwrap();
                progress.inc(1);
            });
            progress.finish();
        }

        Commands::Checksum { archive } => {
            info!("Collecting files...");
            let collected = collect_chunks(&archive, None)?;
            info!("Computing checksum...");
            let progress = stylized_progress_bar(collected.len() as u64);

            let checksum = Arc::new(Mutex::new(Checksum::new()));
            collected.into_iter().par_bridge().for_each_with(Arc::clone(&checksum), |c, (x, y)| {
                let chunk_file = archive.join(format!("{x}/{y}.png"));
                let mut chunk_buf = vec![0_u8; CHUNK_LENGTH];
                read_png(chunk_file, &mut chunk_buf).unwrap();
                checksum.lock().unwrap().add_chunk((x, y), &chunk_buf);

                progress.inc(1);
            });
            progress.finish();
            println!("{}", Arc::try_unwrap(checksum).ok().unwrap().into_inner().unwrap().compute());
        }
    }

    Ok(())
}

#[derive(Clone)]
struct Buffers {
    b1: Vec<u8>,
    b2: Vec<u8>,
    b3: Vec<u8>,
}

impl Buffers {
    #[inline(always)]
    fn split_mut(&mut self) -> (&mut Vec<u8>, &mut Vec<u8>, &mut Vec<u8>) {
        (&mut self.b1, &mut self.b2, &mut self.b3)
    }
}

impl Default for Buffers {
    #[inline(always)]
    fn default() -> Self {
        Self {
            b1: vec![0_u8; CHUNK_LENGTH],
            b2: vec![0_u8; CHUNK_LENGTH],
            b3: vec![0_u8; CHUNK_LENGTH],
        }
    }
}

#[derive(Copy, Clone)]
#[allow(unused)]
struct SharedBuffer(*mut Buffers);

unsafe impl Send for SharedBuffer {}
unsafe impl Sync for SharedBuffer {}
