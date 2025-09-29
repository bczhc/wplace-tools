#![feature(decl_macro)]
#![feature(file_buffered)]
#![feature(likely_unlikely)]
#![feature(yeet_expr)]

use crate::cli::Commands;
use chrono::{Local, TimeZone};
use clap::Parser;
use flate2::{write, Compression};
use log::{debug, error, info};
use rayon::prelude::*;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::abort;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, hint};
use tempfile::NamedTempFile;
use wplace_tools::checksum::Checksum;
use wplace_tools::diff_file::{DiffFileReader, DiffFileWriter, Metadata};
use wplace_tools::indexed_png::{read_png, read_png_reader, write_chunk_png};
use wplace_tools::tar::ChunksTarReader;
use wplace_tools::zip::ChunksZipReader;
use wplace_tools::{
    collect_chunks, new_chunk_file, set_up_logger, stylized_progress_bar, unwrap_os_str, ChunkNumber,
    CHUNK_LENGTH, MUTATION_MASK, PALETTE_INDEX_MASK,
};
use yeet_ops::yeet;

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

            /// Do not verify the checksum.
            #[arg(long, default_value = "false")]
            no_checksum: bool,
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

        /// Print info of the diff file.
        Show {
            #[arg(value_hint = ValueHint::FilePath)]
            diff: PathBuf,
            /// Output as JSON format.
            #[arg(long)]
            json: bool,
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

/// Returns raw diff between two images. None is for identical images.
#[inline(always)]
fn diff_png(base_buf: &mut [u8], new_buf: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    for x in base_buf.iter_mut().zip(new_buf) {
        let i1 = *x.0 & PALETTE_INDEX_MASK;
        let i2 = x.1 & PALETTE_INDEX_MASK;
        if hint::likely(i1 == i2) {
            *x.0 = 0;
        } else {
            *x.0 = i2 | MUTATION_MASK;
        }
    }

    let mut compressor =
        write::DeflateEncoder::new(Cursor::new(Vec::new()), Compression::default());
    compressor.write_all(base_buf)?;
    Ok(Some(compressor.finish()?.into_inner()))
}

#[inline(always)]
fn apply_png(
    base: impl AsRef<Path>,
    output: impl AsRef<Path>,
    diff_data: &[u8; CHUNK_LENGTH],
) -> anyhow::Result<()> {
    let mut base_buf = vec![0_u8; CHUNK_LENGTH];

    if base.as_ref().exists() {
        read_png(base, &mut base_buf)?;
    }

    for (base_pix, diff_pix) in base_buf.iter_mut().zip(diff_data) {
        if hint::unlikely(diff_pix & MUTATION_MASK == MUTATION_MASK) {
            // has mutation flag - apply the pixel
            *base_pix = diff_pix & PALETTE_INDEX_MASK;
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
            let mut output_dir = output
                .parent()
                .expect("Can not get parent of the output file");
            if output_dir == Path::new("") {
                output_dir = Path::new(".");
            }
            let temp_file = NamedTempFile::new_in(output_dir)?;
            debug!("temp_file: {}", temp_file.as_ref().display());
            let parent_name = unwrap_os_str!(base.file_name().expect("No filename"));
            let this_name = unwrap_os_str!(new.file_name().expect("No filename"));
            let output_file = File::create_buffered(temp_file.as_ref())?;
            let mut diff_file = DiffFileWriter::new(
                output_file,
                Metadata {
                    diff_count: 0,                /* placeholder */
                    checksum: Default::default(), /* placeholder */
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
            let handle = spawn(move || {
                let checksum = Arc::new(Mutex::new(Checksum::new()));
                collected.into_par_iter().for_each_with(tx, |tx, (x, y)| {
                    let base_file = base.join(format!("{x}/{y}.png"));
                    let new_file = new.join(format!("{x}/{y}.png"));

                    let mut base_buf = vec![0_u8; CHUNK_LENGTH];
                    let mut new_buf = vec![0_u8; CHUNK_LENGTH];

                    if base_file.exists() {
                        read_png(&base_file, &mut base_buf).unwrap();
                    }
                    read_png(&new_file, &mut new_buf).unwrap();

                    checksum.lock().unwrap().add_chunk((x, y), &new_buf);

                    // It's expecting that a large percent of the chunks are not mutated.
                    // Thus in this case, only computing diff for changed chunks can reduce the process time.
                    if !base_file.exists() || base_buf != new_buf {
                        let compressed_diff = diff_png(&mut base_buf, &new_buf).unwrap();
                        if let Some(b) = compressed_diff {
                            tx.send((x, y, b)).unwrap();
                        }
                    }
                    progress.inc(1);
                });
                progress.finish();
                Arc::try_unwrap(checksum)
                    .unwrap_or_else(|_| unreachable!())
                    .into_inner()
                    .unwrap()
                    .compute()
            });

            let mut diff_counter = 0_u32;
            for (x, y, diff) in rx {
                diff_file.add_chunk_diff((x, y), &diff)?;
                diff_counter += 1;
            }
            diff_file.finish(diff_counter, handle.join().unwrap().into())?;
            temp_file.persist(output)?;
        }
        Commands::Apply {
            base,
            diff,
            output,
            no_checksum,
        } => {
            info!("Opening diff file...");
            let diff_file = DiffFileReader::new(File::open_buffered(&diff)?)?;
            let index = diff_file.index.clone();
            let index_length = index.len();
            let metadata = &diff_file.metadata;
            let checksum = metadata.checksum;
            let changed_chunks = Arc::new(Mutex::new(HashSet::new()));
            print_diff_info(&diff_file);

            info!("Applying diff to {} chunks...", metadata.diff_count);
            let progress = stylized_progress_bar(metadata.diff_count as u64);

            let iter = diff_file.chunk_diff_iter();
            iter.into_iter().par_bridge().for_each(|x| {
                let x = x.unwrap();
                let chunk_x = x.0.0;
                let chunk_y = x.0.1;
                let mut raw_diff: Vec<u8> = Vec::with_capacity(CHUNK_LENGTH);
                let mut decompressor = write::DeflateDecoder::new(&mut raw_diff);
                decompressor.write_all(&x.1).unwrap();
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

            index.iter().par_bridge().for_each(|&(chunk_x, chunk_y)| {
                if !changed_chunks.contains(&(chunk_x, chunk_y)) {
                    // chunks that are in the archive index but don't have their diff data
                    // are unchanged
                    if let Err(e) = fs::copy(
                        base.join(format!("{chunk_x}/{chunk_y}.png")),
                        new_chunk_file(&output, (chunk_x, chunk_y), "png"),
                    ) {
                        error!("Failed to copy: {}; abort", e);
                        abort();
                    };
                    progress.inc(1);
                }
            });
            progress.finish();

            if !no_checksum {
                info!("Checksum validation...");
                let computed = checksum_with_progress(&index, &output);
                if &checksum != computed.as_bytes() {
                    return Err(anyhow::anyhow!("Checksum mismatch!"));
                }
            }
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
            if archive.is_file() {
                let file_ext = archive.extension();
                match file_ext {
                    Some(x) if x == OsStr::new("tar") => {
                        checksum_tar(&archive)?;
                    }
                    Some(x) if x == OsStr::new("zip") => {
                        checksum_zip(&archive)?;
                    }
                    _ => {
                        yeet!(anyhow::anyhow!("Unknown extension: {:?}", file_ext));
                    }
                }
            } else {
                info!("Collecting files...");
                let collected = collect_chunks(&archive, None)?;
                info!("Computing checksum...");
                let hash = checksum_with_progress(&collected, archive);
                println!("{}", hash);
            }
        }

        Commands::Show { diff, json } => {
            let reader = DiffFileReader::new(File::open_buffered(&diff)?)?;
            if json {
                let info = DiffFileInfo::new(&reader);
                println!("{}", serde_json::to_string(&info).unwrap());
            } else {
                print_diff_info(&reader);
            }
        }
    }

    Ok(())
}

fn checksum_with_progress(chunks: &[ChunkNumber], archive_path: impl AsRef<Path>) -> blake3::Hash {
    let progress = stylized_progress_bar(chunks.len() as u64);
    let archive_path = archive_path.as_ref();

    let checksum = Arc::new(Mutex::new(Checksum::new()));
    chunks.iter().par_bridge().for_each(|&(x, y)| {
        let chunk_file = archive_path.join(format!("{x}/{y}.png"));
        let mut chunk_buf = vec![0_u8; CHUNK_LENGTH];
        read_png(chunk_file, &mut chunk_buf).unwrap();
        checksum.lock().unwrap().add_chunk((x, y), &chunk_buf);

        progress.inc(1);
    });
    progress.finish();
    Arc::try_unwrap(checksum)
        .ok()
        .unwrap()
        .into_inner()
        .unwrap()
        .compute()
}

fn checksum_tar(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let path = path.as_ref();
    let mut reader = ChunksTarReader::open_with_index(path)?;
    let map = &reader.map;
    let progress = stylized_progress_bar(map.len() as _);
    let checksum = Arc::new(Mutex::new(Checksum::new()));

    map.into_iter().par_bridge().for_each(|(&n, range)| {
        let reader = reader.open_chunk(n).unwrap().unwrap();
        let mut buf = vec![0_u8; CHUNK_LENGTH];
        read_png_reader(reader, &mut buf).unwrap();
        checksum.lock().unwrap().add_chunk(n, &buf);
        progress.inc(1);
    });
    progress.finish();

    println!(
        "{}",
        Arc::try_unwrap(checksum)
            .ok()
            .unwrap()
            .into_inner()
            .unwrap()
            .compute()
    );
    Ok(())
}

fn checksum_zip(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let path = path.as_ref();
    let reader = ChunksZipReader::open(path)?;
    let progress = stylized_progress_bar(reader.map.len() as _);
    let checksum = Arc::new(Mutex::new(Checksum::new()));

    reader.map.into_iter().par_bridge().for_each(|(n, range)| {
        let mut file = File::open_buffered(path).unwrap();
        file.seek(SeekFrom::Start(range.0)).unwrap();
        let take = file.take(range.1);
        let mut buf = vec![0_u8; CHUNK_LENGTH];
        read_png_reader(take, &mut buf).unwrap();
        checksum.lock().unwrap().add_chunk(n, &buf);
        progress.inc(1);
    });
    progress.finish();

    println!(
        "{}",
        Arc::try_unwrap(checksum)
            .ok()
            .unwrap()
            .into_inner()
            .unwrap()
            .compute()
    );
    Ok(())
}

fn print_diff_info(reader: &DiffFileReader<impl Read>) {
    let meta = &reader.metadata;
    println!(
        "Creation time: {}
Archive name: {}
Parent name: {}
Total chunks: {}
Changed chunks: {}
Checksum: {}",
        Local
            .timestamp_millis_opt(meta.creation_time as i64)
            .unwrap(),
        meta.name,
        meta.parent,
        reader.index.len(),
        meta.diff_count,
        blake3::Hash::from_bytes(meta.checksum)
    )
}

#[derive(Serialize)]
#[serde(rename = "camelCase")]
struct DiffFileInfo {
    creation_time: u64,
    name: String,
    parent: String,
    total_chunks: u32,
    changed_chunks: u32,
    checksum: String,
}

impl DiffFileInfo {
    fn new(reader: &DiffFileReader<impl Read>) -> Self {
        let meta = reader.metadata.clone();
        Self {
            creation_time: meta.creation_time,
            name: meta.name,
            parent: meta.parent,
            total_chunks: reader.index.len().try_into().unwrap(),
            changed_chunks: meta.diff_count,
            checksum: format!("{}", blake3::Hash::from_bytes(meta.checksum)),
        }
    }
}
