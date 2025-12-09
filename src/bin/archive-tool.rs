#![feature(decl_macro)]
#![feature(file_buffered)]
#![feature(likely_unlikely)]
#![feature(yeet_expr)]
#![feature(try_blocks)]
#![warn(clippy::all, clippy::nursery)]

use crate::cli::Commands;
use clap::Parser;
use flate2::{write, Compression};
use log::{debug, error, info};
use rayon::prelude::*;
use std::cell::RefCell;
use std::ffi::OsStr;
use std::fs::{read, File};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::mpsc::sync_channel;
use std::thread::spawn;
use std::{fs, hint, io};
use tempfile::NamedTempFile;
use wplace_tools::checksum::chunk_checksum;
use wplace_tools::diff2::{DiffDataRange, Metadata};
use wplace_tools::indexed_png::{read_png, read_png_reader};
use wplace_tools::tar::ChunksTarReader;
use wplace_tools::{
    apply_png, collect_chunks, diff2, new_chunk_file, open_file_range, set_up_logger,
    stylized_progress_bar, ChunkFetcher, ChunkProcessError, DirChunkFetcher, ExitOnError, TarChunkFetcher,
    CHUNK_LENGTH, MUTATION_MASK, PALETTE_INDEX_MASK,
};

mod cli {
    use clap::{Args, Parser, Subcommand, ValueHint};
    use std::path::PathBuf;
    use wplace_tools::TilesRange;

    #[derive(Debug, Parser)]
    /// Tools for Wplace snapshots
    pub struct Cli {
        #[command(subcommand)]
        pub command: Commands,
    }

    #[derive(Debug, Subcommand)]
    pub enum Commands {
        /// Create diff from `base` to `new`
        Diff {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "NEW", value_hint = ValueHint::FilePath)]
            new: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,
        },

        /// Apply `diff` on `base`
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

        /// Filter chunk files by `tiles_range`
        Filter {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,

            #[command(flatten)]
            tiles_range_arg: TilesRangeArg,
        },

        /// Print info of a diff file
        Show {
            #[arg(value_hint = ValueHint::FilePath)]
            diff: PathBuf,
        },

        /// Test a diff file.
        Test {
            #[arg(value_hint = ValueHint::FilePath)]
            diff: PathBuf,
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

/// Returns compressed diff between two images.
#[inline(always)]
fn diff_png_compressed(base_buf: &mut [u8], new_buf: &[u8]) -> anyhow::Result<Vec<u8>> {
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
    Ok(compressor.finish()?.into_inner())
}

thread_local! {
    static COMPRESSOR_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = cli::Cli::parse();
    match args.command {
        Commands::Diff { base, new, output } => {
            // a special handle for directly processing tar files
            if base.extension() == Some(OsStr::new("tar"))
                && new.extension() == Some(OsStr::new("tar"))
            {
                do_diff_for_tar(base, new, output)?;
                return Ok(());
            }

            do_diff_for_directory(base, new, output)?;
        }
        Commands::Apply { base, diff, output } => {
            info!("Opening diff file...");
            let mut diff_file = diff2::DiffFile::open(File::open_buffered(&diff)?)?;
            let index = diff_file.read_index()?;
            // process them separately (with two separate progress bar)
            let changed_chunks = index
                .iter()
                .filter(|x| x.1.diff_data_range.is_changed())
                .collect::<Vec<_>>();
            let unchanged_chunks = index
                .iter()
                .filter(|x| !x.1.diff_data_range.is_changed())
                .collect::<Vec<_>>();

            info!("Applying diff to {} chunks...", changed_chunks.len());
            let progress = stylized_progress_bar(changed_chunks.len() as u64);

            changed_chunks.into_iter().par_bridge().for_each(|x| {
                let result: anyhow::Result<()> = try {
                    let chunk_x = x.0.0;
                    let chunk_y = x.0.1;
                    let entry = x.1;

                    match entry.diff_data_range {
                        DiffDataRange::Changed { pos, len } => {
                            let diff_reader = open_file_range(&diff, pos, len)?;
                            let mut decompressor = flate2::read::DeflateDecoder::new(diff_reader);
                            let mut raw_diff = vec![0_u8; CHUNK_LENGTH];
                            decompressor.read_exact(&mut raw_diff)?;

                            let base_file = base.join(format!("{chunk_x}/{chunk_y}.png"));
                            let output_file = new_chunk_file(&output, (chunk_x, chunk_y), "png");
                            apply_png(
                                base_file,
                                output_file,
                                <&[_; _]>::try_from(&raw_diff[..])
                                    .expect("Raw diff data length is expected to be 1_000_000"),
                                entry.checksum,
                            )?;
                            progress.inc(1);
                        }
                        DiffDataRange::Unchanged => {
                            // changed_chunks is filtered
                            unreachable!()
                        }
                    }
                };
                result
                    .map_err(|e| ChunkProcessError {
                        inner: e,
                        chunk_number: *x.0,
                        diff_file: None,
                    })
                    .exit_on_error();
            });
            progress.finish();

            info!("Copying unchanged chunks...");
            let progress = stylized_progress_bar(unchanged_chunks.len() as u64);

            unchanged_chunks.into_iter().par_bridge().for_each(|x| {
                let entry = x.1;
                let chunk_x = x.0.0;
                let chunk_y = x.0.1;

                match entry.diff_data_range {
                    DiffDataRange::Unchanged => {
                        if let Err(e) = fs::copy(
                            base.join(format!("{chunk_x}/{chunk_y}.png")),
                            new_chunk_file(&output, (chunk_x, chunk_y), "png"),
                        ) {
                            error!("Failed to copy: {}; abort", e);
                            exit(1);
                        };
                        progress.inc(1);
                    }
                    DiffDataRange::Changed { .. } => {
                        unreachable!()
                    }
                }
            });
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
        Commands::Filter {
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

        Commands::Show { diff } => {
            let mut reader = diff2::DiffFile::open(File::open_buffered(&diff)?)?;
            println!(
                "Metadata: {}",
                serde_json::to_string(&reader.metadata).unwrap()
            );
            let index = reader.read_index()?;
            println!("Total chunks: {}", index.len());
            println!(
                "Changed chunks: {}",
                index
                    .iter()
                    .filter(|x| x.1.diff_data_range.is_changed())
                    .count()
            );
        }

        Commands::Test { diff } => {
            let mut reader = diff2::DiffFile::open(File::open_buffered(&diff)?)?;
            let index = reader.read_index()?;
            assert_eq!(reader.entry_count as usize, index.len());
            let pb = stylized_progress_bar(index.len() as u64);
            index.into_par_iter().for_each(|(n, e)| {
                let result: anyhow::Result<()> = try {
                    match e.diff_data_range {
                        DiffDataRange::Unchanged => {}
                        DiffDataRange::Changed { pos, len } => {
                            let portion = open_file_range(&diff, pos, len)?;
                            let mut decoder = flate2::read::DeflateDecoder::new(portion);
                            io::copy(&mut decoder, &mut io::sink())?;
                        }
                    }
                    pb.inc(1);
                };
                result.exit_on_error();
            });
            pb.finish();
            println!("Done.");
        }
    }

    Ok(())
}

fn do_diff(
    base_fetcher: impl ChunkFetcher + Send + Sync + 'static,
    new_fetcher: impl ChunkFetcher + Send + Sync + 'static,
    output: PathBuf,
) -> anyhow::Result<()> {
    info!("Creating diff file...");
    let mut output_dir = output
        .parent()
        .expect("Can not get parent of the output file");
    if output_dir == Path::new("") {
        output_dir = Path::new(".");
    }
    let temp_file = NamedTempFile::new_in(output_dir)?;
    debug!("temp_file: {}", temp_file.as_ref().display());
    let output_file = File::create_buffered(temp_file.as_ref())?;
    let mut diff_file = diff2::DiffFileWriter::create(output_file, Metadata::default())?;

    let (tx, rx) = sync_channel(1024);
    info!("Processing {} files...", new_fetcher.chunks_len());

    let progress = stylized_progress_bar(new_fetcher.chunks_len() as u64);
    spawn(move || {
        let chunks_iter = new_fetcher.chunks_iter();
        chunks_iter.par_bridge().for_each_with(tx, |tx, (x, y)| {
            let result: anyhow::Result<()> = try {
                let mut base_buf = vec![0_u8; CHUNK_LENGTH];
                let mut new_buf = vec![0_u8; CHUNK_LENGTH];

                let present = new_fetcher.fetch((x, y), &mut new_buf)?;
                assert!(present);
                let base_chunk_present = base_fetcher.fetch((x, y), &mut base_buf)?;

                let checksum = chunk_checksum(&new_buf);

                // It's expecting that a large percent of the chunks are not mutated.
                // Thus in this case, only computing diff for changed chunks can reduce the process time.
                let compressed_diff = if !base_chunk_present || base_buf != new_buf {
                    let compressed_diff = diff_png_compressed(&mut base_buf, &new_buf).unwrap();
                    Some(compressed_diff)
                } else {
                    None
                };
                tx.send((x, y, compressed_diff, checksum)).unwrap();
                progress.inc(1);
            };
            result.exit_on_error();
        });
        progress.finish();
    });

    for (x, y, diff, checksum) in rx {
        diff_file.add_entry((x, y), diff.as_deref(), checksum)?;
    }
    diff_file.finalize()?;
    temp_file.persist(output)?;
    Ok(())
}

fn do_diff_for_directory(base: PathBuf, new: PathBuf, output: PathBuf) -> anyhow::Result<()> {
    info!("Collecting files...");
    let new_fetcher = DirChunkFetcher::new(&new, true)?;
    let base_fetcher = DirChunkFetcher::new(&base, false)?;

    do_diff(base_fetcher, new_fetcher, output)?;
    Ok(())
}

fn do_diff_for_tar(base: PathBuf, new: PathBuf, output: PathBuf) -> anyhow::Result<()> {
    info!("Indexing 'base' tarball...");
    let base_tar = TarChunkFetcher::new(&base)?;
    info!("Indexing 'new' tarball...");
    let new_tar = TarChunkFetcher::new(&new)?;

    do_diff(base_tar, new_tar, output)?;
    Ok(())
}

#[test]
fn test() {}
