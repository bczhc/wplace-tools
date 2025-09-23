#![feature(decl_macro)]
#![feature(file_buffered)]

use crate::cli::Commands;
use byteorder::{ByteOrder, LE};
use clap::Parser;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use log::info;
use rayon::prelude::*;
use std::fs;
use std::fs::File;
use std::io::{stdout, BufReader, BufWriter, Read};
use std::path::Path;
use std::sync::mpsc::sync_channel;
use std::thread::spawn;
use std::time::{SystemTime, UNIX_EPOCH};
use wplace_tools::diff_file::{DiffFileReader, DiffFileWriter, Metadata};
use wplace_tools::{
    collect_chunks, new_chunk_file, read_png, set_up_logger, stylized_progress_bar, unwrap_os_str,
    write_png, CHUNK_LENGTH, MUTATION_MASK, PALETTE_INDEX_MASK,
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

/// Returns None if the two image data is identical.
#[inline(always)]
fn diff_png(base: impl AsRef<Path>, new: impl AsRef<Path>) -> anyhow::Result<Option<Vec<u8>>> {
    let base = base.as_ref();
    let new = new.as_ref();

    let buffers = Buffers::default();
    let (mut buf1, mut buf2, mut diff_buf) = (buffers.b1, buffers.b2, buffers.b3);

    if base.exists() {
        read_png(base, &mut buf1)?;
    }
    read_png(new, &mut buf2)?;

    // It's expecting that a large percent of the chunks are not mutated.
    // Thus in this case, disabling further diff creation can reduce the process time.
    if base.exists() && buf1 == buf2 {
        return Ok(None);
    }

    for i in 0..CHUNK_LENGTH {
        // They shouldn't have two highest ones. Coerce them.
        let i1 = buf1[i] & PALETTE_INDEX_MASK;
        let i2 = buf2[i] & PALETTE_INDEX_MASK;
        if i1 != i2 {
            diff_buf[i] = i2 | MUTATION_MASK;
        }
    }

    Ok(Some(diff_buf))
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

    write_png(output, &base_buf)?;

    Ok(())
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
            let out_stream = BufWriter::new(stdout().lock());
            let out_stream = DeflateEncoder::new(out_stream, Compression::default());
            let mut diff_file = DiffFileWriter::new(
                out_stream,
                Metadata {
                    name: this_name.into(),
                    parent: parent_name.into(),
                    creation_time: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                },
                &collected,
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

            for (x, y, diff) in rx {
                diff_file.add_chunk_diff((x, y), &diff)?;
            }
        }
        Commands::Apply { base, diff, output } => {
            info!("Opening diff file...");
            let diff_file = DiffFileReader::new(File::open_buffered(&diff)?)?;
            let index = &diff_file.index;
            info!(
                "Total chunks: {}, parent: {}, this: {}, creation time: {}",
                index.len(),
                diff_file.metadata.parent,
                diff_file.metadata.name,
                diff_file.metadata.creation_time
            );

            info!("Processing {} files...", index.len());
            let progress = stylized_progress_bar(index.len() as u64);

            let iter = diff_file.chunk_diff_iter()?;
            for x in iter {
                let x = x?;
                let chunk_x = LE::read_u16(&x[0..2]);
                let chunk_y = LE::read_u16(&x[2..4]);
                let diff_data = &x[4..];
                let base_file = base.join(format!("{chunk_x}/{chunk_y}.png"));
                let output_file = new_chunk_file(&output, (chunk_x, chunk_y), "png");
                apply_png(base_file, output_file, diff_data.try_into().unwrap())?;
            }
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

            base_collected.into_par_iter().for_each(|(x, y)| {
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
