#![feature(decl_macro)]

use crate::cli::Commands;
use clap::Parser;
use flate2::Compression;
use rayon::prelude::*;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use wplace_tools::{
    CHUNK_LENGTH, MUTATION_MASK, PALETTE_INDEX_MASK, collect_chunks, new_chunk_file,
    read_index_file, read_png, stylized_progress_bar, write_index_file, write_png,
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

/// Returns if the two image data is identical.
#[inline(always)]
fn diff_png(
    base: impl AsRef<Path>,
    new: impl AsRef<Path>,
    diff_out: impl AsRef<Path>,
) -> anyhow::Result<bool> {
    let base = base.as_ref();
    let new = new.as_ref();

    let mut buffers = Buffers::default();
    let (buf1, buf2, diff_buf) = buffers.split_mut();

    if base.exists() {
        read_png(base, buf1)?;
    }
    read_png(new, buf2)?;

    // It's expecting that a large percent of the chunks are not mutated.
    // Thus in this case, disabling further diff creation can reduce the process time.
    if base.exists() && buf1 == buf2 {
        return Ok(true);
    }

    for i in 0..CHUNK_LENGTH {
        // They shouldn't have two highest ones. Coerce them.
        let i1 = buf1[i] & PALETTE_INDEX_MASK;
        let i2 = buf2[i] & PALETTE_INDEX_MASK;
        if i1 != i2 {
            diff_buf[i] = i2 | MUTATION_MASK;
        }
    }

    let out_file = BufWriter::new(File::create(diff_out)?);
    let mut compressor = flate2::write::DeflateEncoder::new(out_file, Compression::default());
    compressor.write_all(diff_buf)?;

    Ok(false)
}

fn apply_png(
    base: impl AsRef<Path>,
    diff: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let mut diff_buf = vec![0_u8; CHUNK_LENGTH];
    let mut base_buf = vec![0_u8; CHUNK_LENGTH];

    let in_reader = BufReader::new(File::open(diff)?);
    let mut decompressor = flate2::read::DeflateDecoder::new(in_reader);
    decompressor.read_exact(&mut diff_buf)?;
    if base.as_ref().exists() {
        read_png(base, &mut base_buf)?;
    }

    for i in 0..CHUNK_LENGTH {
        // has mutation flag - apply the pixel
        if diff_buf[i] & MUTATION_MASK == MUTATION_MASK {
            base_buf[i] = diff_buf[i] & PALETTE_INDEX_MASK;
        }
    }

    write_png(output, &base_buf)?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = cli::Cli::parse();
    match args.command {
        Commands::Diff { base, new, output } => {
            fs::create_dir_all(&output)?;
            println!("Collecting files...");
            let collected = collect_chunks(&new, None)?;
            // files that make up a full snapshot
            let index_file = output.join("index.txt");
            write_index_file(index_file, &collected)?;

            println!("Processing {} files...", collected.len());
            let progress = stylized_progress_bar(collected.len() as u64);

            // For some unknown reason, when I use ThreadLocal or even manual unsafe per-thread
            // object indexing (code commented out below), the performance gets even worse A LOT
            // compared to the origin
            // version (allocating memory every time on `diff_png` called). Don't know why.

            // let mut buffers = vec![Buffers::default(); rayon::current_num_threads()];
            // let buffers_ptr = SharedBuffer(buffers.as_mut_ptr());

            collected.into_par_iter().for_each(|(x, y)| {
                let base_file = base.join(format!("{x}/{y}.png"));
                let new_file = new.join(format!("{x}/{y}.png"));
                let diff_file = new_chunk_file(&output, (x, y), "bin");

                // let buffers_ptr_ref = buffers_ptr;

                // let local_buffers = unsafe {
                //     &mut *buffers_ptr_ref
                //         .0
                //         .add(rayon::current_thread_index().unwrap())
                // };

                let _ = diff_png(base_file, new_file, diff_file).unwrap();
                progress.inc(1);
            });

            progress.finish();
        }
        Commands::Apply { base, diff, output } => {
            fs::create_dir_all(&output)?;
            println!("Collecting files...");
            let index = read_index_file(diff.join("index.txt"))?;
            println!("Processing {} files...", index.len());
            let progress = stylized_progress_bar(index.len() as u64);

            index.into_par_iter().for_each(|(x, y)| {
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
            });

            progress.finish();
        }
        Commands::Compare { base, new } => {
            println!("Collecting files 'base'...");
            let mut base_collected = collect_chunks(&base, None)?;
            println!("Collecting files 'new'...");
            let mut new_collected = collect_chunks(&new, None)?;

            base_collected.sort();
            new_collected.sort();
            if base_collected != new_collected {
                return Err(anyhow::anyhow!("File lists differ."));
            }

            let length = base_collected.len();
            println!("Processing {} files...", length);
            let progress = stylized_progress_bar(length as u64);

            base_collected.into_par_iter().for_each(|(x, y)| {
                let base_file = base.join(format!("{x}/{y}.png"));
                let new_file = new.join(format!("{x}/{y}.png"));
                let result = compare_png(&base_file, &new_file);
                if result.is_err() {
                    println!("{x}/{y}");
                }
                if !result.unwrap() {
                    eprintln!("{} and {} differ", base_file.display(), new_file.display());
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
            println!("Collecting files...");
            let collected = collect_chunks(&base, tiles_range_arg.parse())?;
            println!("Processing {} files...", collected.len());
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
