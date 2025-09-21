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
    CHUNK_LENGTH, MUTATION_MASK, PALETTE_INDEX_MASK, Progress,
    collect_chunks, read_png, write_png,
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
        Diff {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "NEW", value_hint = ValueHint::FilePath)]
            new: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,

            #[command(flatten)]
            tiles_range_arg: TilesRangeArg,
        },

        Apply {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "DIFF", value_hint = ValueHint::FilePath)]
            diff: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,

            #[command(flatten)]
            tiles_range_arg: TilesRangeArg,
        },

        Compare {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "NEW", value_hint = ValueHint::FilePath)]
            new: PathBuf,

            #[command(flatten)]
            tiles_range_arg: TilesRangeArg,
        },

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

fn diff_png(
    base: impl AsRef<Path>,
    new: impl AsRef<Path>,
    diff_out: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let mut buffers = Buffers::default();
    let (buf1, buf2, diff_buf) = buffers.split_mut();

    if base.as_ref().exists() {
        read_png(base, buf1)?;
    } else {
        // buf1.fill(0);
    }
    read_png(new, buf2)?;

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

    Ok(())
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
        Commands::Diff {
            base,
            new,
            output,
            tiles_range_arg,
        } => {
            println!("Collecting files...");
            let collected = collect_chunks(&new, tiles_range_arg.parse())?;

            println!("Processing {} files...", collected.len());
            let progress = Progress::new(collected.len() as u64)?;

            // For some unknown reason, when I use ThreadLocal or even manual unsafe per-thread
            // object indexing (code commented out below), the performance gets even worse A LOT compared
            // to the origin
            // version (allocating memory every time on `diff_png` called). Don't know why.

            // let mut buffers = vec![Buffers::default(); rayon::current_num_threads()];
            // let buffers_ptr = SharedBuffer(buffers.as_mut_ptr());

            collected.into_par_iter().for_each(|(c1, c2)| {
                let base_file = base.join(format!("{c1}/{c2}.png"));
                let new_file = new.join(format!("{c1}/{c2}.png"));
                let diff_file = output.join(format!("{c1}/{c2}.bin"));
                // let buffers_ptr_ref = buffers_ptr;

                fs::create_dir_all(diff_file.parent().unwrap()).unwrap();

                // let local_buffers = unsafe {
                //     &mut *buffers_ptr_ref
                //         .0
                //         .add(rayon::current_thread_index().unwrap())
                // };

                diff_png(base_file, new_file, diff_file).unwrap();
                progress.inc(1);
            });

            progress.finish();
        }
        Commands::Apply {
            base,
            diff,
            output,
            tiles_range_arg,
        } => {
            println!("Collecting files...");
            let collected = collect_chunks(&diff, tiles_range_arg.parse())?;
            println!("Processing {} files...", collected.len());
            let progress = Progress::new(collected.len() as u64)?;

            collected.into_par_iter().for_each(|(c1, c2)| {
                let base_file = base.join(format!("{c1}/{c2}.png"));
                let diff_file = diff.join(format!("{c1}/{c2}.bin"));
                let output_file = output.join(format!("{c1}/{c2}.png"));

                fs::create_dir_all(output_file.parent().unwrap()).unwrap();

                apply_png(base_file, diff_file, output_file).unwrap();
                progress.inc(1);
            });

            progress.finish();
        }
        Commands::Compare {
            base,
            new,
            tiles_range_arg,
        } => {
            println!("Collecting files...");
            let collected = collect_chunks(&new, tiles_range_arg.parse())?;
            println!("Processing {} files...", collected.len());
            let progress = Progress::new(collected.len() as u64)?;

            collected.into_par_iter().for_each(|(c1, c2)| {
                let base_file = base.join(format!("{c1}/{c2}.png"));
                let new_file = new.join(format!("{c1}/{c2}.png"));
                if !compare_png(&base_file, &new_file).unwrap() {
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
            println!("Collecting files...");
            let collected = collect_chunks(&base, tiles_range_arg.parse())?;
            println!("Processing {} files...", collected.len());
            let progress = Progress::new(collected.len() as u64)?;

            collected.into_par_iter().for_each(|(c1, c2)| {
                let base_file = base.join(format!("{c1}/{c2}.png"));
                let output_file = output.join(format!("{c1}/{c2}.png"));
                fs::create_dir_all(output_file.parent().unwrap()).unwrap();
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
struct SharedBuffer(*mut Buffers);

unsafe impl Send for SharedBuffer {}
unsafe impl Sync for SharedBuffer {}
