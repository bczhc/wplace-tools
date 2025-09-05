#![feature(decl_macro)]

use crate::cli::Commands;
use anyhow::anyhow;
use clap::Parser;
use flate2::Compression;
use indicatif::{ProgressBar, ProgressStyle};
use once_cell::sync::Lazy;
use pathdiff::diff_paths;
use png::{BitDepth, ColorType, Info};
use rayon::prelude::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;
use walkdir::WalkDir;

const CHUNK_LENGTH: usize = 1_000_000;
const MUTATION_MASK: u8 = 0b0100_0000;
const PALETTE_INDEX_MASK: u8 = 0b0011_1111;

/// This is the global unique palette. Not the one as in png (palettes in png files are dynamically set)!
const PALETTE: [[u8; 3]; 64] = [
    // transparency
    [0, 0, 0],
    // black
    [0, 0, 0],
    [60, 60, 60],
    [120, 120, 120],
    [170, 170, 170],
    [210, 210, 210],
    [255, 255, 255],
    [96, 0, 24],
    [165, 14, 30],
    [237, 28, 36],
    [250, 128, 114],
    [228, 92, 26],
    [255, 127, 39],
    [246, 170, 9],
    [249, 221, 59],
    [255, 250, 188],
    [156, 132, 49],
    [197, 173, 49],
    [232, 212, 95],
    [74, 107, 58],
    [90, 148, 74],
    [132, 197, 115],
    [14, 185, 104],
    [19, 230, 123],
    [135, 255, 94],
    [12, 129, 110],
    [16, 174, 166],
    [19, 225, 190],
    [15, 121, 159],
    [96, 247, 242],
    [187, 250, 242],
    [40, 80, 158],
    [64, 147, 228],
    [125, 199, 255],
    [77, 49, 184],
    [107, 80, 246],
    [153, 177, 251],
    [74, 66, 132],
    [122, 113, 196],
    [181, 174, 241],
    [120, 12, 153],
    [170, 56, 185],
    [224, 159, 249],
    [203, 0, 122],
    [236, 31, 128],
    [243, 141, 169],
    [155, 82, 73],
    [209, 128, 120],
    [250, 182, 164],
    [104, 70, 52],
    [149, 104, 42],
    [219, 164, 99],
    [123, 99, 82],
    [156, 132, 107],
    [214, 181, 148],
    [209, 128, 81],
    [248, 178, 119],
    [255, 197, 165],
    [109, 100, 63],
    [148, 140, 107],
    [205, 197, 158],
    [51, 57, 65],
    [109, 117, 141],
    [179, 185, 209],
];

static PALETTE_MAP: Lazy<HashMap<[u8; 3], u8>> = Lazy::new(|| {
    PALETTE
        .iter()
        .enumerate()
        .skip(1)
        .map(|(a, b)| (*b, a as u8))
        .collect()
});

/// Map the inner png colors to the uniform [PALETTE] indices.
fn png_map_palette(palette: &[u8], index: u8, alpha_pos: usize) -> u8 {
    let index = index as usize;
    if index == alpha_pos {
        return 0;
    }
    if index >= palette.len() - 2 {
        panic!("invalid color index!");
    }
    let rgb = <[u8; 3]>::try_from(&palette[(index * 3)..(index * 3 + 3)]).unwrap();
    PALETTE_MAP[&rgb]
}

static PNG_INFO: Lazy<Info> = Lazy::new(|| {
    let mut palette_buf = Cursor::new([0_u8; 64 * 3]);
    for x in PALETTE {
        palette_buf.write_all(&x).unwrap();
    }

    let mut new_info = Info::with_size(1000, 1000);
    new_info.bit_depth = BitDepth::Eight;
    new_info.color_type = ColorType::Indexed;
    // png palette #0 is transparency
    new_info.trns = Some(Cow::from(&[0_u8]));
    new_info.palette = Some(Cow::Owned(palette_buf.get_ref().to_vec()));
    new_info
});

mod cli {
    use crate::TilesRange;
    use clap::{Args, Parser, Subcommand, ValueHint};
    use std::path::PathBuf;

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

fn read_png(path: impl AsRef<Path>, buf: &mut [u8]) -> anyhow::Result<()> {
    let png = png::Decoder::new(BufReader::new(File::open(&path)?));
    let mut reader = png.read_info()?;
    let png_buf_size = reader
        .output_buffer_size()
        .ok_or(anyhow!("Cannot read output buffer size"))?;
    reader.next_frame(buf)?;

    let info = reader.info();
    let palette = info.palette.as_ref().ok_or(anyhow!("No palette"))?.as_ref();
    // this denotes which palette slot is a transparency
    let alpha_pos = info
        .trns
        .as_ref()
        .and_then(|x| x.as_ref().iter().rposition(|x| *x == 0))
        // I may expect if a chunk were painted fully (that's, no transparency pixels at all),
        // the PNG encoder from Wplace may not put a `0` in the `trns` array. Just put a dummy
        // value here.
        .unwrap_or(usize::MAX);
    assert!(alpha_pos < palette.len() || alpha_pos == usize::MAX);

    match info.bit_depth {
        BitDepth::One => {
            assert_eq!(png_buf_size, CHUNK_LENGTH / 8);
            for i in (0..png_buf_size).rev() {
                let byte = buf[i];
                let base = i * 8;
                buf[base + 7] = png_map_palette(palette, byte & 1, alpha_pos);
                buf[base + 6] = png_map_palette(palette, (byte >> 1) & 1, alpha_pos);
                buf[base + 5] = png_map_palette(palette, (byte >> 2) & 1, alpha_pos);
                buf[base + 4] = png_map_palette(palette, (byte >> 3) & 1, alpha_pos);
                buf[base + 3] = png_map_palette(palette, (byte >> 4) & 1, alpha_pos);
                buf[base + 2] = png_map_palette(palette, (byte >> 5) & 1, alpha_pos);
                buf[base + 1] = png_map_palette(palette, (byte >> 6) & 1, alpha_pos);
                buf[base] = png_map_palette(palette, (byte >> 7) & 1, alpha_pos);
            }
        }
        BitDepth::Two => {
            assert_eq!(png_buf_size, CHUNK_LENGTH / 4);
            for i in (0..png_buf_size).rev() {
                let byte = buf[i];

                buf[i * 4 + 3] = png_map_palette(palette, byte & 0b11, alpha_pos);
                buf[i * 4 + 2] = png_map_palette(palette, (byte >> 2) & 0b11, alpha_pos);
                buf[i * 4 + 1] = png_map_palette(palette, (byte >> 4) & 0b11, alpha_pos);
                buf[i * 4] = png_map_palette(palette, (byte >> 6) & 0b11, alpha_pos);
            }
        }
        BitDepth::Four => {
            assert_eq!(png_buf_size, CHUNK_LENGTH / 2);
            for i in (0..png_buf_size).rev() {
                let byte = buf[i];
                let base = i * 2;
                buf[base + 1] = png_map_palette(palette, byte & 0b1111, alpha_pos);
                buf[base] = png_map_palette(palette, (byte >> 4) & 0b1111, alpha_pos);
            }
        }
        BitDepth::Eight => {
            assert_eq!(png_buf_size, CHUNK_LENGTH);
            for i in (0..buf.len()).rev() {
                buf[i] = png_map_palette(palette, buf[i], alpha_pos);
            }
        }
        BitDepth::Sixteen => {
            unreachable!()
        }
    };

    Ok(())
}

#[inline(always)]
fn write_png(path: impl AsRef<Path>, buf: &[u8]) -> anyhow::Result<()> {
    let writer = BufWriter::new(File::create(path)?);
    let encoder = png::Encoder::with_info(writer, PNG_INFO.clone())?;
    let mut writer = encoder.write_header()?;
    writer.write_image_data(&buf)?;
    Ok(())
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

    let mut out_file = BufWriter::new(File::create(diff_out)?);
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

fn collect_chunks(
    dir: impl AsRef<Path>,
    tiles_range: Option<TilesRange>,
) -> anyhow::Result<Vec<(u32, u32)>> {
    let mut collected = Vec::new();
    for x in WalkDir::new(&dir) {
        let entry = x.unwrap();
        let path = entry.path();
        let Some(mut subpath) = diff_paths(path, &dir) else {
            continue;
        };
        if !path.is_file() {
            continue;
        }
        subpath.set_extension("");
        let mut c = subpath.components();
        let Some(c1) = c.next() else {
            continue;
        };
        let Some(c2) = c.next() else {
            continue;
        };
        let (Some(Ok(c1)), Some(Ok(c2))) = (
            c1.as_os_str().to_str().map(|x| x.parse::<u32>()),
            c2.as_os_str().to_str().map(|x| x.parse::<u32>()),
        ) else {
            continue;
        };
        if let Some(ref r) = tiles_range {
            if (r.x_min..=r.x_max).contains(&c1) && (r.y_min..=r.y_max).contains(&c2) {
                collected.push((c1, c2));
            }
        } else {
            collected.push((c1, c2));
        }
    }
    collected.sort();
    Ok(collected)
}

struct Progress {
    pb: ProgressBar,
}

impl Progress {
    fn new(len: u64) -> anyhow::Result<Self> {
        let pb = ProgressBar::new(len);
        pb.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] {wide_bar} {pos:>}/{len:7} {eta}")?
                .progress_chars(">>-"),
        );
        Ok(Self { pb })
    }
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
                progress.pb.inc(1);
            });

            progress.pb.finish_with_message("All done!");
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
                progress.pb.inc(1);
            });

            progress.pb.finish_with_message("All done!");
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
                progress.pb.inc(1);
            });
            progress.pb.finish_with_message("Done.");
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
                progress.pb.inc(1);
            });
            progress.pb.finish_with_message("Done.");
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

#[derive(Copy, Clone)]
struct TilesRange {
    x_min: u32,
    x_max: u32,
    y_min: u32,
    y_max: u32,
}

impl TilesRange {
    fn parse_str(s: &str) -> Option<Self> {
        let split = s.split(",").collect::<Vec<_>>();
        if split.len() != 4 {
            return None;
        }
        Some(Self {
            x_min: split[0].parse().ok()?,
            x_max: split[1].parse().ok()?,
            y_min: split[2].parse().ok()?,
            y_max: split[3].parse().ok()?,
        })
    }
}
