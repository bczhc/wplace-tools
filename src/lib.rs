#![feature(file_buffered)]
#![feature(yeet_expr)]
#![feature(decl_macro)]
#![feature(try_blocks)]
#![feature(likely_unlikely)]
#![warn(clippy::all, clippy::nursery)]

pub mod checksum;
pub mod diff2;
pub mod indexed_png;
pub mod tar;
pub mod zip;

use crate::checksum::chunk_checksum;
use crate::indexed_png::{read_png, read_png_reader, write_png};
use crate::tar::ChunksTarReader;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_regex::regex;
use log::error;
use pathdiff::diff_paths;
use regex::Regex;
use std::env::set_var;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Take};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::{env, fmt, fs, hint, io};
use walkdir::WalkDir;
use yeet_ops::yeet;

pub const CHUNK_WIDTH: usize = 1000;
pub const CHUNK_LENGTH: usize = CHUNK_WIDTH * CHUNK_WIDTH;
pub const CHUNK_DIMENSION: (u32, u32) = (CHUNK_WIDTH as u32, CHUNK_WIDTH as u32);
pub const MUTATION_MASK: u8 = 0b0100_0000;
pub const PALETTE_INDEX_MASK: u8 = 0b0011_1111;

pub type ChunkNumber = (u16, u16);

/// This is the global unique palette. Not the one as in png (palettes in png files are dynamically set)!
pub const GLOBAL_PALETTE: [[u8; 3]; 64] = [
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

pub fn collect_chunks(
    dir: impl AsRef<Path>,
    tiles_range: Option<TilesRange>,
) -> anyhow::Result<Vec<ChunkNumber>> {
    let mut collected = Vec::new();
    for x in WalkDir::new(&dir) {
        let entry = x?;
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
            c1.as_os_str().to_str().map(|x| x.parse()),
            c2.as_os_str().to_str().map(|x| x.parse()),
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

pub fn stylized_progress_bar(len: u64) -> ProgressBar {
    let pb = ProgressBar::new(len);
    pb.set_style(
        #[allow(clippy::literal_string_with_formatting_args)]
        ProgressStyle::with_template("[{elapsed_precise}] {wide_bar} {pos:>}/{len:7} {eta}")
            .unwrap()
            .progress_chars(">>-"),
    );
    pb
}

#[derive(Copy, Clone)]
pub struct TilesRange {
    pub x_min: u16,
    pub x_max: u16,
    pub y_min: u16,
    pub y_max: u16,
}

impl TilesRange {
    pub fn parse_str(s: &str) -> Option<Self> {
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

/// Build the specified chunk file and create its parent folder if necessary.
#[inline(always)]
pub fn new_chunk_file(root: impl AsRef<Path>, (x, y): ChunkNumber, ext: &str) -> PathBuf {
    let subpath = root.as_ref().join(format!("{x}"));
    let path = subpath.join(format!("{y}.{ext}"));
    fs::create_dir_all(subpath).unwrap();
    path
}

pub fn set_up_logger() {
    if env::var("RUST_LOG").is_err() {
        unsafe {
            set_var("RUST_LOG", "info");
        }
    }
    env_logger::init();
}

pub macro unwrap_os_str($x:expr) {
    $x.to_str().expect("Invalid UTF-8")
}

pub fn extract_datetime(s: &str) -> Option<String> {
    let regex = regex!(r"(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{3}Z)");
    Some(regex.captures_iter(s).next()?.get(1)?.as_str().to_string())
}

#[inline(always)]
pub fn apply_chunk(base: &mut [u8], diff_data: &[u8; CHUNK_LENGTH]) {
    for (base_pix, diff_pix) in base.iter_mut().zip(diff_data) {
        if hint::unlikely(diff_pix & MUTATION_MASK == MUTATION_MASK) {
            // has mutation flag - apply the pixel
            *base_pix = diff_pix & PALETTE_INDEX_MASK;
        }
    }
}

#[inline(always)]
pub fn open_file_range(
    path: impl AsRef<Path>,
    pos: u64,
    len: u64,
) -> io::Result<Take<BufReader<File>>> {
    let mut file = File::open_buffered(path)?;
    file.seek(SeekFrom::Start(pos))?;
    Ok(file.take(len))
}

#[inline(always)]
pub fn flate2_decompress(reader: impl Read, buf: &mut [u8]) -> io::Result<()> {
    let mut read = flate2::read::DeflateDecoder::new(reader);
    read.read_exact(buf)
}

#[inline(always)]
pub fn validate_chunk_checksum(chunk: &[u8], checksum: u32) -> anyhow::Result<()> {
    if chunk_checksum(chunk) != checksum {
        yeet!(anyhow::anyhow!("Checksum not matched!"));
    }
    Ok(())
}

pub fn quick_capture<'a>(haystack: &'a str, pattern: &Regex) -> Option<Vec<&'a str>> {
    let capture = pattern.captures(haystack)?;
    Some(
        capture
            .iter()
            .skip(1)
            .flat_map(|x| x.map(|x| x.as_str()))
            .collect(),
    )
}

pub trait ExitOnError<T, E>
where
    E: Display,
    Self: Sized,
{
    fn exit_on_error(self) -> T;
}

impl<T, E: Display> ExitOnError<T, E> for Result<T, E>
where
    Self: Sized,
{
    #[inline(always)]
    fn exit_on_error(self) -> T {
        self.unwrap_or_else(|e| {
            error!("Error occurred: {e}");
            exit(1)
        })
    }
}

/// Canvas for chunk image merging.
pub struct Canvas {
    pub buf: Vec<u8>,
    min_chunk: ChunkNumber,
    pub dimension: (usize, usize),
}

impl Canvas {
    pub fn new(chunk_num_x: u16, chunk_num_y: u16, min_chunk: ChunkNumber) -> Self {
        let dimension = (
            chunk_num_x as usize * CHUNK_WIDTH,
            chunk_num_y as usize * CHUNK_WIDTH,
        );
        let buf = vec![0_u8; dimension.0 * dimension.1];
        Self {
            buf,
            min_chunk,
            dimension,
        }
    }

    pub fn from_chunk_list(chunks: impl Iterator<Item = ChunkNumber>) -> Self {
        let chunks = chunks.collect::<Vec<_>>();
        let min_x = chunks.iter().map(|x| x.0).min().unwrap();
        let max_x = chunks.iter().map(|x| x.0).max().unwrap();
        let min_y = chunks.iter().map(|x| x.1).min().unwrap();
        let max_y = chunks.iter().map(|x| x.1).max().unwrap();
        Self::new(max_x - min_x + 1, max_y - min_y + 1, (min_x, min_y))
    }

    pub fn copy(&mut self, n: ChunkNumber, buf: &[u8; CHUNK_LENGTH]) {
        macro chunk_pixel($buf:expr, $x:expr, $y:expr) {
            $buf[$y * CHUNK_WIDTH + $x]
        }
        macro canvas_pixel($buf:expr, $x:expr, $y:expr) {
            $buf[$y * self.dimension.0 + $x]
        }

        let (chunk_x, chunk_y) = n;
        let (min_x, min_y) = self.min_chunk;

        let rel_x = (chunk_x - min_x) as usize * CHUNK_WIDTH;
        let rel_y = (chunk_y - min_y) as usize * CHUNK_WIDTH;

        for y in 0..CHUNK_WIDTH {
            for x in 0..CHUNK_WIDTH {
                canvas_pixel!(self.buf, (rel_x + x), (rel_y + y)) = chunk_pixel!(buf, x, y);
            }
        }
    }

    #[allow(unused)]
    pub fn save(&self, out: impl AsRef<Path>) -> anyhow::Result<()> {
        write_png(
            out,
            (self.dimension.0 as u32, self.dimension.1 as u32),
            &self.buf,
        )?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ChunkProcessError {
    pub inner: anyhow::Error,
    pub chunk_number: ChunkNumber,
    pub diff_file: Option<String>,
}

impl Display for ChunkProcessError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)?;
        writeln!(f)?;
        writeln!(f, "Chunk number: {:?}", self.chunk_number)?;
        if let Some(s) = &self.diff_file {
            writeln!(f, "Diff file: {s}")?;
        }
        Ok(())
    }
}

pub trait ChunkFetcher {
    fn chunks_iter(&self) -> Box<dyn Iterator<Item = ChunkNumber> + Send + '_>;

    fn chunks_len(&self) -> usize;

    fn fetch(&self, n: ChunkNumber, buf: &mut [u8]) -> anyhow::Result<bool>;

    fn fetch_raw(&self, n: ChunkNumber) -> anyhow::Result<Vec<u8>>;
}

pub struct DirChunkFetcher {
    root: PathBuf,
    chunks: Option<Vec<ChunkNumber>>,
}

impl DirChunkFetcher {
    pub fn new(root: impl AsRef<Path>, index_all: bool) -> anyhow::Result<Self> {
        Ok(Self {
            root: root.as_ref().into(),
            chunks: if index_all {
                Some(collect_chunks(root, None)?)
            } else {
                None
            },
        })
    }
}

impl ChunkFetcher for DirChunkFetcher {
    fn chunks_iter(&self) -> Box<dyn Iterator<Item = ChunkNumber> + Send + '_> {
        let Some(c) = self.chunks.as_ref() else {
            return Box::new(std::iter::empty());
        };
        Box::new(c.iter().copied())
    }

    fn chunks_len(&self) -> usize {
        self.chunks.as_ref().map_or(0, |x| x.len())
    }

    fn fetch(&self, (x, y): ChunkNumber, buf: &mut [u8]) -> anyhow::Result<bool> {
        let file = self.root.join(format!("{x}/{y}.png"));
        if !file.exists() {
            return Ok(false);
        }
        read_png(&file, buf)?;
        Ok(true)
    }

    fn fetch_raw(&self, (x, y): ChunkNumber) -> anyhow::Result<Vec<u8>> {
        let file = self.root.join(format!("{x}/{y}.png"));
        let mut vec = Vec::new();
        if file.exists() {
            File::open_buffered(&file)?.read_to_end(&mut vec)?;
        }
        Ok(vec)
    }
}

pub struct TarChunkFetcher {
    reader: ChunksTarReader,
}

impl TarChunkFetcher {
    pub fn new(tar: impl AsRef<Path>) -> anyhow::Result<Self> {
        let reader = ChunksTarReader::open_with_index(tar)?;
        Ok(Self { reader })
    }
}

impl ChunkFetcher for TarChunkFetcher {
    fn chunks_iter(&self) -> Box<dyn Iterator<Item = ChunkNumber> + Send + '_> {
        Box::new(self.reader.map.keys().copied())
    }

    fn chunks_len(&self) -> usize {
        self.reader.map.len()
    }

    fn fetch(&self, n: ChunkNumber, buf: &mut [u8]) -> anyhow::Result<bool> {
        let Some(c) = self.reader.open_chunk(n) else {
            return Ok(false);
        };
        read_png_reader(c?, buf)?;
        Ok(true)
    }

    fn fetch_raw(&self, n: ChunkNumber) -> anyhow::Result<Vec<u8>> {
        let Some(c) = self.reader.open_chunk(n) else {
            return Ok(vec![]);
        };
        let mut vec = Vec::new();
        io::copy(&mut c?, &mut vec)?;
        Ok(vec)
    }
}
