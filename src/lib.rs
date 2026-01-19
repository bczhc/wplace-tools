#![feature(file_buffered)]
#![feature(yeet_expr)]
#![feature(decl_macro)]
#![feature(try_blocks)]
#![feature(likely_unlikely)]
#![warn(clippy::all, clippy::nursery)]

pub mod checksum;
pub mod diff;
pub mod indexed_png;
pub mod tar;
pub mod zip;

use crate::checksum::chunk_checksum;
use crate::indexed_png::{read_png, read_png_reader, write_png};
use crate::tar::ChunksTarReader;
use anyhow::anyhow;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_regex::regex;
use log::error;
use pathdiff::diff_paths;
use regex::Regex;
use squashfs_reader::FileSystem;
use std::collections::BTreeMap;
use std::env::set_var;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Take, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Arc;
use std::{env, fmt, fs, io, iter};
use walkdir::WalkDir;
use yeet_ops::yeet;

pub const CHUNK_NUMBER_TOTAL: usize = 2048;
pub const CHUNK_WIDTH: usize = 1000;
pub const CHUNK_LENGTH: usize = CHUNK_WIDTH * CHUNK_WIDTH;
pub const CHUNK_DIMENSION: (u32, u32) = (CHUNK_WIDTH as u32, CHUNK_WIDTH as u32);
pub const MUTATION_MASK: u8 = 0b0100_0000;
pub const PALETTE_INDEX_MASK: u8 = 0b0011_1111;
pub const DIFF_DATA_ZSTD_COMPRESSION_LEVEL: i32 = 7;

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

pub type Iso8601Name = String;

pub fn extract_datetime(s: impl AsRef<OsStr>) -> Option<Iso8601Name> {
    let regex = regex!(r"(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{3}Z)");
    Some(
        regex
            .captures_iter(s.as_ref().to_str()?)
            .next()?
            .get(1)?
            .as_str()
            .to_string(),
    )
}

#[inline(always)]
pub fn apply_chunk(base: &mut [u8], diff_data: &[u8; CHUNK_LENGTH]) {
    for (base_pix, &diff_pix) in base.iter_mut().zip(diff_data.iter()) {
        let old_val = *base_pix;
        let new_val = diff_pix & PALETTE_INDEX_MASK;
        // has mutation flag - apply the pixel
        *base_pix = if (diff_pix & MUTATION_MASK) != 0 {
            new_val
        } else {
            old_val
        };
    }
}

#[inline(always)]
pub fn open_file_range(
    path: impl AsRef<Path>,
    pos: u64,
    len: u64,
) -> io::Result<Take<BufReader<File>>> {
    let file = File::open_buffered(path)?;
    reader_range(file, pos, len)
}

#[inline(always)]
pub fn reader_range<R: Read + Seek>(mut reader: R, pos: u64, len: u64) -> io::Result<Take<R>> {
    reader.seek(SeekFrom::Start(pos))?;
    Ok(reader.take(len))
}

#[inline(always)]
pub fn zstd_decompress(reader: impl Read, buf: &mut [u8]) -> io::Result<()> {
    zstd::Decoder::new(reader)?.read_exact(buf)
}

#[inline(always)]
pub fn zstd_compress_to(writer: impl Write, level: i32, buf: &[u8]) -> io::Result<()> {
    let mut encoder = zstd::Encoder::new(writer, level)?;
    encoder.write_all(buf)?;
    encoder.finish()?;
    Ok(())
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

pub trait AnyhowErrorExt<T> {
    fn exit_with_chunk_context(self, n: ChunkNumber, diff_file: Option<impl AsRef<Path>>) -> T;
}

impl<T> AnyhowErrorExt<T> for anyhow::Result<T> {
    #[inline(always)]
    fn exit_with_chunk_context(self, n: ChunkNumber, diff_file: Option<impl AsRef<Path>>) -> T {
        self.map_err(|e| ChunkProcessError {
            inner: e,
            chunk_number: n,
            diff_file: diff_file.map(|x| format!("{}", x.as_ref().display())),
        }).exit_on_error()
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
        writeln!(f, "Details: {:?}", self.inner)?;
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
            return Box::new(iter::empty());
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

pub trait ReadSeek: Read + Seek {}

impl<X: Read + Seek> ReadSeek for X {}

pub trait DiffFilesCollector {
    fn reader(&self, diff_name: &str) -> anyhow::Result<Box<dyn ReadSeek>>;

    fn contains(&self, diff_name: &str) -> bool;

    fn name_iter<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = &'a Iso8601Name> + 'a>;

    fn first(&self) -> Iso8601Name {
        self.name_iter()
            .next()
            // not empty
            .unwrap()
            .clone()
    }

    fn last(&self) -> Iso8601Name {
        self.name_iter()
            .last()
            // not empty
            .unwrap()
            .clone()
    }

    fn range_iter(
        &self,
        start_name: &str,
        end_name: &str,
    ) -> Box<dyn ExactSizeIterator<Item = Iso8601Name> + '_> {
        let result: Option<_> = try {
            let start_pos = self.name_iter().position(|x| x == start_name)?;
            let end_pos = self.name_iter().position(|x| x == end_name)?;
            let length = end_pos - start_pos + 1;
            let iter = self.name_iter().skip(start_pos).take(length).cloned();
            Box::new(iter) as Box<dyn ExactSizeIterator<Item = Iso8601Name>>
        };
        result.unwrap_or_else(|| Box::new(iter::empty()))
    }
}

pub struct DirDiffFilesCollector {
    pub names: BTreeMap<Iso8601Name, Arc<PathBuf>>,
}

impl DirDiffFilesCollector {
    pub fn new(root: impl IntoIterator<Item = impl AsRef<Path>>) -> anyhow::Result<Self> {
        let read_dir_and_append = |root: &Arc<PathBuf>,
                                   tree: &mut BTreeMap<Iso8601Name, Arc<PathBuf>>|
         -> anyhow::Result<()> {
            for x in WalkDir::new(&**root) {
                let x = x?;
                if x.path()
                    .extension()
                    .map(|x| x.to_ascii_lowercase())
                    .as_deref()
                    != Some(OsStr::new("diff"))
                {
                    continue;
                }
                if x.path().is_file() {
                    let filename = x
                        .file_name()
                        .to_str()
                        .ok_or_else(|| anyhow!("Invalid filename"))?;
                    tree.insert(
                        extract_datetime(filename)
                            .ok_or_else(|| anyhow!("Malformed diff filename"))?,
                        Arc::clone(root),
                    );
                }
            }
            Ok(())
        };

        let roots = root
            .into_iter()
            .map(|x| Arc::new(x.as_ref().to_path_buf()))
            .collect::<Vec<_>>();

        let mut tree = BTreeMap::new();
        for root in &roots {
            read_dir_and_append(root, &mut tree)?;
        }
        if tree.is_empty() {
            yeet!(anyhow!("No diff files found from diff sources"));
        }
        Ok(Self { names: tree })
    }
}

impl DiffFilesCollector for DirDiffFilesCollector {
    fn reader(&self, diff_name: &str) -> anyhow::Result<Box<dyn ReadSeek>> {
        let root = self
            .names
            .get(diff_name)
            .ok_or_else(|| anyhow!("No entry: {diff_name}"))?;
        Ok(Box::new(File::open_buffered(
            root.join(format!("{diff_name}.diff")),
        )?))
    }

    fn contains(&self, diff_name: &str) -> bool {
        self.names.contains_key(diff_name)
    }

    fn name_iter<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = &'a Iso8601Name> + 'a> {
        Box::new(self.names.iter().map(|x| x.0))
    }
}

pub struct SqfsDiffFilesCollector {
    fs_list: Vec<FileSystem<File>>,
    names: BTreeMap<Iso8601Name, usize /* index of fs_list */>,
}

impl SqfsDiffFilesCollector {
    pub fn new(images: impl IntoIterator<Item = impl AsRef<Path>>) -> anyhow::Result<Self> {
        let mut fs_vec = Vec::new();
        let mut tree = BTreeMap::new();

        for (idx, path) in images.into_iter().enumerate() {
            let fs = FileSystem::from_path(path.as_ref())?;
            let root = fs.read_dir("/")?;
            for x in root {
                let e = x?;
                let Some(name) = extract_datetime(e.name()) else {
                    continue;
                };
                tree.insert(name, idx);
            }
            fs_vec.push(fs);
        }
        Ok(Self {
            fs_list: fs_vec,
            names: tree,
        })
    }
}

impl DiffFilesCollector for SqfsDiffFilesCollector {
    fn reader(&self, diff_name: &str) -> anyhow::Result<Box<dyn ReadSeek>> {
        let &index = self
            .names
            .get(diff_name)
            .ok_or_else(|| anyhow!("No such name"))?;
        let fs = &self.fs_list[index];
        let reader = fs.open(format!("{diff_name}.diff"))?;
        Ok(Box::new(reader))
    }

    fn contains(&self, diff_name: &str) -> bool {
        self.names.contains_key(diff_name)
    }

    fn name_iter<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = &'a Iso8601Name> + 'a> {
        Box::new(self.names.iter().map(|x| x.0))
    }
}

pub macro chunk_buf() {
    vec![0_u8; CHUNK_LENGTH]
}
