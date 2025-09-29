#![feature(file_buffered)]
#![feature(yeet_expr)]
#![feature(decl_macro)]
#![feature(try_blocks)]

pub mod checksum;
pub mod diff_file;
pub mod indexed_png;
pub mod zip;
pub mod tar;

use indicatif::{ProgressBar, ProgressStyle};
use pathdiff::diff_paths;
use std::env::set_var;
use std::path::{Path, PathBuf};
use std::{env, fs};
use lazy_regex::regex;
use walkdir::WalkDir;

pub const CHUNK_LENGTH: usize = 1_000_000;
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

pub fn extract_datetime(s: &str) -> String {
    let extract = |name: &str| {
        let regex = regex!(r"(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{3}Z)");
        regex
            .captures_iter(name)
            .next()
            .unwrap()
            .get(1)
            .unwrap()
            .as_str()
            .to_string()
    };
    extract(s)
}
