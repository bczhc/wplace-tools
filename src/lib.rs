use anyhow::anyhow;
use indicatif::{ProgressBar, ProgressStyle};
use once_cell::sync::Lazy;
use pathdiff::diff_paths;
use png::{BitDepth, ColorType, Info};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Write};
use std::path::Path;
use walkdir::WalkDir;

pub const CHUNK_LENGTH: usize = 1_000_000;
pub const MUTATION_MASK: u8 = 0b0100_0000;
pub const PALETTE_INDEX_MASK: u8 = 0b0011_1111;

/// This is the global unique palette. Not the one as in png (palettes in png files are dynamically set)!
pub const PALETTE: [[u8; 3]; 64] = [
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

pub static PALETTE_MAP: Lazy<HashMap<[u8; 3], u8>> = Lazy::new(|| {
    PALETTE
        .iter()
        .enumerate()
        .skip(1)
        .map(|(a, b)| (*b, a as u8))
        .collect()
});

pub fn collect_chunks(
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

pub fn read_png(path: impl AsRef<Path>, buf: &mut [u8]) -> anyhow::Result<()> {
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
pub fn write_png(path: impl AsRef<Path>, buf: &[u8]) -> anyhow::Result<()> {
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

    let writer = BufWriter::new(File::create(path)?);
    let encoder = png::Encoder::with_info(writer, PNG_INFO.clone())?;
    let mut writer = encoder.write_header()?;
    writer.write_image_data(buf)?;
    Ok(())
}

pub struct Progress {
    pb: ProgressBar,
}

impl Progress {
    pub fn new(len: u64) -> anyhow::Result<Self> {
        let pb = ProgressBar::new(len);
        pb.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] {wide_bar} {pos:>}/{len:7} {eta}")?
                .progress_chars(">>-"),
        );
        Ok(Self { pb })
    }

    pub fn finish(&self) {
        self.pb.finish();
    }

    #[inline(always)]
    pub fn inc(&self, delta: u64) {
        self.pb.inc(1);
    }
}

#[derive(Copy, Clone)]
pub struct TilesRange {
    pub x_min: u32,
    pub x_max: u32,
    pub y_min: u32,
    pub y_max: u32,
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
