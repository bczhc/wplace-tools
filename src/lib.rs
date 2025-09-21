use anyhow::anyhow;
use indicatif::{ProgressBar, ProgressStyle};
use once_cell::sync::Lazy;
use pathdiff::diff_paths;
use png::{BitDepth, ColorType, Info};
use std::borrow::Cow;
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

/// This is used as a hashing algorithm.
const fn pack_rgb(rgb: &[u8]) -> u32 {
    ((rgb[0] as u32) << 16) | ((rgb[1] as u32) << 8) | (rgb[2] as u32)
}

/// Use large-array-based lookup table to get the index in O(1)
fn create_palette_lookup_table() -> Vec<u8> {
    let mut lut = vec![u8::MAX; 0xffffff + 1];
    for (index, x) in PALETTE.iter().enumerate().skip(1) {
        lut[pack_rgb(x) as usize] = index.try_into().unwrap();
    }
    lut
}

pub static mut PALETTE_LOOKUP_TABLE: Option<&[u8]> = None;

pub fn initialize_palette_lookup_table() {
    unsafe { PALETTE_LOOKUP_TABLE = Some(Vec::leak(create_palette_lookup_table())) }
}

pub fn collect_chunks(
    dir: impl AsRef<Path>,
    tiles_range: Option<TilesRange>,
) -> anyhow::Result<Vec<(u32, u32)>> {
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

/// Map indices in an indexed PNG -> indices in the global [PALETTE]
pub struct PixelMapper {
    /// Raw palette format: \[r0, g0, b0, r1, g1, b1, ...\]<br/>
    /// Grouped png_palette format: \[\[r0, g0, b0\], \[r1, g1, b1\], ...\]
    png_palette: Vec<[u8; 3]>,
    /// See: https://www.w3.org/TR/PNG-DataRep.html#DR.Alpha-channel
    trns: Vec<u8>,
    palette_lookup_table: &'static [u8],
}

impl PixelMapper {
    fn new(png_info: &Info) -> PixelMapper {
        let raw_palette = png_info.palette.as_ref().expect("No palette").to_vec();
        assert_eq!(raw_palette.len() % 3, 0);
        let palette_size = raw_palette.len() / 3;
        // group by three
        let png_palette = raw_palette
            .chunks(3)
            .collect::<Vec<_>>()
            .into_iter()
            .map(|x| {
                assert_eq!(x.len(), 3);
                <[u8; 3]>::try_from(x).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(png_palette.len(), palette_size);

        let mut expanded_trns = vec![0_u8; png_palette.len()];
        match png_info.trns.as_ref() {
            None => {
                // all colors in the raw palette are fully opaque
                expanded_trns.fill(255);
            }
            Some(trns) => {
                // If trns are not as long as raw_palette, expand it. The missing trns positions
                // should all indicate a full opaque.
                expanded_trns.write_all(trns).unwrap();
                expanded_trns[trns.len()..].fill(255);
            }
        };

        let lut = unsafe {
            #[allow(static_mut_refs)]
            if PALETTE_LOOKUP_TABLE.is_none() {
                initialize_palette_lookup_table();
            }
            PALETTE_LOOKUP_TABLE.unwrap()
        };

        Self {
            png_palette,
            trns: expanded_trns,
            palette_lookup_table: lut,
        }
    }

    #[inline(always)]
    pub fn map(&self, index: u8) -> u8 {
        let index = index as usize;
        if self.trns[index] == 0 {
            // this pixel is a transparency!
            // PALETTE[0] denotes a transparency
            return 0;
        }
        self.palette_lookup_table[pack_rgb(&self.png_palette[index]) as usize]
    }
}

pub fn read_png(path: impl AsRef<Path>, buf: &mut [u8]) -> anyhow::Result<()> {
    let png = png::Decoder::new(BufReader::new(File::open(&path)?));
    let mut reader = png.read_info()?;
    let png_buf_size = reader
        .output_buffer_size()
        .ok_or(anyhow!("Cannot read output buffer size"))?;
    reader.next_frame(buf)?;

    let info = reader.info();
    let pixel_mapper = PixelMapper::new(info);

    match info.bit_depth {
        BitDepth::One => {
            debug_assert_eq!(png_buf_size, CHUNK_LENGTH / 8);
            for i in (0..png_buf_size).rev() {
                let byte = buf[i];
                let base = i * 8;
                buf[base + 7] = pixel_mapper.map(byte & 1);
                buf[base + 6] = pixel_mapper.map((byte >> 1) & 1);
                buf[base + 5] = pixel_mapper.map((byte >> 2) & 1);
                buf[base + 4] = pixel_mapper.map((byte >> 3) & 1);
                buf[base + 3] = pixel_mapper.map((byte >> 4) & 1);
                buf[base + 2] = pixel_mapper.map((byte >> 5) & 1);
                buf[base + 1] = pixel_mapper.map((byte >> 6) & 1);
                buf[base] = pixel_mapper.map((byte >> 7) & 1);
            }
        }
        BitDepth::Two => {
            debug_assert_eq!(png_buf_size, CHUNK_LENGTH / 4);
            for i in (0..png_buf_size).rev() {
                let byte = buf[i];

                buf[i * 4 + 3] = pixel_mapper.map(byte & 0b11);
                buf[i * 4 + 2] = pixel_mapper.map((byte >> 2) & 0b11);
                buf[i * 4 + 1] = pixel_mapper.map((byte >> 4) & 0b11);
                buf[i * 4] = pixel_mapper.map((byte >> 6) & 0b11);
            }
        }
        BitDepth::Four => {
            debug_assert_eq!(png_buf_size, CHUNK_LENGTH / 2);
            for i in (0..png_buf_size).rev() {
                let byte = buf[i];
                let base = i * 2;
                buf[base + 1] = pixel_mapper.map(byte & 0b1111);
                buf[base] = pixel_mapper.map((byte >> 4) & 0b1111);
            }
        }
        BitDepth::Eight => {
            debug_assert_eq!(png_buf_size, CHUNK_LENGTH);
            for i in (0..buf.len()).rev() {
                buf[i] = pixel_mapper.map(buf[i]);
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
