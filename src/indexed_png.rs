use crate::GLOBAL_PALETTE;
use anyhow::anyhow;
use once_cell::sync::Lazy;
use png::{BitDepth, ColorType, Info};
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufRead, BufWriter, Cursor, Seek, Write};
use std::path::Path;

static PALETTE_DATA_IN_PNG: Lazy<[u8; 64 * 3]> = Lazy::new(|| {
    let mut palette_buf = Cursor::new([0_u8; 64 * 3]);
    for x in GLOBAL_PALETTE {
        palette_buf.write_all(&x).unwrap();
    }
    palette_buf.into_inner()
});

/// This is used as a hashing algorithm.
const fn pack_rgb(rgb: &[u8]) -> u32 {
    ((rgb[0] as u32) << 16) | ((rgb[1] as u32) << 8) | (rgb[2] as u32)
}

/// Use large-array-based lookup table to get the index in O(1)
fn create_palette_lookup_table() -> Vec<u8> {
    let mut lut = vec![u8::MAX; 0xffffff + 1];
    for (index, x) in GLOBAL_PALETTE.iter().enumerate().skip(1) {
        lut[pack_rgb(x) as usize] = index.try_into().unwrap();
    }
    lut
}

pub static mut PALETTE_LOOKUP_TABLE: Option<&[u8]> = None;

pub fn initialize_palette_lookup_table() {
    unsafe { PALETTE_LOOKUP_TABLE = Some(Vec::leak(create_palette_lookup_table())) }
}

/// Maps indices from a local PNG palette to the global Wplace palette.
///
/// This optimized version pre-computes all possible 8-bit mappings during
/// construction to ensure O(1) access with perfect cache locality.
///
/// This optimized version is created by Gemini.
pub struct PixelMapper<const MAX_COLORS: usize = 64> {
    /// A pre-computed mapping for every possible u8 index (0-255).
    /// This fits entirely in the CPU L1 cache, avoiding the 16MB LUT
    /// and RGB packing overhead during pixel iteration.
    fast_map: [u8; 256],
}

impl PixelMapper {
    /// Creates a new PixelMapper and pre-calculates the fast_map.
    pub fn new(png_info: &Info) -> Self {
        // 1. Extract the local palette from the PNG info [4]
        let raw_palette = png_info.palette.as_ref().expect("No palette found in PNG");
        assert_eq!(raw_palette.len() % 3, 0);
        let color_count = raw_palette.len() / 3;

        let mut png_palette = [[0_u8; 3]; 64];
        let mut groups = raw_palette.chunks(3);
        for x in png_palette.iter_mut().take(color_count) {
            *x = groups.next().unwrap().try_into().unwrap();
        }

        // 2. Handle transparency (tRNS) mapping
        // Alpha values: 0 = transparent, 255 = fully opaque.
        let mut expanded_trns = [255_u8; 64];
        if let Some(trns) = png_info.trns.as_ref() {
            (&mut expanded_trns[..]).write_all(trns).unwrap();
            // Remaining colors not specified in tRNS are opaque
            expanded_trns[trns.len()..].fill(255);
        }

        // 3. Access the 16MB Global Palette Lookup Table (LUT)
        let lut = unsafe {
            #[allow(static_mut_refs)]
            if PALETTE_LOOKUP_TABLE.is_none() {
                initialize_palette_lookup_table();
            }
            PALETTE_LOOKUP_TABLE.unwrap()
        };

        // 4. Pre-calculate all 256 possible mappings (LUT of LUT)
        // This shifts the computation cost from the pixel loop to the initialization.
        let mut fast_map = [0_u8; 256];
        for i in 0..256 {
            if i < color_count {
                if expanded_trns[i] == 0 {
                    // PNG transparency maps to Index 0 in the global palette
                    fast_map[i] = 0;
                } else {
                    // Pack RGB and perform the expensive 16MB lookup only once per color
                    let rgb = &png_palette[i];
                    fast_map[i] = lut[pack_rgb(rgb) as usize];
                }
            } else {
                // Default out-of-range indices to transparency
                fast_map[i] = 0;
            }
        }

        Self { fast_map }
    }

    /// Maps a local PNG palette index to a global palette index.
    ///
    /// Optimized for extreme performance by using a single array access.
    /// This is significantly faster than the previous implementation which
    /// performed bit-shifting and large memory lookups per pixel.
    #[inline(always)]
    pub const fn map(&self, index: u8) -> u8 {
        self.fast_map[index as usize]
    }
}

#[inline(always)]
pub fn read_png(path: impl AsRef<Path>, index_buf: &mut [u8]) -> anyhow::Result<()> {
    read_png_reader(File::open_buffered(path)?, index_buf)
}

pub fn read_png_reader(reader: impl BufRead + Seek, index_buf: &mut [u8]) -> anyhow::Result<()> {
    let png = png::Decoder::new(reader);
    let mut reader = png.read_info()?;
    let png_buf_size = reader
        .output_buffer_size()
        .ok_or_else(|| anyhow!("Cannot read output buffer size"))?;
    reader.next_frame(index_buf)?;

    let info = reader.info();
    let pixel_mapper = PixelMapper::new(info);

    match info.bit_depth {
        BitDepth::One => {
            for i in (0..png_buf_size).rev() {
                let byte = index_buf[i];
                let base = i * 8;
                index_buf[base + 7] = pixel_mapper.map(byte & 1);
                index_buf[base + 6] = pixel_mapper.map((byte >> 1) & 1);
                index_buf[base + 5] = pixel_mapper.map((byte >> 2) & 1);
                index_buf[base + 4] = pixel_mapper.map((byte >> 3) & 1);
                index_buf[base + 3] = pixel_mapper.map((byte >> 4) & 1);
                index_buf[base + 2] = pixel_mapper.map((byte >> 5) & 1);
                index_buf[base + 1] = pixel_mapper.map((byte >> 6) & 1);
                index_buf[base] = pixel_mapper.map((byte >> 7) & 1);
            }
        }
        BitDepth::Two => {
            for i in (0..png_buf_size).rev() {
                let byte = index_buf[i];

                index_buf[i * 4 + 3] = pixel_mapper.map(byte & 0b11);
                index_buf[i * 4 + 2] = pixel_mapper.map((byte >> 2) & 0b11);
                index_buf[i * 4 + 1] = pixel_mapper.map((byte >> 4) & 0b11);
                index_buf[i * 4] = pixel_mapper.map((byte >> 6) & 0b11);
            }
        }
        BitDepth::Four => {
            for i in (0..png_buf_size).rev() {
                let byte = index_buf[i];
                let base = i * 2;
                index_buf[base + 1] = pixel_mapper.map(byte & 0b1111);
                index_buf[base] = pixel_mapper.map((byte >> 4) & 0b1111);
            }
        }
        BitDepth::Eight => {
            for i in (0..index_buf.len()).rev() {
                index_buf[i] = pixel_mapper.map(index_buf[i]);
            }
        }
        BitDepth::Sixteen => {
            unreachable!()
        }
    };

    Ok(())
}

#[inline(always)]
pub fn write_chunk_png(path: impl AsRef<Path>, buf: &[u8]) -> anyhow::Result<()> {
    let mut img_info = Info::with_size(1000, 1000);
    img_info.bit_depth = BitDepth::Eight;
    img_info.color_type = ColorType::Indexed;
    // png palette #0 is transparency
    img_info.trns = Some(Cow::from(&[0_u8]));
    img_info.palette = Some(Cow::Borrowed(PALETTE_DATA_IN_PNG.as_ref()));

    let writer = BufWriter::new(File::create(path)?);
    let encoder = png::Encoder::with_info(writer, img_info)?;
    let mut writer = encoder.write_header()?;
    writer.write_image_data(buf)?;
    Ok(())
}

pub fn write_png(
    path: impl AsRef<Path>,
    dimension: (u32, u32),
    index_data: &[u8],
) -> anyhow::Result<()> {
    let mut new_info = Info::with_size(dimension.0, dimension.1);
    new_info.bit_depth = BitDepth::Eight;
    new_info.color_type = ColorType::Indexed;
    // png palette #0 is transparency
    new_info.trns = Some(Cow::from(&[0_u8]));
    new_info.palette = Some(Cow::Owned(PALETTE_DATA_IN_PNG.to_vec()));

    let writer = File::create_buffered(path)?;
    let encoder = png::Encoder::with_info(writer, new_info)?;
    let mut writer = encoder.write_header()?;
    writer.write_image_data(index_data)?;
    Ok(())
}
