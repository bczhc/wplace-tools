use crate::GLOBAL_PALETTE;
use anyhow::anyhow;
use once_cell::sync::Lazy;
use png::{BitDepth, ColorType, Info};
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Write};
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

/// Map indices in an indexed PNG -> indices in the global [GLOBAL_PALETTE]
pub struct PixelMapper<
    // Use fixed array here to eliminate heap allocations
    const MAX_COLORS: usize = 64,
> {
    /// Raw palette format: \[r0, g0, b0, r1, g1, b1, ...\]<br/>
    /// Grouped png_palette format: \[\[r0, g0, b0\], \[r1, g1, b1\], ...\]
    png_palette: [[u8; 3]; MAX_COLORS],
    /// See: https://www.w3.org/TR/PNG-DataRep.html#DR.Alpha-channel
    trns: [u8; MAX_COLORS],
    palette_lookup_table: &'static [u8],
}

impl PixelMapper {
    fn new(png_info: &Info) -> PixelMapper {
        let raw_palette = png_info.palette.as_ref().expect("No palette");
        assert_eq!(raw_palette.len() % 3, 0);
        let color_count = raw_palette.len() / 3;
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
        assert_eq!(png_palette.len(), color_count);

        let mut png_palette = [Default::default(); 64];
        let mut groups = raw_palette.chunks(3);
        for x in png_palette.iter_mut().take(color_count) {
            *x = groups.next().unwrap().try_into().unwrap();
        }

        let mut expanded_trns = [0_u8; 64];
        match png_info.trns.as_ref() {
            None => {
                // all colors in the raw palette are fully opaque
                expanded_trns.fill(255);
            }
            Some(trns) => {
                // If trns are not as long as raw_palette, expand it. The missing trns positions
                // should all indicate a full opaque.
                (&mut expanded_trns[..]).write_all(trns).unwrap();
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

pub fn read_png(path: impl AsRef<Path>, index_buf: &mut [u8]) -> anyhow::Result<()> {
    let png = png::Decoder::new(BufReader::new(File::open(&path)?));
    let mut reader = png.read_info()?;
    let png_buf_size = reader
        .output_buffer_size()
        .ok_or(anyhow!("Cannot read output buffer size"))?;
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
