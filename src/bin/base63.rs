#![feature(file_buffered)]

use clap::Parser;
use num_bigint::BigUint;
use num_integer::Roots;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::iter;
use std::path::PathBuf;
use wplace_tools::indexed_png::{read_png, write_png};

/// Base63 with Wplace palette
///
/// This tool represents binary data to a PNG image (or vise versa) in Wplace colors.
/// Thus, you can record any binary data on the Wplace canvas.
#[derive(Parser, Debug)]
#[command(version = "0.1.0")]
struct Args {
    input: PathBuf,
    output: PathBuf,
    #[arg(default_value = "false", short, long)]
    decode: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.decode {
        false => {
            // encode
            let mut input_data = Vec::new();
            File::open_buffered(&args.input)?.read_to_end(&mut input_data)?;
            let mut image_data = encode(&input_data);
            let dimension = image_dimension(image_data.len() as u32);
            // padding zeros (transparency)
            image_data.extend(iter::repeat_n(
                0,
                (dimension.0 * dimension.1) as usize - image_data.len(),
            ));
            write_png(args.output, dimension, &image_data)?;
        }
        true => {
            // decode
            let png = png::Decoder::new(BufReader::new(File::open(&args.input)?));
            let reader = png.read_info()?;
            let info = reader.info();
            let pixel_count = info.width * info.height;
            let mut buf = vec![0_u8; pixel_count as usize];
            read_png(args.input, &mut buf)?;
            let decoded = decode(&buf);
            File::create_buffered(args.output)?.write_all(&decoded)?;
        }
    }

    Ok(())
}

fn encode(data: &[u8]) -> Vec<u8> {
    let mut indices = BigUint::from_bytes_be(data).to_radix_be(63);
    for x in indices.iter_mut() {
        // shift by one because PALETTE[0] is defined as transparency
        *x += 1;
        assert!(*x >= 1 && *x <= 63)
    }
    indices
}

fn decode(image_index_data: &[u8]) -> Vec<u8> {
    let mut data = image_index_data.to_vec();
    // transparency pixels are not expected (except the trailing padding)
    // so truncate the input data to the transparency (if present)
    if let Some(i) = data.iter().position(|x| *x == 0) {
        data.truncate(i);
    }

    for x in data.iter_mut() {
        // shift by one because PALETTE[0] is defined as transparency
        *x -= 1;
        assert!(*x <= 62);
    }
    BigUint::from_radix_be(&data, 63).unwrap().to_bytes_be()
}

fn image_dimension(length: u32) -> (u32, u32) {
    fn is_square(n: u32) -> bool {
        let a = u32::sqrt(&n);
        a * a == n
    }

    if is_square(length) {
        (length.sqrt(), length.sqrt())
    } else {
        let h = length.sqrt();
        let w = f64::ceil(length as f64 / h as f64) as u32;
        assert!(w * h >= length);
        (w, h)
    }
}
