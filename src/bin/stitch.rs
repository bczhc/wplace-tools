#![feature(decl_macro)]
#![feature(try_blocks)]

use clap::Parser;
use lazy_regex::regex;
use log::{error, info};
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::{fs, io};
use wplace_tools::indexed_png::{read_png, write_png};
use wplace_tools::{
    extract_datetime, quick_capture, set_up_logger, ChunkNumber, CHUNK_LENGTH, CHUNK_WIDTH,
};

#[derive(clap::Parser)]
struct Args {
    /// Directory to the output of `retrieve`.
    dir: PathBuf,

    /// Output directory.
    out_dir: PathBuf,
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = Args::parse();
    let dir = &args.dir;
    let out_dir = &args.out_dir;
    fs::create_dir_all(out_dir)?;
    info!("Indexing...");
    let mut index: HashMap<ChunkNumber, Vec<String>> = Default::default();
    for x in fs::read_dir(dir)? {
        let x = x?;
        let n = parse_chunk_str(x.file_name().to_str().unwrap()).unwrap();
        let mut snap_names = Vec::new();
        for x in fs::read_dir(x.path())? {
            let x = x?;
            let filename = x.file_name().to_str().unwrap().to_string();
            snap_names.push(extract_datetime(&filename).unwrap());
        }
        index.insert(n, snap_names);
    }

    let mut all_snap_names = index.values().flatten().collect::<Vec<_>>();
    all_snap_names.sort();
    all_snap_names.dedup();
    let full_snap_names = all_snap_names;
    let latest_snap = *full_snap_names.last().unwrap();

    let chunks = index.keys().collect::<Vec<_>>();
    let min_x = chunks.iter().map(|x| x.0).min().unwrap();
    let max_x = chunks.iter().map(|x| x.0).max().unwrap();
    let min_y = chunks.iter().map(|x| x.1).min().unwrap();
    let max_y = chunks.iter().map(|x| x.1).max().unwrap();

    full_snap_names
        .iter()
        .enumerate()
        .par_bridge()
        .for_each(|(i, &name)| {
            let r: anyhow::Result<()> = try {
                let mut canvas = Canvas::new(max_x - min_x + 1, max_y - min_y + 1, (min_x, min_y));

                info!("Frame {}; Processing {name}", i + 1);
                for &n in &chunks {
                    let png_file = dir.join(format!("{}-{}/{}.png", n.0, n.1, name));
                    let mut chunk_buf = vec![0_u8; CHUNK_LENGTH];
                    if png_file.exists() {
                        read_png(png_file, &mut chunk_buf)?;
                    }
                    canvas.copy(*n, <&[_; _]>::try_from(&chunk_buf[..]).unwrap());
                }

                canvas.save(out_dir.join(format!("{name}.png")))?;
            };
            if r.is_err() {
                error!("Error: {:?}", r);
                exit(1);
            }
        });
    Ok(())
}

fn parse_chunk_str(s: &str) -> Option<ChunkNumber> {
    let r = regex!(r"^(\d+)\-(\d+)$");
    quick_capture(s, r).map(|x| (x[0].parse().unwrap(), x[1].parse().unwrap()))
}

struct Canvas {
    buf: Vec<u8>,
    min_chunk: ChunkNumber,
    pub dimension: (usize, usize),
}

impl Canvas {
    fn new(chunk_num_x: u16, chunk_num_y: u16, min_chunk: ChunkNumber) -> Self {
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

    fn copy(&mut self, n: ChunkNumber, buf: &[u8; CHUNK_LENGTH]) {
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

    fn save(&self, out: impl AsRef<Path>) -> anyhow::Result<()> {
        write_png(
            out,
            (self.dimension.0 as u32, self.dimension.1 as u32),
            &self.buf,
        )?;
        Ok(())
    }
}
