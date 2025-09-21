mod diff_size_of_chunks_changed;

use std::fs::File;
use std::io::BufReader;
/// Find chunks that are fully painted (no transparency at all).
///
/// This only reads `trns` from the PNG info - a high efficient way.
/// If a chunk has no transparency color, the PNG encoder from
/// Wplace is expected not to put a zero in the `trns` field.
use rayon::prelude::*;
use std::path::Path;
use wplace_tools::{CHUNK_LENGTH, Progress, collect_chunks, read_png};

fn main() -> anyhow::Result<()> {
    let path = Path::new("/mnt/nvme/wplace-archives/1/2025-09-21T09-35-13.789Z+2h49m");
    let chunks = collect_chunks(path, None)?;
    let progress = Progress::new(chunks.len() as u64)?;
    chunks.into_par_iter().for_each(|(x, y)| {
        let png_file = path.join(format!("{x}/{y}.png"));

        let png = png::Decoder::new(BufReader::new(File::open(&png_file).unwrap()));
        let reader = png.read_info().unwrap();
        let info = reader.info();
        // I may expect if a chunk were painted fully (that's, no transparency pixels at all),
        // the PNG encoder from Wplace may not put a `0` in the `trns` array.
        let no_alpha = info
            .trns
            .as_ref()
            .is_none_or(|x| !x.contains(&0));
        if no_alpha {
            println!("{:?}", (x, y));
        }

        progress.inc(1);
    });
    progress.finish();
    Ok(())
}
