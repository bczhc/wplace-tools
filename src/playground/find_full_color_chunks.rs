/// Find chunks that are fully painted (no transparency at all).
///
/// This only reads `trns` from the PNG info - a high efficient way.
/// If a chunk has no transparency color, the PNG encoder from
/// Wplace is expected not to put a zero in the `trns` field.
use rayon::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use wplace_tools::{collect_chunks, Progress};


fn main() -> anyhow::Result<()> {
    let path = Path::new("/mnt/nvme/wplace-archive/2025-09-04T13-20-46.618Z+3h0m");
    let chunks = collect_chunks(path, None)?;
    let progress = Progress::new(chunks.len() as u64)?;
    chunks.into_par_iter().for_each(|(x, y)| {
        let png_file = path.join(format!("{x}/{y}.png"));

        let png = png::Decoder::new(BufReader::new(File::open(&png_file).unwrap()));
        let reader = png.read_info().unwrap();
        let info = reader.info();
        let no_alpha = info
            .trns
            .as_ref()
            .is_none_or(|x| x.iter().rposition(|&x| x == 0).is_none());
        if no_alpha {
            println!("{:?}", (x, y));
        }

        progress.inc(1);
    });
    progress.finish();
    Ok(())
}
