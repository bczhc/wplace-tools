#![feature(file_buffered)]

/// Find chunks that are fully painted (no transparency at all).
///
/// This only reads `trns` from the PNG info - a high efficient way.
/// If a chunk has no transparency color, the PNG encoder from
/// Wplace is expected not to put a zero in the `trns` field.
use rayon::prelude::*;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::Path;
use std::sync::mpsc::channel;
use wplace_tools::{collect_chunks, stylized_progress_bar};

fn main() -> anyhow::Result<()> {
    let mut output = File::create_buffered("output/full-color-chunks.txt")?;
    let (tx, rx) = channel();

    let path = Path::new("/mnt/nvme/wplace-archives/1/2025-09-21T09-35-13.789Z+2h49m");
    let chunks = collect_chunks(path, None)?;
    let progress = stylized_progress_bar(chunks.len() as u64);
    chunks.into_par_iter().for_each_with(tx, |sender, (x, y)| {
        let png_file = path.join(format!("{x}/{y}.png"));

        let png = png::Decoder::new(BufReader::new(File::open(&png_file).unwrap()));
        let reader = png.read_info().unwrap();
        let info = reader.info();
        // According to the spec, `trns` not having any zero means a full opaque PNG.
        let no_alpha = info.trns.as_ref().is_none_or(|x| !x.contains(&0));
        if no_alpha {
            println!("{:?}", (x, y));
            sender.send(format!("{:?}", (x, y))).unwrap();
        }

        progress.inc(1);
    });
    progress.finish();

    for x in rx {
        use io::Write;
        writeln!(&mut output, "{x}")?;
    }
    Ok(())
}
