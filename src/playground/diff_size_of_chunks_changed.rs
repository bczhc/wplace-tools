use bytesize::ByteSize;
use rayon::prelude::*;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use wplace_tools::{CHUNK_LENGTH, collect_chunks, read_png, stylized_progress_bar};

fn main() -> anyhow::Result<()> {
    // let range = TilesRange::parse_str("501,619,1019,1034").unwrap();

    let base = Path::new("/mnt/nvme/wplace-archives/1/2025-09-21T06-32-28.284Z+3h2m");
    let new = Path::new("/mnt/nvme/wplace-archives/1/2025-09-21T09-35-13.789Z+2h49m");
    let diff = Path::new("/mnt/nvme/wplace-archives/1/diff-by-cmp");

    println!("Collecting files...");
    let collected = collect_chunks(new, None)?;

    println!("Processing {} files...", collected.len());
    let progress = stylized_progress_bar(collected.len() as u64);

    let total_size = AtomicU64::new(0);

    collected.into_par_iter().for_each(|(c1, c2)| {
        let filename = format!("{c1}/{c2}.png");
        let base_file = base.join(&filename);
        let new_file = new.join(&filename);
        let diff_file = diff.join(&filename);
        fs::create_dir_all(diff_file.parent().unwrap()).unwrap();

        let mut buf1 = vec![0_u8; CHUNK_LENGTH];
        let mut buf2 = vec![0_u8; CHUNK_LENGTH];

        let mut changed = false;
        if !base_file.exists() {
            // if the parent file doesn't exist, add the new one fully
            changed = true;
        } else {
            read_png(&base_file, &mut buf1).unwrap();
            read_png(&new_file, &mut buf2).unwrap();
            if buf1 != buf2 {
                changed = true;
            }
        }

        if changed {
            total_size.fetch_add(new_file.metadata().unwrap().len(), Ordering::SeqCst);
            fs::copy(&new_file, &diff_file).unwrap();
        }

        progress.inc(1);
    });

    progress.finish();

    println!(
        "{}",
        ByteSize(total_size.load(Ordering::SeqCst)).display().iec()
    );

    Ok(())
}
