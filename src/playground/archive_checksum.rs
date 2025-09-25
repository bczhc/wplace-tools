use std::cmp::Ordering;
use std::path::Path;
use std::sync::{Arc, Mutex};
use rayon::prelude::*;
use wplace_tools::{collect_chunks, stylized_progress_bar, CHUNK_LENGTH};
use wplace_tools::indexed_png::read_png;

fn main() -> anyhow::Result<()> {
    // let path = Path::new("/mnt/nvme/wplace-archives/1/2025-09-21T09-35-13.789Z+2h49m");
    let path = Path::new("/mnt/nvme/wplace-archives/1/cn/snap1");
    let collected = collect_chunks(path, None)?;
    let hash_list = Arc::new(Mutex::new(Vec::new()));

    let progress = stylized_progress_bar(collected.len() as u64);

    collected.into_par_iter().for_each_with(Arc::clone(&hash_list),|hash_list, (x,y)| {
        let chunk_file = path.join(format!("{x}/{y}.png"));
        let mut buf = vec![0_u8; CHUNK_LENGTH];
        read_png(&chunk_file, &mut buf).unwrap();
        let hash = blake3::hash(&buf);
        hash_list.lock().unwrap().push((x,y, hash));
        progress.inc(1);
    });

    progress.finish();

    let mut hash_list =Arc::try_unwrap(hash_list).unwrap().into_inner().unwrap();
    hash_list.sort_by(|a,b| {
        match a.0.cmp(&b.0) {
            Ordering::Equal => {
                a.1.cmp(&b.1)
            }
            o => o
        }
    });

    let mut hasher = blake3::Hasher::new();
    for (_x, _y, sub_hash) in hash_list {
        hasher.update(sub_hash.as_bytes());
    }
    let hash = hasher.finalize();

    println!("{}", hash);
    Ok(())
}
