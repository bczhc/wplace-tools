#![feature(file_buffered)]

use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::abort;
use std::time::Instant;
use wplace_tools::checksum::CHUNK_CRC32;
use wplace_tools::diff2::DiffDataRange;
use wplace_tools::indexed_png::{read_png, write_chunk_png};
use wplace_tools::{apply_chunk, diff2, extract_datetime, ChunkNumber, CHUNK_LENGTH};

fn main() -> anyhow::Result<()> {
    let new_diff_path = Path::new("/mnt/nvme/wplace-archives/mine/new-diff");

    let mut filenames = Vec::new();
    for x in walkdir::WalkDir::new(new_diff_path) {
        let x = x?;
        if x.path().is_file() {
            filenames.push(x.file_name().to_os_string());
        }
    }
    filenames.sort();

    println!("Collecting index...");
    let collected = filenames
        .iter()
        .par_bridge()
        .map(|x| {
            let mut reader =
                diff2::DiffFile::open(File::open_buffered(new_diff_path.join(x)).unwrap()).unwrap();
            let index = reader.read_index().unwrap();
            (extract_datetime(x.to_str().unwrap()), index)
        })
        .collect::<HashMap<_, _>>();

    let chunk_number: ChunkNumber = (1717, 837);
    let initial = "2025-08-09T20-01-14.231Z";
    let names = filenames
        .iter()
        .map(|x| extract_datetime(x.to_str().unwrap()))
        .collect::<Vec<_>>();
    let apply_list = &names[0..];

    let mut chunk = [0_u8; CHUNK_LENGTH];
    let mut diff_data = [0_u8; CHUNK_LENGTH];
    read_png(
        format!(
            "/mnt/nvme/wplace-archives/mine/full/{initial}/{}/{}.png",
            chunk_number.0, chunk_number.1
        ),
        &mut chunk,
    )?;

    let start = Instant::now();
    for x in apply_list {
        let entry = &collected[x][&chunk_number];
        println!(
            "Applying {x} changed: {}...",
            matches!(entry.diff_data_range,DiffDataRange::Changed {..})
        );
        match &entry.diff_data_range {
            DiffDataRange::Unchanged => {
                // just pass
            }
            DiffDataRange::Changed { pos, len } => {
                let mut file = File::open_buffered(new_diff_path.join(format!("{x}.diff")))?;
                file.seek(SeekFrom::Start(*pos))?;
                let take = file.take(*len);
                let mut decoder = flate2::read::DeflateDecoder::new(take);
                decoder.read_exact(&mut diff_data)?;
                apply_chunk(&mut chunk, &diff_data);
                let string = format!("/home/bczhc/{}-{}", chunk_number.0, chunk_number.1);
                let dir = Path::new(string.as_str());
                fs::create_dir_all(dir)?;

                // checksum validation
                if CHUNK_CRC32.checksum(&chunk) != entry.checksum {
                    eprintln!("Checksum not matched!");
                    abort();
                }

                write_chunk_png(dir.join(format!("{x}.png")), &chunk)?;
            }
        }
    }

    println!("{:?}", start.elapsed());

    Ok(())
}
