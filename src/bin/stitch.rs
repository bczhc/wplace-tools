use clap::Parser;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use lazy_regex::regex;
use wplace_tools::{stylized_progress_bar, Canvas, ChunkNumber, CHUNK_LENGTH};
use wplace_tools::indexed_png::read_png;

#[derive(clap::Parser)]
struct Args {
    dir: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let out_dir = args.dir.join("stitched");
    fs::create_dir_all(&out_dir)?;

    // 存放所有文件名的 HashSet（自动去重）
    let mut filenames = HashSet::new();

    // 遍历 dir 下的所有条目
    for entry in fs::read_dir(&args.dir)? {
        let entry = entry?;
        let path = entry.path();

        // 只处理子目录
        if path.is_dir() {
            // 遍历子目录内文件
            for file_entry in fs::read_dir(path)? {
                let file_entry = file_entry?;
                let file_path = file_entry.path();

                if file_path.is_file() {
                    if let Some(name) = file_path.file_name().and_then(|n| n.to_str()) {
                        filenames.insert(name.to_string());
                    }
                }
            }
        }
    }

    let mut chunk_list: Vec<ChunkNumber> = Vec::new();
    for x in fs::read_dir(&args.dir)? {
        let x = x?;
        let dir_name = x.file_name();
        let dir_name = dir_name.to_str().expect("Invalid UTF-8");
        let format = regex!(r"^\d+\-\d+$");
        if !format.is_match(dir_name) {
            continue;
        }
        let mut split = dir_name.split('-');
        let chunk_x: u16 = split.next().unwrap().parse()?;
        let chunk_y: u16 = split.next().unwrap().parse()?;
        chunk_list.push((chunk_x, chunk_y));
    }

    let pb = stylized_progress_bar(filenames.len() as u64);
    filenames.into_par_iter().for_each(|x| {
        let mut canvas = Canvas::from_chunk_list(chunk_list.iter().copied());
        for n in &chunk_list {
            let img_path = args.dir.join(format!("{}-{}", n.0,n.1)).join(&x);
            let mut img_buf = vec![0_u8; CHUNK_LENGTH];
            if img_path.exists() {
                read_png(&img_path, &mut img_buf).unwrap();
            }
            canvas.copy(*n, <_>::try_from(&img_buf[..]).unwrap());
        }
        let out_file = out_dir.join(x);
        canvas.save(out_file).unwrap();
        pb.inc(1);
    });

    Ok(())
}
