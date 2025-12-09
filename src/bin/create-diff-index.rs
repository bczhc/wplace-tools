#![feature(file_buffered)]
#![feature(yeet_expr)]
#![feature(try_blocks)]
#![feature(decl_macro)]
#![warn(clippy::all, clippy::nursery)]

use anyhow::anyhow;
use byteorder::{LittleEndian, WriteBytesExt, LE};
use clap::Parser;
use lazy_regex::regex;
use log::{debug, info};
use rayon::prelude::*;
use rocksdb::{DBCompressionType, WriteBatch, WriteOptions};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::{stdin, Cursor, Read, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread::spawn;
use threadpool::ThreadPool;
use wplace_tools::diff2::{DiffDataRange, IndexEntry};
use wplace_tools::diff_index::{collect_diff_files, make_key};
use wplace_tools::indexed_png::{read_png_reader, write_chunk_png, write_png};
use wplace_tools::tar::ChunksTarReader;
use wplace_tools::{
    apply_chunk, diff2, extract_datetime, flate2_decompress, open_file_range, quick_capture,
    set_up_logger, stylized_progress_bar, validate_chunk_checksum, Canvas, ChunkNumber, ChunkProcessError,
    ExitOnError, CHUNK_DIMENSION, CHUNK_LENGTH,
};
use yeet_ops::yeet;

#[derive(clap::Parser)]
#[command(version)]
struct Args {
    /// Directory containing all the consecutive .diff files
    diff_dir: PathBuf,

    /// RocksDB folder
    db_path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = Args::parse();
    let diff_path = Path::new(&args.diff_dir);

    info!("Collecting diff files...");
    let diff_list = collect_diff_files(&args.diff_dir)?;

    create_index_db(diff_path.into(), diff_list, args.db_path)?;
    Ok(())
}

fn create_index_db(
    diff_path: PathBuf,
    diff_list: Vec<String>,
    db_path: PathBuf,
) -> anyhow::Result<()> {
    let diff_list_len = diff_list.len();

    let track_file_path = db_path.join("processed.txt");
    let mut processed = std::collections::HashSet::new();
    if track_file_path.exists() {
        processed = std::fs::read_to_string(&track_file_path)?
            .lines()
            .map(|s| s.to_string())
            .collect();
    }
    info!("Number of all diff files: {}", diff_list_len);
    info!("Skipped {} diff files", processed.len());

    let (tx, rx) = sync_channel(16);
    let diff_path_clone = diff_path.clone();
    let diff_list_clone = diff_list.clone();

    spawn(move || {
        diff_list_clone
            .par_iter()
            // skip diff files already processed
            .filter(|x| !processed.contains(*x))
            .for_each(|x| {
                let mut reader = diff2::DiffFile::open(
                    File::open_buffered(diff_path_clone.join(format!("{x}.diff"))).unwrap(),
                )
                .unwrap();
                let index = reader.read_index().unwrap();
                tx.send((x.clone(), index)).unwrap();
            });
    });

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_max_background_jobs(16);
    opts.set_write_buffer_size(256 * 1048576);
    opts.set_row_cache(&rocksdb::Cache::new_lru_cache(1 << 30));
    opts.set_blob_cache(&rocksdb::Cache::new_lru_cache(1 << 30));
    opts.set_compression_type(DBCompressionType::Zlib);
    opts.increase_parallelism(16);
    let db = rocksdb::DB::open(&opts, db_path.clone())?;

    let pb = stylized_progress_bar(diff_list_len as u64);
    let config = bincode::config::standard();

    let mut track_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&track_file_path)?;

    rx.into_iter().for_each(|(diff_name, index)| {
        let mut key_buf = [0_u8; 100];
        let mut value_buf = [0_u8; 100];

        let mut write_batch = WriteBatch::new();
        for x in index {
            let key = make_key(&diff_name, x.0, &mut key_buf);
            let value_len = bincode::encode_into_slice(x.1, &mut value_buf, config).unwrap();
            let value = &value_buf[..value_len];
            write_batch.put(&key, value);

        }
        db.write(write_batch).unwrap();
        db.flush().unwrap();

        writeln!(track_file, "{}", diff_name).unwrap();
        track_file.flush().unwrap();
        pb.inc(1);
    });
    pb.finish();
    Ok(())
}
