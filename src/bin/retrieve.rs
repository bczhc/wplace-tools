#![feature(file_buffered)]
#![feature(yeet_expr)]
#![feature(try_blocks)]
#![feature(decl_macro)]
#![warn(clippy::all, clippy::nursery)]

use anyhow::anyhow;
use byteorder::{LittleEndian, WriteBytesExt, LE};
use clap::Parser;
use lazy_regex::regex;
use log::{debug, info, warn};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::{stdin, Cursor, Read, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread::spawn;
use bincode::config::standard;
use rocksdb::{DBCompressionType, Options, WriteBatch, WriteOptions, DB};
use threadpool::ThreadPool;
use wplace_tools::diff2::{DiffDataRange, IndexEntry};
use wplace_tools::indexed_png::{read_png_reader, write_chunk_png, write_png};
use wplace_tools::tar::ChunksTarReader;
use wplace_tools::{
    apply_chunk, diff2, extract_datetime, flate2_decompress, open_file_range, quick_capture,
    set_up_logger, stylized_progress_bar, validate_chunk_checksum, Canvas, ChunkNumber, ChunkProcessError,
    ExitOnError, CHUNK_DIMENSION, CHUNK_LENGTH,
};
use yeet_ops::yeet;
use wplace_tools::diff_index::{collect_diff_files, make_key};

#[derive(clap::Parser)]
#[command(version)]
/// Chunk image retrieval tool
struct Args {
    /// Chunk(s) to retrieve. Format: x1-y1,x2-y2,x3-y3,... or x1-y1..x2-y2
    #[arg(short, long)]
    chunk: String,

    /// Directory containing all the consecutive .diff files
    #[arg(short, long)]
    diff_dir: PathBuf,

    /// RocksDB folder for diff index
    #[arg(short = 'i', long)]
    index_db: PathBuf,

    /// Path to the initial snapshot (tarball format)
    #[arg(short, long)]
    base_snapshot: PathBuf,

    /// Output path
    #[arg(short, long)]
    out: PathBuf,

    /// Snapshot name of the restoration point. If not present, use the newest one in `diff_dir`.
    #[arg(short = 't', long)]
    at: Option<String>,

    /// If enabled, instead of retrieving only the target one, also retrieve all chunks prior to it.
    ///
    /// By this, timelapse videos can be easily created.
    #[arg(short, long)]
    all: bool,

    /// Disable checksum validation. Only for debugging purposes.
    #[arg(long, default_value = "false")]
    disable_csum: bool,

    /// Stitch chunks together to a big image.
    #[arg(short, long)]
    stitch: bool,
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = Args::parse();
    let chunks = parse_chunk_string(&args.chunk)?;

    let diff_path = Path::new(&args.diff_dir);

    info!("Collecting diff files...");
    let diff_list = collect_diff_files(&args.diff_dir)?;
    let last_diff_list = diff_list
        .last()
        .ok_or_else(|| anyhow::anyhow!("Empty diff list!"))?;
    let goal_snapshot = args.at.as_ref().unwrap_or(last_diff_list);

    let Some(dest_snap_pos) = diff_list.iter().position(|x| x == goal_snapshot) else {
        yeet!(anyhow::anyhow!(
            "Cannot find the destination snapshot in the diff list"
        ));
    };
    let base_snapshot_name = extract_datetime(
        format!(
            "{}",
            args.base_snapshot
                .file_name()
                .expect("No filename")
                .display()
        )
        .as_str(),
    )
    .ok_or_else(|| anyhow!("Malformed base snapshot name"))?;
    let base_start = diff_list
        .iter()
        .position(|x| x == &base_snapshot_name)
        .map(|x| x + 1)
        .unwrap_or(0);
    let apply_list = &diff_list[base_start..=dest_snap_pos];

    let diff_list_not_processed=collect_diff_list_not_processed(diff_list.clone(), &args.index_db)?;
    if !diff_list_not_processed.is_empty() {
        warn!("Diff index database missing or incomplete; creating it...");
        create_index_db(diff_path.into(), diff_list.clone(), diff_list_not_processed, (&args.index_db).into())?;
    }

    info!("Indexing tarball...");
    let tar = ChunksTarReader::open_with_index(&args.base_snapshot)?;

    info!("Opening index db...");
    let options = Options::default();
    let index_db = DB::open(&options, args.index_db)?;

    info!("Retrieving...");
    let pb = stylized_progress_bar((apply_list.len() * chunks.len()) as u64);

    let image_saver = ImageSaverPool::new()?;

    let mut chunks_buf = chunks
        .iter()
        .map(|&n| {
            let a: anyhow::Result<_> = try { (n, retrieve_chunk(&tar, n, true)?) };
            a
        })
        .collect::<Result<Vec<_>, _>>()?;

    for (idx, name) in apply_list.iter().enumerate() {
        let is_last_snapshot = idx == apply_list.len() - 1;
        let stitch_canvas = match args.stitch {
            true => Some(Canvas::from_chunk_list(chunks_buf.iter().map(|x| x.0))),
            false => None,
        };

        chunks_buf.par_iter_mut().for_each(|(n, chunk_buf)| {
            pb.inc(1);
            let result: anyhow::Result<()> = try {
                let chunk_out = args.out.join(format!("{}-{}", n.0, n.1));
                fs::create_dir_all(&chunk_out)?;

                let mut diff_data = vec![0_u8; CHUNK_LENGTH];

                let mut key_buf = [0_u8; 100];
                let entry_blob = index_db.get(make_key(name, *n, &mut key_buf))?;
                let entry = match entry_blob {
                    None => {
                        // chunk had not been created in this snapshot
                        info!("Chunk not present in this snapshot '{}', skipping...", name);
                        return;
                    }
                    Some(e) => {
                        let e: (IndexEntry, usize) = bincode::decode_from_slice(&e, standard())?;
                        e.0
                    }
                };

                match entry.diff_data_range {
                    DiffDataRange::Unchanged => {
                        // just pass
                    }
                    DiffDataRange::Changed { pos, len } => {
                        let reader =
                            open_file_range(diff_path.join(format!("{name}.diff")), pos, len)?;
                        flate2_decompress(reader, &mut diff_data)?;
                        apply_chunk(chunk_buf, <&[_; _]>::try_from(&diff_data[..]).unwrap());
                        if !args.disable_csum {
                            validate_chunk_checksum(chunk_buf, entry.checksum)?;
                        }
                    }
                }

                let img_path = chunk_out.join(format!("{name}.png"));
                if args.all || is_last_snapshot {
                    write_chunk_png(&img_path, chunk_buf)?;
                    image_saver.submit(img_path, CHUNK_DIMENSION, chunk_buf.clone());
                }
            };
            result
                .map_err(|e| ChunkProcessError {
                    inner: e,
                    chunk_number: *n,
                    diff_file: Some(name.into()),
                })
                .exit_on_error();
        });
        // save the stitched image
        if let Some(mut c) = stitch_canvas {
            let stitch_out = args.out.join("stitched");
            fs::create_dir_all(&stitch_out)?;

            if args.all || is_last_snapshot {
                for x in &chunks_buf {
                    c.copy(x.0, <&[_; _]>::try_from(&x.1[..]).unwrap());
                }
                let out_file = stitch_out.join(format!("{name}.png"));
                image_saver.submit(
                    out_file,
                    (c.dimension.0 as u32, c.dimension.1 as u32),
                    c.buf,
                );
            }
        }
    }
    pb.finish();
    info!("Waiting for image saver...");
    image_saver.join();

    Ok(())
}

fn parse_chunk_string(s: &str) -> anyhow::Result<Vec<ChunkNumber>> {
    let mut chunks: Vec<ChunkNumber> = Vec::new();
    let s = s.chars().filter(|x| !x.is_whitespace()).collect::<String>();
    let split = s.split(',');
    for x in split {
        let p1 = regex!(r"^(\d+)\-(\d+)\.\.(\d+)\-(\d+)$");
        let p2 = regex!(r"^(\d+)\-(\d+)$");
        if p1.is_match(x) {
            let group = quick_capture(x, p1).unwrap();
            let start: ChunkNumber = (group[0].parse()?, group[1].parse()?);
            let end: ChunkNumber = (group[2].parse()?, group[3].parse()?);
            expand_chunks_range(start, end)
                .iter()
                .for_each(|&x| chunks.push(x));
        } else if p2.is_match(x) {
            let group = quick_capture(x, p2).unwrap();
            chunks.push((group[0].parse()?, group[1].parse()?));
        } else {
            yeet!(anyhow::anyhow!("Malformed chunk string: {}", s))
        }
    }
    Ok(chunks)
}

/// `start` and `end` represent the two diagonal points.
fn expand_chunks_range(start: ChunkNumber, end: ChunkNumber) -> Vec<(u16, u16)> {
    const fn range(n1: u16, n2: u16) -> RangeInclusive<u16> {
        if n1 < n2 { n1..=n2 } else { n2..=n1 }
    }

    let x_range = range(start.0, end.0);
    let y_range = range(start.1, end.1);
    let mut collected = x_range
        .flat_map(|x| y_range.clone().map(move |y| (x, y)))
        .collect::<Vec<_>>();
    collected.sort();
    collected
}

fn retrieve_chunk(
    snapshot: &ChunksTarReader,
    n: ChunkNumber,
    allow_non_exist: bool,
) -> anyhow::Result<Vec<u8>> {
    let mut buf = vec![0_u8; CHUNK_LENGTH];
    if allow_non_exist && !snapshot.map.contains_key(&n) {
        return Ok(buf);
    }
    let chunk_reader = snapshot.open_chunk(n).unwrap()?;

    read_png_reader(chunk_reader, &mut buf)?;
    Ok(buf)
}

struct ImageSaverPool {
    pool: ThreadPool,
}

impl ImageSaverPool {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            pool: ThreadPool::new(num_cpus::get()),
        })
    }

    fn submit(&self, path: impl AsRef<Path> + Send + 'static, dimension: (u32, u32), buf: Vec<u8>) {
        self.pool.execute(move || {
            let path = path;
            write_png(&path, dimension, &buf).exit_on_error();
            debug!("Saved: {}", path.as_ref().display());
        });
    }

    fn join(self) {
        self.pool.join();
    }
}

fn collect_diff_list_not_processed(diff_list: Vec<String>, db_path: impl AsRef<Path>) -> anyhow::Result<Vec<String>> {
    let track_file_path = db_path.as_ref().join("processed.txt");
    let mut processed = std::collections::HashSet::new();
    if track_file_path.exists() {
        processed = std::fs::read_to_string(&track_file_path)?
            .lines()
            .map(|s| s.to_string())
            .collect();
    }

    // skip diff files already processed
    let diff_list = diff_list
        .into_iter()
        .filter(|x| !processed.contains(x))
        .collect::<Vec<_>>();
    Ok(diff_list)
}

fn create_index_db(
    diff_path: PathBuf,
    diff_list: Vec<String>,
    diff_list_not_processed: Vec<String>,
    db_path: PathBuf,
) -> anyhow::Result<()> {
    let track_file_path = db_path.join("processed.txt");
    let remaining_diff_list_len = diff_list_not_processed.len();
    info!("Number of all diff files: {}", diff_list.len());
    info!("Skipped {} diff files", diff_list.len() - remaining_diff_list_len);

    let (tx, rx) = sync_channel(16);

    spawn(move || {
        diff_list_not_processed.par_iter().for_each(|x| {
            let mut reader = diff2::DiffFile::open(
                File::open_buffered(diff_path.join(format!("{x}.diff"))).unwrap(),
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

    let pb = stylized_progress_bar(remaining_diff_list_len as u64);
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
