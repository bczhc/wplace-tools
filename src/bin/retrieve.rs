#![feature(file_buffered)]
#![feature(yeet_expr)]
#![feature(try_blocks)]
#![feature(decl_macro)]
#![feature(likely_unlikely)]
#![feature(mpmc_channel)]
#![warn(clippy::all, clippy::nursery)]

use anyhow::anyhow;
use clap::Parser;
use lazy_regex::regex;
use log::{debug, info, warn};
use rayon::prelude::*;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::mpmc::{sync_channel, Receiver, Sender};
use std::thread::{spawn, JoinHandle};
use std::{fs, hint};
use wplace_tools::indexed_png::{read_png_reader, write_png};
use wplace_tools::tar::ChunksTarReader;
use wplace_tools::{apply_chunk, diff, quick_capture, set_up_logger, stylized_progress_bar, validate_chunk_checksum, zstd_decompress, Canvas, ChunkNumber, ChunkProcessError, DiffFilesCollector, DirDiffFilesCollector, ExitOnError, SqfsDiffFilesCollector, CHUNK_DIMENSION, CHUNK_LENGTH};
use yeet_ops::yeet;

#[derive(clap::Parser)]
#[command(author, version)]
/// Chunk image retrieval tool
struct Args {
    /// Chunk(s) to retrieve. Format: x1-y1,x2-y2,x3-y3,... or x1-y1..x2-y2
    #[arg(short, long)]
    chunk: String,

    /// Directory or SquashFS image containing all the .diff files
    ///
    #[arg(short, long, required = true)]
    diff_source: Vec<PathBuf>,

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

    /// Only save the stitched images. This implies `--stitch`.
    #[arg(long)]
    only_stitched: bool,
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = Args::parse();
    let chunks = parse_chunk_string(&args.chunk)?;

    info!("Collecting diff files...");
    let diff_source: &(dyn DiffFilesCollector + Send + Sync + 'static) =
        if args.diff_source.iter().all(|x| x.is_file()) {
            &SqfsDiffFilesCollector::new(&args.diff_source)?
        } else if args.diff_source.iter().all(|x| x.is_dir()) {
            &DirDiffFilesCollector::new(&args.diff_source)?
        } else {
            return Err(anyhow!("SquashFS and Dir diff inputs cannot be mixed."));
        };

    info!("Diff file count: {}", diff_source.name_iter().len());
    let goal_snapshot = args.at.unwrap_or_else(|| diff_source.last());

    if !diff_source.contains(&goal_snapshot) {
        yeet!(anyhow::anyhow!(
            "Cannot find the destination snapshot from diff sources"
        ));
    }

    let apply_list_start = diff_source.first();
    let apply_list = diff_source.range_iter(&apply_list_start, &goal_snapshot);
    let apply_list_len = apply_list.len();

    info!("Indexing initial tarball...");
    let tar = ChunksTarReader::open_with_index(&args.base_snapshot)?;

    info!("Retrieving...");
    let pb = stylized_progress_bar((apply_list_len * chunks.len()) as u64);

    let image_saver = ImageSaver::new();

    // Retrieve chunk from the initial tarball for later processes on it.
    let mut chunks_buf = chunks
        .iter()
        .map(|&n| {
            let a: anyhow::Result<_> = try { (n, retrieve_tar_chunk(&tar, n, true)?) };
            a
        })
        .collect::<Result<Vec<_>, _>>()?;

    // sequentially apply .diff files
    for (idx, name) in apply_list.enumerate() {
        let is_last_snapshot = idx == apply_list_len - 1;
        let stitch_canvas = match args.stitch | args.only_stitched {
            true => Some(Canvas::from_chunk_list(chunks_buf.iter().map(|x| x.0))),
            false => None,
        };

        // parallelize if multiple chunks are requested
        chunks_buf.par_iter_mut().for_each(|(n, chunk_buf)| {
            pb.inc(1);
            let result: anyhow::Result<()> = try {
                let chunk_out = args.out.join(format!("{}-{}", n.0, n.1));

                let mut diff_data = vec![0_u8; CHUNK_LENGTH];
                let mut diff_file = diff::DiffFile::open(diff_source.reader(&name)?)?;
                let chunk_index = diff_file.query_chunk(*n)?;

                let entry = match chunk_index {
                    None => {
                        // chunk had not been created in this snapshot
                        info!("Chunk not present in this snapshot '{}', skipping...", name);
                        return;
                    }
                    Some(e) => e,
                };

                if hint::unlikely(entry.is_changed()) {
                    let portion_reader = diff_file.open_chunk(&entry)?;
                    zstd_decompress(portion_reader, &mut diff_data)?;
                    apply_chunk(chunk_buf, <&[_; _]>::try_from(&diff_data[..]).unwrap());
                    if !args.disable_csum {
                        validate_chunk_checksum(chunk_buf, entry.checksum)?;
                    }
                } else {
                    // just pass
                }

                let img_path = chunk_out.join(format!("{name}.png"));
                if args.all || is_last_snapshot {
                    if !args.only_stitched {
                        image_saver.submit(img_path, CHUNK_DIMENSION, chunk_buf.clone());
                    }
                }
            };
            result
                .map_err(|e| ChunkProcessError {
                    inner: e,
                    chunk_number: *n,
                    diff_file: Some(name.clone()),
                })
                .exit_on_error();
        });
        // save the stitched image
        if let Some(mut c) = stitch_canvas {
            let stitch_out = args.out.join("stitched");

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
    image_saver.finish_and_join();

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

fn retrieve_tar_chunk(
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

type WrappedTask = Box<dyn FnOnce() + Send>;
struct ImageSaver {
    task_tx: Sender<WrappedTask>,
    finish_handles: Vec<JoinHandle<()>>,
}

impl ImageSaver {
    fn new() -> Self {
        let cpu_count = num_cpus::get();
        let (tx, rx) = sync_channel(cpu_count / 2);

        let mut handles = Vec::new();
        for _ in 0..cpu_count {
            let thread_rx: Receiver<WrappedTask> = Receiver::clone(&rx);
            handles.push(spawn(move || {
                for x in thread_rx {
                    x();
                }
            }));
        }
        Self {
            task_tx: tx,
            finish_handles: handles,
        }
    }

    fn submit(&self, path: impl AsRef<Path> + Send + 'static, dimension: (u32, u32), buf: Vec<u8>) {
        let task = move || {
            let path = path.as_ref();
            fs::create_dir_all(path.parent().expect("Can't get parent path"))
                .expect("Can't create folder");
            write_png(&path, dimension, &buf).exit_on_error();
            debug!("Saved: {}", path.display());
        };
        self.task_tx.send(Box::new(task)).unwrap();
    }

    fn finish_and_join(self) {
        drop(self.task_tx);
        for x in self.finish_handles {
            x.join().unwrap();
        }
    }
}
