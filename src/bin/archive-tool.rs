#![feature(decl_macro)]
#![feature(file_buffered)]
#![feature(likely_unlikely)]
#![feature(yeet_expr)]
#![feature(try_blocks)]
#![warn(clippy::all, clippy::nursery)]

use crate::cli::Commands;
use clap::Parser;
use log::{debug, info, warn};
use rayon::prelude::*;
use std::cell::RefCell;
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::thread::spawn;
use std::{fs, io};
use tempfile::NamedTempFile;
use wplace_tools::checksum::chunk_checksum;
use wplace_tools::indexed_png::read_png;
use wplace_tools::{
    CHUNK_LENGTH, ChunkFetcher, DIFF_DATA_ZSTD_COMPRESSION_LEVEL, DirChunkFetcher, ExitOnError,
    MUTATION_MASK, PALETTE_INDEX_MASK, TarChunkFetcher, chunk_buf, collect_chunks, diff,
    new_chunk_file, open_file_range, set_up_logger, stylized_progress_bar,
};

mod cli {
    use clap::{Args, Parser, Subcommand, ValueHint};
    use std::path::PathBuf;
    use wplace_tools::TilesRange;

    #[derive(Debug, Parser)]
    #[command(author, version)]
    /// Tools for Wplace snapshots (mainly diffing/restoring)
    pub struct Cli {
        #[command(subcommand)]
        pub command: Commands,
    }

    #[derive(Debug, Subcommand)]
    pub enum Commands {
        /// Create diff from `base` to `new`
        Diff {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "NEW", value_hint = ValueHint::FilePath)]
            new: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,
        },

        /// Apply diff files.
        Apply(ApplyCmd),

        /// Compare two archives. This is used to verify if a diff-apply pipeline works correctly.
        Compare {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "NEW", value_hint = ValueHint::FilePath)]
            new: PathBuf,
        },

        /// Filter chunk files by `tiles_range`
        Filter {
            #[arg(value_name = "BASE", value_hint = ValueHint::FilePath)]
            base: PathBuf,

            #[arg(value_name = "OUTPUT", value_hint = ValueHint::FilePath)]
            output: PathBuf,

            #[command(flatten)]
            tiles_range_arg: TilesRangeArg,
        },

        /// Print info of a diff file
        Show {
            #[arg(value_hint = ValueHint::FilePath)]
            diff: PathBuf,
        },

        /// Test a diff file.
        Test {
            #[arg(value_hint = ValueHint::FilePath)]
            diff: PathBuf,
        },
    }

    #[derive(Args, Debug)]
    pub struct TilesRangeArg {
        /// Range of tiles. Format: <x-min>,<x-max>,<y-min>,<y-max>
        #[arg(short = 'r', long)]
        pub tiles_range: Option<String>,
    }

    impl TilesRangeArg {
        pub fn parse(&self) -> Option<TilesRange> {
            self.tiles_range
                .as_ref()
                .and_then(|x| TilesRange::parse_str(x))
        }
    }

    #[derive(Args, Debug)]
    pub struct ApplyCmd {
        /// Initial archive. Tarball/folder is supported.
        #[arg(value_hint = clap::ValueHint::FilePath)]
        pub initial: PathBuf,

        /// Diff files to be applied.
        #[arg(value_hint = clap::ValueHint::FilePath, num_args = 1.., required = true)]
        pub diffs: Vec<PathBuf>,

        /// The final produced snapshot path after all diffs being applied.
        #[arg(value_hint = clap::ValueHint::FilePath, short, long)]
        pub output: Option<PathBuf>,

        /// Add this flag when `output` is not specified.
        #[arg(long)]
        pub dry_run: bool,

        /// Disable checksum validation.
        #[arg(long)]
        pub no_checksum: bool,
    }
}

#[inline(always)]
fn compare_png(base: impl AsRef<Path>, new: impl AsRef<Path>) -> anyhow::Result<bool> {
    let mut img1 = vec![0_u8; CHUNK_LENGTH];
    let mut img2 = vec![0_u8; CHUNK_LENGTH];
    read_png(base, &mut img1)?;
    read_png(new, &mut img2)?;
    Ok(img1 == img2)
}

/// Diff the two buffer. New data will be written back to `base_buf`.
#[inline(always)]
fn diff_chunk(base_buf: &mut [u8], new_buf: &[u8]) {
    for (b, &n) in base_buf.iter_mut().zip(new_buf) {
        let i1 = *b & PALETTE_INDEX_MASK;
        let i2 = n & PALETTE_INDEX_MASK;

        let mutated = i2 | MUTATION_MASK;

        *b = if i1 == i2 { 0 } else { mutated };
    }
}

/// Returns compressed diff between two images.
#[inline(always)]
fn diff_chunk_compressed(base_buf: &mut [u8], new_buf: &[u8]) -> anyhow::Result<Vec<u8>> {
    diff_chunk(base_buf, new_buf);
    Ok(zstd::encode_all(
        &mut (&base_buf[..]),
        DIFF_DATA_ZSTD_COMPRESSION_LEVEL,
    )?)
}

thread_local! {
    static COMPRESSOR_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = cli::Cli::parse();
    match args.command {
        Commands::Diff { base, new, output } => {
            // a special handle for directly processing tar files
            if base.extension() == Some(OsStr::new("tar"))
                && new.extension() == Some(OsStr::new("tar"))
            {
                do_diff_for_tar(base, new, output)?;
                return Ok(());
            }

            do_diff_for_directory(base, new, output)?;
        }

        Commands::Apply(cmd) => {
            apply::main(cmd)?;
        }

        Commands::Compare { base, new } => {
            info!("Collecting files 'base'...");
            let mut base_collected = collect_chunks(&base, None)?;
            info!("Collecting files 'new'...");
            let mut new_collected = collect_chunks(&new, None)?;

            base_collected.sort();
            new_collected.sort();
            if base_collected != new_collected {
                return Err(anyhow::anyhow!("File lists differ."));
            }

            let length = base_collected.len();
            info!("Processing {} files...", length);
            let progress = stylized_progress_bar(length as u64);

            // job-stealing parallelization is enough here
            base_collected.into_iter().par_bridge().for_each(|(x, y)| {
                let base_file = base.join(format!("{x}/{y}.png"));
                let new_file = new.join(format!("{x}/{y}.png"));
                let result = compare_png(&base_file, &new_file).unwrap();
                if !result {
                    info!("{} and {} differ", base_file.display(), new_file.display());
                }
                progress.inc(1);
            });
            progress.finish();
        }

        Commands::Filter {
            base,
            output,
            tiles_range_arg,
        } => {
            fs::create_dir_all(&output)?;
            info!("Collecting files...");
            let collected = collect_chunks(&base, tiles_range_arg.parse())?;
            info!("Processing {} files...", collected.len());
            let progress = stylized_progress_bar(collected.len() as u64);

            collected.into_par_iter().for_each(|(x, y)| {
                let base_file = base.join(format!("{x}/{y}.png"));
                let output_file = new_chunk_file(&output, (x, y), "png");
                fs::copy(base_file, output_file).unwrap();
                progress.inc(1);
            });
            progress.finish();
        }

        Commands::Show { diff } => {
            let mut diff_file = diff::DiffFile::open(File::open_buffered(&diff)?)?;
            println!("Version: {}", diff::VERSION);
            println!(
                "Metadata: {}",
                serde_json::to_string(&diff_file.metadata).unwrap()
            );
            let index = diff_file.collect_index()?;
            println!("Total chunks: {}", index.len());
            println!(
                "Changed chunks: {}",
                index.iter().filter(|x| x.1.is_changed()).count()
            );
        }

        Commands::Test { diff } => {
            let mut reader = diff::DiffFile::open(File::open_buffered(&diff)?)?;
            let index = reader.collect_index()?;
            assert_eq!(reader.entry_count as usize, index.len());
            let pb = stylized_progress_bar(index.len() as u64);
            index.into_par_iter().for_each(|(_n, e)| {
                let result: anyhow::Result<()> = try {
                    match e.is_changed() {
                        false => {}
                        true => {
                            let portion = open_file_range(&diff, e.pos, e.len)?;

                            let mut decoder = zstd::Decoder::new(portion)?;
                            io::copy(&mut decoder, &mut io::sink())?;
                        }
                    }
                    pb.inc(1);
                };
                result.exit_on_error();
            });
            pb.finish();
            println!("Done.");
        }
    }

    Ok(())
}

fn do_diff(
    base_fetcher: impl ChunkFetcher + Send + Sync + 'static,
    new_fetcher: impl ChunkFetcher + Send + Sync + 'static,
    output: PathBuf,
) -> anyhow::Result<()> {
    info!("Creating diff file...");
    let mut output_dir = output
        .parent()
        .expect("Can not get parent of the output file");
    if output_dir == Path::new("") {
        output_dir = Path::new(".");
    }
    let temp_file = NamedTempFile::new_in(output_dir)?;
    debug!("temp_file: {}", temp_file.as_ref().display());
    let output_file = File::create_buffered(temp_file.as_ref())?;
    let mut diff_file =
        diff::DiffFileWriter::create(output_file, diff::Metadata::default(), diff::VERSION)?;

    let (tx, rx) = sync_channel(1024);
    info!("Processing {} files...", new_fetcher.chunks_len());

    let progress = stylized_progress_bar(new_fetcher.chunks_len() as u64);
    spawn(move || {
        let chunks_iter = new_fetcher.chunks_iter();
        chunks_iter.par_bridge().for_each_with(
            (tx, chunk_buf!(), chunk_buf!()),
            |(tx, base_buf, new_buf), (x, y)| {
                let result: anyhow::Result<()> = try {
                    let present = new_fetcher.fetch((x, y), new_buf)?;
                    assert!(present);
                    let base_chunk_present = base_fetcher.fetch((x, y), base_buf)?;
                    if !base_chunk_present {
                        base_buf.fill(0);
                    }

                    let checksum = chunk_checksum(new_buf);

                    // It's expecting that a large percent of the chunks are not mutated.
                    // Thus in this case, only computing diff for changed chunks can reduce the process time.
                    let compressed_diff = if !base_chunk_present || base_buf != new_buf {
                        let compressed_diff = diff_chunk_compressed(base_buf, new_buf).unwrap();
                        Some(compressed_diff)
                    } else {
                        None
                    };
                    tx.send((x, y, compressed_diff, checksum)).unwrap();
                    progress.inc(1);
                };
                result.exit_on_error();
            },
        );
        progress.finish();
    });

    for (x, y, diff, checksum) in rx {
        diff_file.add_entry((x, y), diff.as_deref(), checksum)?;
    }
    diff_file.finalize()?;
    temp_file.persist(output)?;
    Ok(())
}

fn do_diff_for_directory(base: PathBuf, new: PathBuf, output: PathBuf) -> anyhow::Result<()> {
    info!("Collecting files...");
    let new_fetcher = DirChunkFetcher::new(&new, true)?;
    let base_fetcher = DirChunkFetcher::new(&base, false)?;

    do_diff(base_fetcher, new_fetcher, output)?;
    Ok(())
}

fn do_diff_for_tar(base: PathBuf, new: PathBuf, output: PathBuf) -> anyhow::Result<()> {
    info!("Indexing 'base' tarball...");
    let base_tar = TarChunkFetcher::new(&base)?;
    info!("Indexing 'new' tarball...");
    let new_tar = TarChunkFetcher::new(&new)?;

    do_diff(base_tar, new_tar, output)?;
    Ok(())
}

mod apply {
    use crate::cli::ApplyCmd;
    use log::{info, warn};
    use rayon::iter::{ParallelBridge, ParallelIterator};
    use std::fs::File;
    use std::io;
    use std::io::{Cursor, Write};
    use std::path::Path;
    use std::process::exit;
    use wplace_tools::diff::{DiffFile, IndexEntry};
    use wplace_tools::indexed_png::{read_png_reader, write_chunk_png};
    use wplace_tools::{
        CHUNK_NUMBER_TOTAL, ChunkFetcher, ChunkNumber, ChunkProcessError, DirChunkFetcher,
        ExitOnError, TarChunkFetcher, apply_chunk, chunk_buf, diff, new_chunk_file,
        open_file_range, stylized_progress_bar, validate_chunk_checksum, zstd_decompress,
    };
    use yeet_ops::yeet;

    const ARRAY_LEN: usize = CHUNK_NUMBER_TOTAL * CHUNK_NUMBER_TOTAL;
    const ZSTD_LEVEL: i32 = 3;

    #[inline(always)]
    /// A simple transform from ChunkNumber(u16, u16) to the usize array index.
    const fn array_index(n: ChunkNumber) -> usize {
        (n.0 as usize * CHUNK_NUMBER_TOTAL) + n.1 as usize
    }

    type DynFetcher = dyn ChunkFetcher + Send + Sync + 'static;

    fn apply_1st_diff(
        memory: Option<&mut [Option<Vec<u8>>]>,
        base_fetcher: &DynFetcher,
        diff: impl AsRef<Path>,
        output: Option<&Path>,
        no_checksum: bool,
    ) -> anyhow::Result<()> {
        let diff = diff.as_ref();

        let mut diff_file = diff::DiffFile::open(File::open_buffered(diff)?)?;
        let index = diff_file.collect_index()?;
        let changed_chunks = index
            .iter()
            .filter(|x| x.1.is_changed())
            .collect::<Vec<_>>();
        let unchanged_chunks = index
            .iter()
            .filter(|x| !x.1.is_changed())
            .collect::<Vec<_>>();

        let memory_ptr = memory.map(|x| x.as_mut_ptr() as usize);

        macro cell_ptr($n:expr, $ptr:expr) {
            unsafe { &mut *($ptr as *mut Option<Vec<u8>>).add(array_index($n)) }
        }

        let pb = stylized_progress_bar(changed_chunks.len() as u64);
        changed_chunks.into_iter().par_bridge().for_each_with(
            (chunk_buf!(), chunk_buf!()),
            |(raw_diff, base_buf), x| {
                let result: anyhow::Result<()> = try {
                    let &n = x.0;
                    let entry = x.1;

                    let diff_reader = open_file_range(diff, entry.pos, entry.len)?;
                    zstd_decompress(diff_reader, raw_diff)?;

                    // if the base chunk is not present, the buffer will be just zeros
                    // - totally fine for the applying process.
                    let base_present = base_fetcher.fetch(n, base_buf)?;
                    if !base_present {
                        base_buf.fill(0);
                    }

                    apply_chunk(
                        base_buf,
                        <&[_; _]>::try_from(&raw_diff[..])
                            .expect("Raw diff data length is expected to be 1_000_000"),
                    );

                    if !no_checksum {
                        validate_chunk_checksum(base_buf, entry.checksum)?;
                    }
                    if let Some(p) = memory_ptr {
                        let cell_ptr = cell_ptr!(n, p);
                        *cell_ptr = Some(zstd::encode_all(&base_buf[..], ZSTD_LEVEL)?);
                    }
                    if let Some(output) = output {
                        let chunk_png = new_chunk_file(output, n, "png");
                        write_chunk_png(chunk_png, base_buf)?;
                    }

                    pb.inc(1);
                };

                result
                    .map_err(|e| ChunkProcessError {
                        inner: e,
                        chunk_number: *x.0,
                        diff_file: None,
                    })
                    .exit_on_error();
            },
        );
        pb.finish();

        info!("Processing unchanged chunks...");
        let pb = stylized_progress_bar(unchanged_chunks.len() as u64);
        unchanged_chunks.into_iter().par_bridge().for_each_with(
            chunk_buf!(),
            |buf, (&n, entry)| {
                let result: anyhow::Result<()> = try {
                    let png_raw = base_fetcher.fetch_raw(n)?;

                    if !no_checksum {
                        read_png_reader(Cursor::new(&png_raw), buf)?;
                        validate_chunk_checksum(buf, entry.checksum).exit_on_error();
                    }
                    if let Some(p) = memory_ptr {
                        let cell_ptr = cell_ptr!(n, p);
                        *cell_ptr = Some(zstd::encode_all(&buf[..], ZSTD_LEVEL)?);
                    }
                    if let Some(output) = output {
                        let chunk_png = new_chunk_file(output, n, "png");
                        io::copy(&mut (&png_raw[..]), &mut File::create_buffered(chunk_png)?)?;
                    }

                    pb.inc(1);
                };
                result.exit_on_error();
            },
        );
        pb.finish();

        Ok(())
    }

    fn apply_non_1st_diff(
        memory: &mut [Option<Vec<u8>>],
        diff: impl AsRef<Path>,
        output: Option<&Path>,
        no_checksum: bool,
    ) -> anyhow::Result<()> {
        let diff_path = diff.as_ref();

        let mut diff_file = DiffFile::open(File::open(diff_path)?)?;
        let index = diff_file.collect_index()?;
        let changed_entries: Vec<(ChunkNumber, IndexEntry)> = index
            .iter()
            .filter(|(_, e)| e.is_changed())
            .map(|x| (*x.0, *x.1))
            .collect();

        let memory_ptr = memory.as_mut_ptr() as usize;

        let decompress_to = |n: ChunkNumber, to: &mut [u8]| {
            // SAFETY: Rayon ensures unique access to each entry during iteration,
            // and we are accessing index 'idx' which is unique to ChunkNumber 'n'
            let cell_ptr = unsafe {
                let ptr = memory_ptr as *mut Option<Vec<u8>>;
                &mut *ptr.add(array_index(n))
            };

            // Decompress existing base
            if let Some(compressed_base) = cell_ptr.as_ref() {
                zstd_decompress(Cursor::new(compressed_base), to).unwrap();
            } else {
                to.fill(0);
            }
            cell_ptr
        };

        let pb = stylized_progress_bar(changed_entries.len() as u64);
        changed_entries.into_iter().par_bridge().for_each_with(
            (chunk_buf!(), chunk_buf!()),
            |(base_buf, diff_data_buf), (n, entry)| {
                let result: anyhow::Result<()> = try {
                    let cell_ptr = decompress_to(n, base_buf);

                    // Load and apply diff
                    let diff_reader = open_file_range(diff_path, entry.pos, entry.len)?;
                    zstd_decompress(diff_reader, diff_data_buf)?;

                    apply_chunk(base_buf, (&diff_data_buf[..]).try_into().unwrap());
                    if !no_checksum {
                        validate_chunk_checksum(base_buf, entry.checksum).exit_on_error();
                    }

                    // Recompress and update memory
                    if let Some(buf) = &mut *cell_ptr {
                        buf.clear();
                        let mut encoder = zstd::Encoder::new(buf, ZSTD_LEVEL)?;
                        encoder.write_all(base_buf)?;
                        encoder.finish()?;
                    } else {
                        let re_compressed = zstd::encode_all(&base_buf[..], ZSTD_LEVEL)?;
                        *cell_ptr = Some(re_compressed);
                    }
                    pb.inc(1);
                };
                result.exit_on_error();
            },
        );
        pb.finish();

        if let Some(output) = output {
            info!("Writing to disk...");
            let pb = stylized_progress_bar(index.len() as u64);
            index
                .into_iter()
                .par_bridge()
                .for_each_with(chunk_buf!(), |buf, (n, entry)| {
                    let _cell_ptr = decompress_to(n, buf);
                    write_chunk_png(new_chunk_file(output, n, "png"), buf).unwrap();
                    if !no_checksum {
                        validate_chunk_checksum(buf, entry.checksum).exit_on_error();
                    }
                    pb.inc(1);
                });
            pb.finish();
        }

        Ok(())
    }

    pub fn main(mut args: ApplyCmd) -> anyhow::Result<()> {
        if !args.dry_run && args.output.is_none() {
            warn!(
                "`--output` is missed? Please add `--dry-run` when you intend to ignore the output."
            );
            exit(1);
        }
        if args.dry_run {
            args.output = None;
        }

        let base_fetcher: Box<DynFetcher> =
            if args.initial.extension().map(|x| x.to_ascii_lowercase()) == Some("tar".into()) {
                info!("Reading base tarball...");
                Box::new(TarChunkFetcher::new(&args.initial)?)
            } else if args.initial.is_dir() {
                Box::new(DirChunkFetcher::new(&args.initial, false)?)
            } else {
                yeet!(anyhow::anyhow!("Unknown 'base' file type"))
            };
        assert!(!args.diffs.is_empty(), "Clap ensures");

        let diff_total = args.diffs.len();
        let print_log = |i: usize, path: &Path| {
            info!(
                "Applying diff [{}/{}]: {}...",
                i,
                diff_total,
                path.display()
            );
        };

        if args.diffs.len() == 1 {
            print_log(1, &args.diffs[0]);
            // There's only one diff to be applied. No need to write to an intermediate memory.
            apply_1st_diff(
                None,
                &*base_fetcher,
                &args.diffs[0],
                args.output.as_deref(),
                args.no_checksum,
            )?;
            return Ok(());
        }

        // Buffer to store all the (intermediate) processed data
        let mut memory_store: Vec<Option<Vec<u8>>> = (0..ARRAY_LEN).map(|_| None).collect();

        let first = &args.diffs[0];
        let last = &args.diffs[args.diffs.len() - 1];
        let intermediates = &args.diffs[1..(args.diffs.len() - 1)];
        print_log(1, first);
        apply_1st_diff(
            Some(&mut memory_store),
            &*base_fetcher,
            first,
            None,
            args.no_checksum,
        )?;

        for (diff_i, diff_path) in intermediates.iter().enumerate() {
            print_log(diff_i + 1 + 1, diff_path);
            apply_non_1st_diff(&mut memory_store, diff_path, None, args.no_checksum)?;
        }

        print_log(diff_total, last);
        apply_non_1st_diff(
            &mut memory_store,
            last,
            args.output.as_deref(),
            args.no_checksum,
        )?;

        info!("Done.");
        Ok(())
    }
}

#[test]
fn test() {}
