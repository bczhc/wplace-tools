use clap::Parser;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use wplace_tools::{diff2, open_file_range, set_up_logger, diff3};
use log::info;

// 引入之前定义的 diff3 模块

#[derive(Parser, Debug)]
#[command(name = "diff2-to-diff3", version = "1.0", about = "Convert wplace diff2 format to diff3")]
struct Args {
    /// Path to the source diff2 file
    #[arg(index = 1)]
    input: PathBuf,

    /// Path to the output diff3 file
    #[arg(index = 2)]
    output: PathBuf,
}

fn main() -> anyhow::Result<()> {
    set_up_logger();
    let args = Args::parse();

    info!("Opening source diff2 file: {:?}", args.input);
    // Open the diff2 file and read its index [3]
    let mut old_diff = diff2::DiffFile::open(BufReader::new(File::open(&args.input)?))?;
    let old_index = old_diff.read_index()?;
    info!("Total entries in source: {}", old_index.len());

    info!("Creating destination diff3 file: {:?}", args.output);
    let out_file = File::create(&args.output)?;
    // Metadata can be carried over if necessary; here we use default [4]
    let mut writer = diff3::DiffFileWriter::create(out_file, diff3::Metadata::default())?;

    let pb = wplace_tools::stylized_progress_bar(old_index.len() as u64);

    for (n, entry) in old_index {
        match entry.diff_data_range {
            diff2::DiffDataRange::Changed { pos, len } => {
                // Read the compressed diff data from the old file [5, 6]
                let mut data_reader = open_file_range(&args.input, pos, len)?;
                let mut buffer = vec![0_u8; len as usize];
                data_reader.read_exact(&mut buffer)?;

                // Write to diff3 writer (this handles data placement and index record)
                writer.add_entry(n, Some(&buffer), entry.checksum)?;
            }
            diff2::DiffDataRange::Unchanged => {
                // Record an unchanged entry (pos and len will be 0 in diff3)
                writer.add_entry(n, None, entry.checksum)?;
            }
        }
        pb.inc(1);
    }

    pb.finish();
    info!("Finalizing diff3 file (sorting index)...");
    // finalize handles sorting the entries by (x, y) for binary search support
    writer.finalize()?;

    info!("Conversion completed successfully.");
    Ok(())
}