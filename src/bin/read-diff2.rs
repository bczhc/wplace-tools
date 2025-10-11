#![feature(file_buffered)]

use std::fs::File;
use wplace_tools::diff2;

fn main() -> anyhow::Result<()>{
    let path = "/mnt/nvme/wplace-archives/mine/new-diff/2025-08-09T22-23-45.217Z.diff";
    let mut r = diff2::DiffFile::open(File::open_buffered(path)?)?;
    println!("{}", r.index_pos);
    println!("{}", r.entry_count);
    let map = r.read_index()?;
    println!("{}", map.len());
    for x in map.iter().take(10) {
        println!("{:?}", x);
    }
    Ok(())
}
