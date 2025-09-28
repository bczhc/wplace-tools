#![feature(file_buffered)]

use lazy_regex::regex;
use std::fs::File;
use std::path::Path;
use rayon::prelude::*;
use wplace_tools::diff_file::{DiffFileReader, DiffFileWriter};

fn alter_names(path: impl AsRef<Path>, output: impl AsRef<Path>) -> anyhow::Result<()> {
    let diff_file = DiffFileReader::new(File::open_buffered(path)?)?;

    let extract = |name: &str| {
        let regex = regex!(r"(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{3}Z)");
        regex
            .captures_iter(name)
            .next()
            .unwrap()
            .get(1)
            .unwrap()
            .as_str()
            .to_string()
    };

    let mut new_metadata = diff_file.metadata.clone();
    new_metadata.name = extract(&diff_file.metadata.name).into();
    new_metadata.parent = extract(&diff_file.metadata.parent).into();

    let mut writer = DiffFileWriter::new(
        File::create_buffered(output)?,
        new_metadata,
        diff_file.index.clone(),
    )?;

    for x in diff_file.chunk_diff_iter() {
        let x = x?;
        writer.add_chunk_diff(x.0, &x.1)?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let names = [
        "2025-08-23T09-56-23.874Z.diff",
        "2025-08-23T12-56-03.346Z.diff",
        "2025-08-23T15-59-27.962Z.diff",
        "2025-08-23T19-13-56.459Z.diff",
        "2025-08-23T22-40-12.027Z.diff",
        "2025-08-24T01-38-10.189Z.diff",
        "2025-08-24T04-38-35.525Z.diff",
        "2025-08-24T07-37-56.521Z.diff",
        "2025-08-24T10-51-02.763Z.diff",
        "2025-08-24T13-59-34.565Z.diff",
        "2025-08-24T17-06-51.821Z.diff",
        "2025-08-24T21-31-33.285Z.diff",
        "2025-08-25T00-38-57.447Z.diff",
        "2025-08-25T03-55-58.133Z.diff",
        "2025-08-25T07-11-39.763Z.diff",
        "2025-08-25T10-32-50.456Z.diff",
        "2025-08-25T13-55-23.230Z.diff",
    ];

    let dir = Path::new("/mnt/nvme/wplace-archives/mine/1");
    names.into_par_iter().for_each(|x| {
        println!("{}", x);
        let diff_file = dir.join(x);
        alter_names(diff_file, dir.join("new").join(x)).unwrap();
    });
    Ok(())
}
