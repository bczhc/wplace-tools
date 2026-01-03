#![allow(dead_code)]
//! Utility to read files inside a ZIP directly.

use crate::indexed_png::read_png_reader;
use crate::{CHUNK_LENGTH, ChunkNumber};
use lazy_regex::regex;
use rawzip::RECOMMENDED_BUFFER_SIZE;
use rawzip::path::{RawPath, ZipFilePath};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

type ChunkIndexMap = HashMap<ChunkNumber, (u64, u64)>;

pub struct ChunksZipReader {
    zip_file: BufReader<File>,
    pub map: ChunkIndexMap,
}

impl ChunksZipReader {
    pub fn open(zip: impl AsRef<Path>) -> anyhow::Result<Self> {
        let map = collect_zip_entries(&zip)?;
        Ok(Self {
            map,
            zip_file: File::open_buffered(zip)?,
        })
    }

    /// Returns None if no such chunk is present
    pub fn read_chunk(&mut self, c: ChunkNumber) -> io::Result<Option<Vec<u8>>> {
        let Some(&range) = self.map.get(&c) else {
            return Ok(None);
        };
        self.zip_file.seek(SeekFrom::Start(range.0))?;
        let take = self.zip_file.by_ref().take(range.1);
        let mut buf = vec![0_u8; CHUNK_LENGTH];
        read_png_reader(take, &mut buf).unwrap();
        Ok(Some(buf))
    }
}

fn collect_zip_entries(path: impl AsRef<Path>) -> anyhow::Result<ChunkIndexMap> {
    let mut buffer = [0_u8; RECOMMENDED_BUFFER_SIZE];
    let zip = rawzip::ZipArchive::from_file(File::open(path)?, &mut buffer)?;
    let mut entries = zip.entries(&mut buffer);
    assert!(zip.entries_hint() > 1);
    let first = entries.next_entry()?.unwrap();
    // expect the first one is the root directory
    let file_path = unwrap_file_path(first.file_path());
    assert!(file_path.ends_with('/'));
    let root_path = String::from(file_path);

    let mut map = HashMap::new();
    loop {
        let Some(e) = entries.next_entry()? else {
            break;
        };
        let file_path = unwrap_file_path(e.file_path());
        assert!(file_path.starts_with(&root_path));
        let filename = &file_path[root_path.len()..];

        // skip directory entries
        if filename.ends_with('/') {
            continue;
        }

        let chunk_path_regex = regex!(r"^(\d+)/(\d+)\.png$");
        assert!(chunk_path_regex.is_match(filename));
        let captures = chunk_path_regex.captures(filename).unwrap();
        let chunk_x = captures
            .get(1)
            .unwrap()
            .as_str()
            .parse::<u16>()
            .expect("Not an integer");
        let chunk_y = captures
            .get(2)
            .unwrap()
            .as_str()
            .parse::<u16>()
            .expect("Not an integer");

        let data_range = zip.get_entry(e.wayfinder())?.compressed_data_range();
        map.insert((chunk_x, chunk_y), data_range);
    }

    Ok(map)
}

#[inline(always)]
fn unwrap_file_path(path: ZipFilePath<RawPath<'_>>) -> &str {
    std::str::from_utf8(path.as_bytes()).expect("Invalid UTF-8")
}
