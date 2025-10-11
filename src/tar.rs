use crate::{open_file_range, ChunkNumber};
use lazy_regex::regex;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom, Take};
use std::path::{Path, PathBuf};
use tar::EntryType;

pub struct Range {
    pub start: u64,
    pub size: u64,
}

pub struct ChunksTarReader {
    pub map: BTreeMap<ChunkNumber, Range>,
    path: PathBuf,
    pub root_name: String,
}

impl ChunksTarReader {
    pub fn open_with_index(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let (index, root_name) = Self::index_chunks(&path)?;
        Ok(Self {
            map: index,
            path: path.as_ref().into(),
            root_name,
        })
    }

    fn index_chunks(
        path: impl AsRef<Path>,
    ) -> anyhow::Result<(BTreeMap<ChunkNumber, Range>, String)> {
        let mut map = BTreeMap::new();

        let mut tar = tar::Archive::new(File::open_buffered(path)?);

        let mut entries = tar.entries()?;
        let first = entries.next().expect("No entries")?;
        // the first one is expected to be the root directory
        assert_eq!(first.header().entry_type(), EntryType::Directory);
        let root_name = first
            .path()?
            .file_name()
            .expect("Can't get filename")
            .to_str()
            .expect("Invalid UTF-8")
            .to_string();
        for x in entries {
            let x = x?;
            if x.header().entry_type() != EntryType::Regular {
                continue;
            }
            let regex = regex!(r".*/(\d+)/(\d+)\.png$");
            let cow = x.path()?;
            let path_str = cow.to_str().expect("Invalid UTF-8");
            assert!(regex.is_match(&path_str));
            let group = regex.captures(&path_str).unwrap();
            let chunk_number: ChunkNumber = (
                group.get(1).unwrap().as_str().parse()?,
                group.get(2).unwrap().as_str().parse()?,
            );
            let range = Range {
                start: x.raw_file_position(),
                size: x.size(),
            };
            map.insert(chunk_number, range);
        }
        Ok((map, root_name))
    }

    pub fn open_chunk(
        &self,
        chunk_number: ChunkNumber,
    ) -> Option<io::Result<Take<BufReader<File>>>> {
        match self.map.get(&chunk_number) {
            None => None,
            Some(range) => {
                Some(open_file_range(&self.path, range.start, range.size))
            }
        }
    }
}
