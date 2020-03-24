mod morton;
mod stream;

use std::io::{Read, Result as IOResult, Write};
use std::path::{Path, PathBuf};

pub struct CompressedEdgesBlockSet {
    blocks: Vec<CompressedEdges>,
    // block_length: u64,
    // num_node_groups: u64,
}

impl CompressedEdgesBlockSet {
    pub fn from_files<P: AsRef<Path>, I: IntoIterator<Item = P>>(paths: I) -> IOResult<Self> {
        let mut blocks = Vec::new();
        for path in paths.into_iter() {
            blocks.push(CompressedEdges::from_file(path)?);
        }
        Ok(Self { blocks })
    }

    pub fn from_dir<P: AsRef<Path>, F: Fn(u64) -> bool>(path: P, filter: F) -> IOResult<Self> {
        // let mut metadata_path = path.as_ref().to_path_buf();
        // metadata_path.push("metadata.properties");
        // let metadata = BufReader::new(File::open(metadata_path)?);
        // let mut block_length = None;
        // let mut num_node_groups = None;
        // let rex_block_length =
        //     regex::Regex::new(r"blockLength=(\d+)").expect("problem building regex");
        // let rex_node_groups =
        //     regex::Regex::new(r"numNodeGroups=(\d+)").expect("problem building regex");
        // for line in metadata.lines() {
        //     let line = line.expect("problem reading line");
        //     if let Some(caps) = rex_block_length.captures(&line) {
        //         block_length.replace(
        //             caps.get(1)
        //                 .unwrap()
        //                 .as_str()
        //                 .parse::<u64>()
        //                 .expect("problem parsing"),
        //         );
        //     }
        //     if let Some(caps) = rex_node_groups.captures(&line) {
        //         num_node_groups.replace(
        //             caps.get(1)
        //                 .unwrap()
        //                 .as_str()
        //                 .parse::<u64>()
        //                 .expect("problem parsing"),
        //         );
        //     }
        // }
        // let block_length = block_length.expect("missing chunkLength in metadata");
        // let num_node_groups = num_node_groups.expect("missing numNodeGroups in metadata");

        // let mut blocks = Vec::new();
        let mut paths = Vec::new();

        let rex = regex::Regex::new(r"\d+").expect("error building regex");
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(digits) = rex.find(
                path.file_name()
                    .expect("unable to get file name")
                    .to_str()
                    .expect("unable to convert to string"),
            ) {
                let chunk_start: u64 = digits.as_str().parse().expect("problem parsing");
                if filter(chunk_start) {
                    // blocks.push(CompressedEdges::from_file(path)?);
                    paths.push(path);
                }
            }
        }

        Self::from_files(paths)
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (u32, u32, u32)> + 'a {
        self.blocks
            .iter()
            .flat_map(|b| b.iter().map(|(u, v)| (u, v, 1)))
    }
}

pub struct CompressedEdges {
    raw: Vec<u8>,
}

impl CompressedEdges {
    /// Loads the contents of the file in memory, for doing multiple iterations faster
    pub fn from_file<P: AsRef<Path>>(path: P) -> IOResult<Self> {
        use std::fs::File;
        use std::io::BufReader;
        let mut reader = BufReader::new(File::open(path)?);
        let mut raw = Vec::new();
        reader.read_to_end(&mut raw)?;
        Ok(Self { raw })
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (u32, u32)> + 'a {
        use std::io::Cursor;
        let cursor = Cursor::new(&self.raw);
        let mut reader = stream::DifferenceStreamReader::new(cursor);

        std::iter::from_fn(move || {
            let z = reader.read().expect("problem reading form the stream");
            if z == 0 {
                None
            } else {
                Some(morton::zorder_to_pair(z))
            }
        })
    }
}

pub struct CompressedPairsWriter {
    output_path: PathBuf,
    encoded: Vec<u64>,
    block_size: u64,
}

impl CompressedPairsWriter {
    pub fn to_file<P: AsRef<Path>>(path: P, block_size: u64) -> Self {
        Self {
            output_path: path.as_ref().to_path_buf(),
            encoded: Vec::new(),
            block_size,
        }
    }

    pub fn write(&mut self, pair: (u32, u32)) {
        self.encoded.push(morton::pair_to_zorder(pair));
    }

    fn flush(&mut self) -> IOResult<()> {
        use std::fs::File;
        use std::io::BufWriter;

        println!("Flushing compressed edges in multiple files");

        self.encoded.sort();

        if !self.output_path.is_dir() {
            std::fs::create_dir(self.output_path.clone())?;
        }

        let get_path = |id| {
            let mut path = self.output_path.clone();
            path.push(format!("part-{}.bin", id));
            path
        };

        let mut part_id = 0;
        let p = get_path(part_id);
        println!("opening {:?}", p);
        let writer = BufWriter::new(File::create(p)?);
        let mut writer = stream::DifferenceStreamWriter::new(writer);
        let mut cnt = 0;
        for &x in self.encoded.iter() {
            if cnt % self.block_size == 0 {
                part_id += 1;
                let p = get_path(part_id);
                println!("opening {:?}", p);
                let mut writer2 =
                    stream::DifferenceStreamWriter::new(BufWriter::new(File::create(p)?));
                std::mem::swap(&mut writer, &mut writer2);
                writer2.close()?;
            }
            if writer.is_new_elem(x) {
                // Remove duplicate edges
                writer.write(x)?;
            }
            cnt += 1;
        }
        writer.close()?;
        Ok(())
    }
}

impl Drop for CompressedPairsWriter {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}
