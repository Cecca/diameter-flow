mod morton;
mod stream;

use bitstream_io::*;
use std::fmt::Debug;
use std::io::{Read, Result as IOResult, Write};
use std::path::{Path, PathBuf};

pub struct CompressedEdgesBlockSet {
    blocks: Vec<CompressedEdges>,
}

impl CompressedEdgesBlockSet {
    pub fn from_files<P, I>(paths: I) -> IOResult<Self>
    where
        P: AsRef<Path> + Debug,
        I: IntoIterator<Item = (P, Option<P>)>,
    {
        let mut blocks = Vec::new();
        for (path, weights_path) in paths.into_iter() {
            blocks.push(CompressedEdges::from_file(path, weights_path)?);
        }
        Ok(Self { blocks })
    }

    pub fn from_dir<P: AsRef<Path>, F: Fn(u64) -> bool>(path: P, filter: F) -> IOResult<Self> {
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
                    unimplemented!("read the weights, if present");
                    paths.push((path, None));
                }
            }
        }

        Self::from_files(paths)
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (u32, u32, u32)> + 'a {
        self.blocks.iter().flat_map(|b| b.iter())
    }
}

pub struct CompressedEdges {
    raw: Vec<u8>,
    weights: Option<Vec<u32>>,
}

impl CompressedEdges {
    /// Loads the contents of the file in memory, for doing multiple iterations faster
    pub fn from_file<P: AsRef<Path> + Debug>(path: P, weights_path: Option<P>) -> IOResult<Self> {
        use std::fs::File;
        use std::io::BufReader;
        use std::io::Error;
        use std::io::ErrorKind::UnexpectedEof;
        let mut reader = BufReader::new(File::open(path)?);
        let mut raw = Vec::new();
        reader.read_to_end(&mut raw)?;
        let weights = weights_path.map(|path| {
            println!("reading weights from {:?}", path);
            let mut weights = Vec::new();
            let mut reader = BitReader::<_, BE>::new(BufReader::new(
                File::open(path).expect("failed to open the weights file"),
            ));
            loop {
                match reader.read(32) {
                    Ok(w) => weights.push(w),
                    Err(e) => match e.kind() {
                        UnexpectedEof => break,
                        _ => panic!("{:?}", e),
                    },
                }
            }

            weights
        });
        Ok(Self { raw, weights })
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (u32, u32, u32)> + 'a {
        use std::io::Cursor;
        let cursor = Cursor::new(&self.raw);
        let mut reader = stream::DifferenceStreamReader::new(cursor);
        let mut weights = self.weights.as_ref().map(|vec| vec.iter());

        std::iter::from_fn(move || {
            let z = reader.read().expect("problem reading form the stream");
            if z == 0 {
                None
            } else {
                let (u, v) = morton::zorder_to_pair(z);
                let w = if let Some(iter) = weights.as_mut() {
                    *iter.next().expect("weights exhausted too soon!")
                } else {
                    1
                };
                Some((u, v, w))
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

pub struct CompressedTripletsWriter {
    output_path: PathBuf,
    encoded: Vec<(u64, u32)>,
    block_size: u64,
}

impl CompressedTripletsWriter {
    pub fn to_file<P: AsRef<Path>>(path: P, block_size: u64) -> Self {
        Self {
            output_path: path.as_ref().to_path_buf(),
            encoded: Vec::new(),
            block_size,
        }
    }

    pub fn write(&mut self, (u, v, w): (u32, u32, u32)) {
        self.encoded.push((morton::pair_to_zorder((u, v)), w));
    }

    fn flush(&mut self) -> IOResult<()> {
        use std::fs::File;
        use std::io::BufWriter;

        println!("Flushing compressed edges and weights in multiple files");

        self.encoded.sort();

        if !self.output_path.is_dir() {
            std::fs::create_dir(self.output_path.clone())?;
        }

        let get_path = |id| {
            let mut path = self.output_path.clone();
            path.push(format!("part-{}.bin", id));
            path
        };
        let get_path_weights = |id| {
            let mut path = self.output_path.clone();
            path.push(format!("weights-{}.bin", id));
            path
        };

        let mut part_id = 0;
        let p = get_path(part_id);
        println!("opening {:?}", p);
        let writer = BufWriter::new(File::create(p)?);
        let mut writer = stream::DifferenceStreamWriter::new(writer);
        let mut weights_writer =
            BitWriter::<_, BE>::new(BufWriter::new(File::create(get_path_weights(part_id))?));
        let mut cnt = 0;
        for &(x, w) in self.encoded.iter() {
            if cnt % self.block_size == 0 {
                part_id += 1;
                let p = get_path(part_id);
                println!("opening {:?}", p);
                let mut writer2 =
                    stream::DifferenceStreamWriter::new(BufWriter::new(File::create(p)?));
                let mut weights_writer2 =
                    BitWriter::new(BufWriter::new(File::create(get_path_weights(part_id))?));
                std::mem::swap(&mut writer, &mut writer2);
                std::mem::swap(&mut weights_writer, &mut weights_writer2);
                writer2.close()?;
                weights_writer2.into_writer().flush()?;
            }
            if writer.is_new_elem(x) {
                // Remove duplicate edges
                writer.write(x)?;
                weights_writer.write(32, w);
            }
            cnt += 1;
        }
        writer.close()?;
        weights_writer.into_writer().flush()?;
        Ok(())
    }
}

impl Drop for CompressedTripletsWriter {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}
