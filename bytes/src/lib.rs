#[macro_use]
extern crate serde;

mod morton;
mod stream;

use bitstream_io::*;
use std::fmt::Debug;
use std::io::{Read, Result as IOResult, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy)]
pub enum LoadType {
    InMemory,
    Offline,
}

pub struct CompressedEdgesBlockSet {
    arrangement: Matrix,
    blocks: Vec<CompressedEdges>,
}

impl CompressedEdgesBlockSet {
    pub fn from_files<P, I>(arrangement: Matrix, load: LoadType, paths: I) -> IOResult<Self>
    where
        P: AsRef<Path> + Debug,
        I: IntoIterator<Item = (P, Option<P>)>,
    {
        let mut blocks = Vec::new();
        for (path, weights_path) in paths.into_iter() {
            blocks.push(CompressedEdges::from_file(load, path, weights_path)?);
        }

        Ok(Self {
            arrangement,
            blocks,
        })
    }

    pub fn node_processors(&self, x: u32) -> impl Iterator<Item = u32> {
        self.arrangement.node_processors(x)
    }

    pub fn for_each<F: FnMut(u32, u32, u32)>(&self, mut action: F) {
        for block in self.blocks.iter() {
            block.for_each(&mut action);
        }
    }

    pub fn byte_size(&self) -> u64 {
        self.blocks.iter().map(|b| b.byte_size()).sum()
    }

    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }
}

pub enum CompressedEdges {
    InMemory {
        raw: Vec<u8>,
        weights: Option<Vec<u32>>,
    },
    Offline {
        raw_path: PathBuf,
        weights_path: Option<PathBuf>,
    },
}

impl CompressedEdges {
    /// Loads the contents of the file in memory, for doing multiple iterations faster
    pub fn from_file<P: AsRef<Path> + Debug>(
        load: LoadType,
        path: P,
        weights_path: Option<P>,
    ) -> IOResult<Self> {
        match load {
            LoadType::Offline => Ok(Self::Offline {
                raw_path: path.as_ref().to_path_buf(),
                weights_path: weights_path.map(|p| p.as_ref().to_path_buf()),
            }),
            LoadType::InMemory => {
                use std::fs::File;
                use std::io::BufReader;
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
                Ok(Self::InMemory { raw, weights })
            }
        }
    }

    pub fn for_each<F: FnMut(u32, u32, u32)>(&self, action: &mut F) {
        match self {
            Self::InMemory { raw, weights } => {
                use std::io::Cursor;
                let cursor = Cursor::new(&raw);
                let mut reader = stream::DifferenceStreamReader::new(cursor);

                if let Some(weights) = weights.as_ref() {
                    let mut weights = weights.iter();
                    loop {
                        let z = reader.read().expect("problem reading form the stream");
                        if z == 0 {
                            return;
                        } else {
                            let (u, v) = morton::zorder_to_pair(z);
                            let w = *weights.next().expect("weights exhausted too soon!");
                            action(u, v, w);
                        }
                    }
                } else {
                    loop {
                        let z = reader.read().expect("problem reading form the stream");
                        if z == 0 {
                            return;
                        } else {
                            let (u, v) = morton::zorder_to_pair(z);
                            action(u, v, 1);
                        }
                    }
                }
            }
            Self::Offline {
                raw_path,
                weights_path,
            } => {
                use std::fs::File;
                use std::io::{BufRead, BufReader, Read};
                let file_reader = BufReader::new(File::open(raw_path).unwrap());
                let mut reader = stream::DifferenceStreamReader::new(file_reader);

                if let Some(weights_path) = weights_path.as_ref() {
                    let mut weights = BitReader::<_, BE>::new(BufReader::new(
                        File::open(weights_path).expect("failed to open the weights file"),
                    ));

                    loop {
                        let z = reader.read().expect("problem reading form the stream");
                        if z == 0 {
                            return;
                        } else {
                            let (u, v) = morton::zorder_to_pair(z);
                            let w = weights.read(32).expect("weights exhausted too soon!");
                            action(u, v, w);
                        }
                    }
                } else {
                    loop {
                        let z = reader.read().expect("problem reading form the stream");
                        if z == 0 {
                            return;
                        } else {
                            let (u, v) = morton::zorder_to_pair(z);
                            action(u, v, 1);
                        }
                    }
                }
            }
        }
    }

    pub fn byte_size(&self) -> u64 {
        match self {
            Self::InMemory { raw, weights } => raw.len() as u64 * 8,
            Self::Offline {
                raw_path,
                weights_path,
            } => {
                use std::fs::File;
                use std::io::Seek;
                use std::io::SeekFrom;
                File::open(raw_path)
                    .unwrap()
                    .seek(SeekFrom::End(0))
                    .unwrap()
            }
        }
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct Matrix {
    blocks_per_side: u32,
    elems_per_block: u32,
}

impl Matrix {
    pub fn new(blocks_per_side: u32, side_elements: u32) -> Self {
        let elems_per_block = (side_elements as f64 / blocks_per_side as f64).ceil() as u32;
        Self {
            blocks_per_side,
            elems_per_block,
        }
    }

    /// Gets the processors that might have edges incident to a node
    pub fn node_processors(&self, node: u32) -> impl Iterator<Item = u32> {
        let block_idx = node / self.elems_per_block;
        let n_blocks = self.blocks_per_side;
        let mut row_idx = 0;
        let mut col_idx = 0;
        std::iter::from_fn(move || {
            if row_idx < n_blocks {
                let res = Some(row_idx * n_blocks + block_idx);
                row_idx += 1;
                return res;
            } else if col_idx < n_blocks {
                let res = Some(block_idx * n_blocks + col_idx);
                col_idx += 1;
                return res;
            } else {
                return None;
            }
        })
    }

    pub fn row_major_block(&self, (x, y): (u32, u32)) -> u32 {
        // The index within a block
        let inner_x = x % self.elems_per_block;
        let inner_y = y % self.elems_per_block;
        // The index of the (square) block
        let block_x = x / self.elems_per_block;
        let block_y = y / self.elems_per_block;

        if inner_x < inner_y {
            // Upper triangle
            block_x * self.blocks_per_side + block_y
        } else {
            // Lower triangle
            block_y * self.blocks_per_side + block_x
        }
    }
}

pub struct CompressedPairsWriter {
    output_path: PathBuf,
    encoded: Vec<u64>,
    node_blocks: u32,
    max_id: u32,
}

impl CompressedPairsWriter {
    pub fn to_file<P: AsRef<Path>>(path: P, node_blocks: u32) -> Self {
        Self {
            output_path: path.as_ref().to_path_buf(),
            encoded: Vec::new(),
            node_blocks,
            max_id: 0,
        }
    }

    pub fn write(&mut self, pair: (u32, u32)) {
        self.max_id = std::cmp::max(self.max_id, std::cmp::max(pair.0, pair.1));
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

        let mut writers = Vec::new();
        for part_id in 0..(self.node_blocks * self.node_blocks) {
            let p = get_path(part_id);
            println!("opening {:?}", p);
            let writer = BufWriter::new(File::create(p)?);
            let writer = stream::DifferenceStreamWriter::new(writer);
            writers.push(writer);
        }

        let matrix = Matrix::new(self.node_blocks, self.max_id + 1);

        for &x in self.encoded.iter() {
            let writer = &mut writers[matrix.row_major_block(morton::zorder_to_pair(x)) as usize];
            if writer.is_new_elem(x) {
                // Remove duplicate edges
                writer.write(x)?;
            }
        }

        for writer in writers.into_iter() {
            writer.close()?;
        }

        let writer = File::create(self.output_path.join("arrangement.bin"))
            .expect("error creating metadata file");
        bincode::serialize_into(writer, &matrix).expect("problem serializing matrix arrangement");
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
    node_blocks: u32,
    max_id: u32,
}

impl CompressedTripletsWriter {
    pub fn to_file<P: AsRef<Path>>(path: P, node_blocks: u32) -> Self {
        Self {
            output_path: path.as_ref().to_path_buf(),
            encoded: Vec::new(),
            node_blocks,
            max_id: 0,
        }
    }

    pub fn write(&mut self, (u, v, w): (u32, u32, u32)) {
        self.max_id = std::cmp::max(self.max_id, std::cmp::max(u, v));
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

        let matrix = Matrix::new(self.node_blocks, self.max_id + 1);

        let mut writers = Vec::new();
        for part_id in 0..(self.node_blocks * self.node_blocks) {
            let p = get_path(part_id);
            println!("opening {:?}", p);
            let writer = BufWriter::new(File::create(p)?);
            let weights_writer =
                BitWriter::<_, BE>::new(BufWriter::new(File::create(get_path_weights(part_id))?));
            let writer = stream::DifferenceStreamWriter::new(writer);
            writers.push((writer, weights_writer));
        }
        for &(x, w) in self.encoded.iter() {
            let (writer, weights_writer) =
                &mut writers[matrix.row_major_block(morton::zorder_to_pair(x)) as usize];
            if writer.is_new_elem(x) {
                // Remove duplicate edges
                writer.write(x)?;
                weights_writer.write(32, w);
            }
        }

        for (writer, weights_writer) in writers.into_iter() {
            writer.close()?;
            weights_writer.into_writer().flush()?;
        }
        let writer = File::create(self.output_path.join("arrangement.bin"))
            .expect("error creating metadata file");
        bincode::serialize_into(writer, &matrix).expect("problem serializing matrix arrangement");

        Ok(())
    }
}

impl Drop for CompressedTripletsWriter {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}
