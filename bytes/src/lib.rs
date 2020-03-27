mod morton;
mod stream;

use bitstream_io::*;
use std::fmt::Debug;
use std::io::{Read, Result as IOResult, Write};
use std::path::{Path, PathBuf};

// Modelled after Rust's HashMap entry
#[derive(Clone)]
pub enum Entry<D: Clone> {
    /// An occupied entry.
    Occupied(D),
    /// A vacant entry.
    Vacant,
}

impl<D: Clone> Entry<D> {
    pub fn is_occupied(&self) -> bool {
        match &self {
            Self::Occupied(_) => true,
            _ => false,
        }
    }

    pub fn and_modify<F: Fn(&mut D)>(&mut self, f: F) -> &mut Self {
        match self {
            Self::Occupied(val) => f(val),
            Self::Vacant => (), //do nothing
        }
        self
    }

    pub fn or_insert(&mut self, d: D) -> &mut Self {
        match self {
            Self::Occupied(val) => (),
            Self::Vacant => *self = Self::Occupied(d),
        }

        self
    }

    pub fn unwrap(self) -> D {
        match self {
            Self::Occupied(v) => v,
            Self::Vacant => panic!("empty entry"),
        }
    }
}

pub struct OffsetArrayMap<D: Clone> {
    offset: usize,
    data: Vec<Entry<D>>,
}

impl<D: Clone + 'static> OffsetArrayMap<D> {
    fn new(offset: u32, len: u32) -> Self {
        Self {
            offset: offset as usize,
            data: vec![Entry::Vacant; len as usize],
        }
    }

    pub fn occupancy(&self) -> f64 {
        let occupied = self.data.iter().filter(|x| !x.is_occupied()).count();
        occupied as f64 / self.allocated_size() as f64
    }

    pub fn allocated_size(&self) -> usize {
        self.data.len()
    }

    pub fn set(&mut self, index: u32, value: D) {
        self.data[index as usize - self.offset] = Entry::Occupied(value);
    }

    pub fn entry<'a>(&'a mut self, index: u32) -> &'a mut Entry<D> {
        &mut self.data[index as usize - self.offset]
    }

    pub fn get<'a>(&'a self, index: u32) -> &'a Entry<D> {
        &self.data[index as usize - self.offset]
    }

    pub fn into_iter(self) -> impl Iterator<Item = (u32, D)> {
        let offset = self.offset;
        self.data
            .into_iter()
            .filter(|e| e.is_occupied())
            .enumerate()
            .map(move |(i, val)| ((i + offset) as u32, val.unwrap()))
    }
}

pub struct CompressedEdgesBlockSet {
    blocks: Vec<CompressedEdges>,
    min_node: u32,
    max_node: u32,
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

        let mut min_node = std::u32::MAX;
        let mut max_node = std::u32::MIN;
        for block in blocks.iter() {
            block.iter().for_each(|(src, dst, _)| {
                min_node = std::cmp::min(src, min_node);
                min_node = std::cmp::min(dst, min_node);
                max_node = std::cmp::max(src, max_node);
                max_node = std::cmp::max(dst, max_node);
            });
        }

        Ok(Self {
            blocks,
            min_node,
            max_node,
        })
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (u32, u32, u32)> + 'a {
        self.blocks.iter().flat_map(|b| b.iter())
    }

    pub fn node_map<D: Clone + 'static>(&self) -> OffsetArrayMap<D> {
        println!(
            "Creating OffsetArrayMap for nodes between {} and {}",
            self.min_node, self.max_node
        );
        OffsetArrayMap::new(self.min_node, self.max_node - self.min_node + 1)
    }

    pub fn for_each<F: FnMut(u32, u32, u32)>(&self, mut action: F) {
        for block in self.blocks.iter() {
            block.for_each(&mut action);
        }
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

    pub fn for_each<F: FnMut(u32, u32, u32)>(&self, action: &mut F) {
        use std::io::Cursor;
        let cursor = Cursor::new(&self.raw);
        let mut reader = stream::DifferenceStreamReader::new(cursor);
        let _weights = self.weights.as_ref().map(|vec| vec.iter());

        if let Some(weights) = self.weights.as_ref() {
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

    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (u32, u32, u32)> + 'a> {
        use std::io::Cursor;
        let cursor = Cursor::new(&self.raw);
        let mut reader = stream::DifferenceStreamReader::new(cursor);
        let _weights = self.weights.as_ref().map(|vec| vec.iter());

        if let Some(weights) = self.weights.as_ref() {
            let mut weights = weights.iter();
            Box::new(std::iter::from_fn(move || {
                let z = reader.read().expect("problem reading form the stream");
                if z == 0 {
                    None
                } else {
                    let (u, v) = morton::zorder_to_pair(z);
                    let w = *weights.next().expect("weights exhausted too soon!");
                    Some((u, v, w))
                }
            }))
        } else {
            Box::new(std::iter::from_fn(move || {
                let z = reader.read().expect("problem reading form the stream");
                if z == 0 {
                    None
                } else {
                    let (u, v) = morton::zorder_to_pair(z);
                    Some((u, v, 1))
                }
            }))
        }
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

#[test]
fn test_entry() {
    let mut arr = OffsetArrayMap::new(4, 10);
    arr.entry(5).or_insert(5u32);
    let actual = arr.get(5).clone();
    assert!(actual.unwrap() == 5);

    arr.entry(5).and_modify(|v| *v = 4);
    let actual = arr.get(5).clone();
    assert!(actual.unwrap() == 4);
}
