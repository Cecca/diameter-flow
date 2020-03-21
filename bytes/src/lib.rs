mod hilbert;
mod stream;

use std::io::{Read, Result as IOResult, Write};
use std::path::{Path, PathBuf};

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

    pub fn for_each<F: FnMut(u32, u32, u32)>(&self, mut action: F) -> IOResult<()> {
        use std::io::Cursor;
        let cursor = Cursor::new(&self.raw);
        read_pairs(cursor, move |u, v| action(u, v, 1))
    }
}

pub struct CompressedPairsWriter {
    output_path: PathBuf,
    encoder: hilbert::BytewiseHilbert,
    encoded: Vec<u64>,
}

impl CompressedPairsWriter {
    pub fn to_file<P: AsRef<Path>>(path: P) -> Self {
        Self {
            output_path: path.as_ref().to_path_buf(),
            encoder: hilbert::BytewiseHilbert::new(),
            encoded: Vec::new(),
        }
    }

    pub fn write(&mut self, pair: (u32, u32)) {
        self.encoded.push(self.encoder.entangle(pair));
    }

    fn flush(&mut self) -> IOResult<()> {
        use std::fs::File;
        use std::io::BufWriter;

        self.encoded.sort();
        let writer = BufWriter::new(File::create(self.output_path.clone())?);
        let mut writer = stream::DifferenceStreamWriter::new(writer);
        for &x in self.encoded.iter() {
            writer.write(x)?;
        }
        let out_bits = self.output_path.metadata().unwrap().len() * 8;
        let in_bits = self.encoded.len() * 64;
        println!(
            "Compression ratio is {}, {} bits per edge",
            out_bits as f64 / in_bits as f64,
            out_bits as f64 / self.encoded.len() as f64
        );
        writer.close()?;
        Ok(())
    }
}

impl Drop for CompressedPairsWriter {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

fn read_pairs<R: Read, F: FnMut(u32, u32)>(reader: R, mut action: F) -> IOResult<()> {
    let decoder = hilbert::BytewiseHilbert::new();
    let mut reader = stream::DifferenceStreamReader::new(reader);

    loop {
        let x = reader.read()?;
        if x == 0 {
            return Ok(());
        }
        let (u, v) = decoder.detangle(x);
        action(u, v);
    }
}
