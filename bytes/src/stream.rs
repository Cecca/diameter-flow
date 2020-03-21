use bitstream_io::{BitReader, BitWriter};
use std::io::{Read, Result as IOResult, Write};

pub struct DifferenceStreamWriter<W: Write> {
    inner: GammaStreamWriter<W>,
    last: u64,
}

impl<W: Write> DifferenceStreamWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner: GammaStreamWriter::new(inner),
            last: 0,
        }
    }

    #[inline]
    pub fn write(&mut self, elem: u64) -> IOResult<()> {
        assert!(self.last < elem);
        let diff = elem - self.last;
        self.inner.write(diff)
    }

    pub fn close(mut self) -> IOResult<()> {
        self.inner.close()
    }
}

pub struct DifferenceStreamReader<R: Read> {
    inner: GammaStreamReader<R>,
    last: u64,
}

impl<R: Read> DifferenceStreamReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: GammaStreamReader::new(inner),
            last: 0,
        }
    }

    pub fn read(&mut self) -> IOResult<u64> {
        let diff = self.inner.read()?;
        let elem = self.last + diff;
        self.last = elem;
        Ok(elem)
    }
}

pub struct GammaStreamWriter<W: Write> {
    inner: BitWriter<W, bitstream_io::BE>,
}

impl<W: Write> GammaStreamWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner: BitWriter::new(inner),
        }
    }

    #[inline]
    pub fn write(&mut self, elem: u64) -> Result<(), std::io::Error> {
        let N = 64 - elem.leading_zeros(); // the number of bits to represent `elem`
        for _ in 0..(N - 1) {
            self.inner.write_bit(false)?;
        }
        self.inner.write(N, elem)
    }

    pub fn close(mut self) -> Result<(), std::io::Error> {
        // Write 65 zeros to signal the end of the stream
        for _ in 0..=65 {
            self.inner.write_bit(false)?;
        }
        self.inner.byte_align()?;
        self.inner.into_writer().flush()
    }
}

pub struct GammaStreamReader<R: Read> {
    inner: BitReader<R, bitstream_io::BE>,
}

impl<R: Read> GammaStreamReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: BitReader::new(inner),
        }
    }

    pub fn read(&mut self) -> Result<u64, std::io::Error> {
        let mut N = 0;
        while !self.inner.read_bit()? {
            N += 1;
            if N > 64 {
                // 0 is a value out of the domain of possible values,
                // hence we use it to signal the end of the stream.
                return Ok(0);
            }
        }
        let elem: u64 = self.inner.read(N)?;
        Ok(elem | (1 << N))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::env::temp_dir;
    use std::fs::File;

    #[test]
    fn test_encode_decode() {
        for x in 1..100 {
            let mut file = temp_dir();
            file.push("tmp.bin");
            let mut writer = GammaStreamWriter::new(File::create(&file).unwrap());
            assert!(writer.write(x).is_ok());
            writer.close().unwrap();
            let mut reader = GammaStreamReader::new(File::open(&file).unwrap());
            let res = reader.read();
            assert!(res.is_ok(), "error was: {:?}", res.unwrap_err());
            assert_eq!(res.unwrap(), x);
        }
    }

    #[test]
    fn test_many_encode_decode() {
        use rand::distributions::Distribution;

        let rng = rand::rngs::ThreadRng::default();
        let distrib = rand::distributions::Uniform::new(1u64, std::u64::MAX);
        let mut values: Vec<u64> = distrib.sample_iter(rng).take(10).collect();
        values.sort();

        let mut file = temp_dir();
        file.push("tmp.bin");

        let mut writer = GammaStreamWriter::new(File::create(&file).unwrap());
        for x in values.iter() {
            assert!(writer.write(*x).is_ok());
        }
        writer.close().unwrap();

        let mut reader = GammaStreamReader::new(File::open(&file).unwrap());
        for &expected in values.iter() {
            let res = reader.read();
            assert!(res.is_ok(), "error is {:?}", res);
            assert_eq!(res.unwrap(), expected);
        }
    }

    #[test]
    fn test_unspecified_length_encode_decode() {
        use rand::distributions::Distribution;

        let rng = rand::rngs::ThreadRng::default();
        let distrib = rand::distributions::Uniform::new(1u64, std::u64::MAX);
        let mut values: Vec<u64> = distrib.sample_iter(rng).take(10).collect();
        values.sort();

        let mut file = temp_dir();
        file.push("tmp.bin");

        let mut writer = GammaStreamWriter::new(File::create(&file).unwrap());
        for x in values.iter() {
            assert!(writer.write(*x).is_ok());
        }
        writer.close().unwrap();

        let mut reader = GammaStreamReader::new(File::open(&file).unwrap());
        let mut actual = Vec::new();
        loop {
            let x = reader.read().unwrap();
            if x == 0 {
                // end of the stream
                break;
            }
            actual.push(x);
        }
        assert_eq!(actual, values);
    }
}
