use bitstream_io::{BitReader, BitWriter};
use std::io::{Read, Write};

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
    fn gamma_code(&mut self, elem: u64) -> Result<(), std::io::Error> {
        let N = 64 - elem.leading_zeros(); // the number of bits to represent `elem`
        for _ in 0..(N - 1) {
            self.inner.write_bit(false)?;
        }
        self.inner.write(N, elem)
    }

    pub fn write(&mut self, element: u64) -> Result<(), std::io::Error> {
        self.gamma_code(element)
    }

    pub fn close(mut self) -> Result<(), std::io::Error> {
        // Write 65 zeros to signal the end of the stream
        // for _ in 0..=65 {
        //     self.inner.write_bit(false)?;
        // }
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

    fn gamma_decode(&mut self) -> Result<u64, std::io::Error> {
        let mut N = 0;
        while !self.inner.read_bit()? {
            N += 1;
            // if N > 64 {
            //     return Ok(None);
            // }
        }
        let elem: u64 = self.inner.read(N)?;
        Ok(elem | (1 << N))
    }

    pub fn read(&mut self) -> Result<u64, std::io::Error> {
        self.gamma_decode()
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
            // let mut read = File::open(&file).unwrap();
            // let mut s = Vec::new();
            // read.read_to_end(&mut s);
            // println!("{:?}", s);
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

    // #[test]
    // fn test_encode_decode_unspecified_length() {
    //     use rand::distributions::Distribution;

    //     let rng = rand::rngs::ThreadRng::default();
    //     let distrib = rand::distributions::Uniform::new(1u64, std::u64::MAX);
    //     let mut values: Vec<u64> = distrib.sample_iter(rng).take(10).collect();
    //     values.sort();

    //     let mut file = temp_dir();
    //     file.push("tmp.bin");

    //     let mut writer = GammaDifferenceStreamWriter::new(File::create(&file).unwrap());
    //     for x in values.iter() {
    //         assert!(writer.write(*x).is_ok());
    //     }
    //     writer.close().unwrap();

    //     let mut reader = GammaDifferenceStreamReader::new(File::open(&file).unwrap());
    //     let mut actual = Vec::new();
    //     while let Some(x) = reader.read().unwrap() {
    //         actual.push(x);
    //     }
    //     assert_eq!(actual, values);
    // }
}
