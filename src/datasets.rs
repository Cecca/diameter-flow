use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::InputSession;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::fmt::Write;
use std::fs;
use std::fs::File;
use std::ops::{AddAssign, Mul};
use std::path::PathBuf;
use timely::progress::Timestamp;
use url::Url;

#[derive(Abomonation, Clone, PartialEq, PartialOrd, Debug)]
pub struct WeightedEdge {
    pub dst: u32,
    pub weight: u32,
}

impl Eq for WeightedEdge {}

impl Ord for WeightedEdge {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("problem comparing weights")
    }
}

#[derive(Debug, Deserialize)]
pub enum Dataset {
    Snap(String),
}

impl Dataset {
    pub fn load_dataflow<T: Timestamp + Clone>(
        &self,
        input_handle: &mut InputSession<T, (u32, WeightedEdge), isize>,
    ) {
        match self {
            Self::Snap(url) => {
                read_text_edge_file_unweighted(
                    &maybe_download_and_remap_file(url),
                    |(src, dst)| {
                        //input_handle.update((src, dst), MinSum { value: 1.0 });
                        input_handle.insert((src, WeightedEdge { dst, weight: 1 }));
                        // Also add the flipped edge, to make the graph undirected
                        input_handle.insert((
                            dst,
                            WeightedEdge {
                                dst: src,
                                weight: 1,
                            },
                        ));
                    },
                );
            }
        };
    }

    pub fn num_edges(&self) -> usize {
        match self {
            Self::Snap(url) => {
                let mut cnt = 0;
                read_text_edge_file_unweighted(&maybe_download_file(url), |_| {
                    cnt += 1;
                });
                cnt
            }
        }
    }
}

fn global_dataset_directory() -> PathBuf {
    let mut path = dirs::home_dir().expect("could not find home directory");
    path.push(".graph-datasets");
    if !path.exists() {
        fs::create_dir_all(&path).expect("Problem creating dataset directory");
    }
    path
}

fn dataset_directory(url: &str) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.input(url);
    let result = hasher.result();
    let mut hash_string = String::new();
    write!(hash_string, "{:X}", result).expect("problem writing hash string");
    let mut path = global_dataset_directory();
    path.push(hash_string);
    fs::create_dir_all(&path).expect("problem creating directory");
    path
}

fn dataset_file_path(url: &str) -> PathBuf {
    let parsed = Url::parse(url).expect("not a valid url");
    let basename = parsed
        .path_segments()
        .expect("could not get path segments")
        .last()
        .expect("empty path?");
    let mut dir = dataset_directory(url);
    dir.push(basename);
    dir
}

fn remapped_dataset_file_path(url: &str) -> PathBuf {
    let parsed = Url::parse(url).expect("not a valid url");
    let basename = parsed
        .path_segments()
        .expect("could not get path segments")
        .last()
        .expect("empty path?");
    let mut dir = dataset_directory(url);
    dir.push(basename);
    let dir_clone = dir.clone(); // this is to appease the borrow checker
    let orig_extension = dir_clone
        .extension()
        .expect("missing extension")
        .to_string_lossy();
    dir.set_extension(format!(".remapped.{}", orig_extension));
    dir
}

fn maybe_download_file(url: &str) -> PathBuf {
    let dataset_path = dataset_file_path(url);
    if !dataset_path.exists() {
        println!("Downloading dataset");
        let mut resp = reqwest::get(url).expect("problem while getting the url");
        let mut out = File::create(&dataset_path).expect("failed to create file");
        std::io::copy(&mut resp, &mut out).expect("failed to copy content");
    } else {
        //println!("Dataset {:?} already exists, doing nothing", dataset_path);
    }
    dataset_path
}

fn maybe_download_and_remap_file(url: &str) -> PathBuf {
    let remapped_path = remapped_dataset_file_path(url);
    if !remapped_path.exists() {
        println!("Remapping the file");
        text_edge_file_remap(&maybe_download_file(url), &remapped_path);
    }
    remapped_path
}

fn text_edge_file_remap(input_path: &PathBuf, output_path: &PathBuf) {
    use std::io::{BufWriter, Write};

    let mut cnt = 0;
    let mut node_map = std::collections::HashMap::new();
    let mut output = BufWriter::new(GzEncoder::new(
        File::create(output_path).expect("problem creating output file"),
        Compression::best(),
    ));
    read_text_edge_file_unweighted(input_path, |(src, dst)| {
        let src = *node_map.entry(src).or_insert_with(|| {
            let old = cnt;
            cnt += 1;
            old
        });
        let dst = *node_map.entry(dst).or_insert_with(|| {
            let old = cnt;
            cnt += 1;
            old
        });
        writeln!(output, "{} {}", src, dst).expect("error writing line");
    });
}

fn read_text_edge_file_unweighted<F>(path: &PathBuf, mut action: F)
where
    F: FnMut((u32, u32)),
{
    use std::io::{BufRead, BufReader};
    let reader = BufReader::new(GzDecoder::new(
        File::open(path).expect("problems opening file"),
    ));
    for line in reader.lines() {
        let line = line.expect("error reading line");
        if !line.starts_with("#") {
            let mut tokens = line.split_whitespace();
            let src = tokens
                .next()
                .expect("no source in line")
                .parse::<u32>()
                .expect("could not parse source");
            let dst = tokens
                .next()
                .expect("no destination in line")
                .parse::<u32>()
                .expect("could not parse destination");
            action((src, dst));
        }
    }
}
