use crate::distributed_graph::*;

use bytes::*;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use timely::communication::Allocate;
use timely::worker::Worker;
use url::Url;

mod bvconvert;

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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    pub num_nodes: u32,
    pub num_edges: u32,
    pub min_weight: u32,
    pub max_weight: u32,
}

#[derive(Debug)]
pub enum Dataset {
    Snap(String),
    Dimacs(String),
    WebGraph(String),
}

impl Dataset {
    pub fn snap<S: Into<String>>(s: S) -> Self {
        Self::Snap(s.into())
    }
    pub fn dimacs<S: Into<String>>(s: S) -> Self {
        Self::Dimacs(s.into())
    }
    pub fn webgraph<S: Into<String>>(s: S) -> Self {
        Self::WebGraph(s.into())
    }

    pub fn as_vec(&self) -> Vec<((u32, u32), u32)> {
        let mut edges = Vec::new();
        self.for_each(|u, v, w| {
            edges.push(((u, v), w));
        });
        edges
    }

    fn metadata_key(&self) -> String {
        match self {
            Self::Dimacs(url) => format!("dimacs::{}", url),
            Self::Snap(url) => format!("snap::{}", url),
            Self::WebGraph(name) => format!("webgraph::{}", name),
        }
    }

    fn metadata_map() -> HashMap<String, Metadata> {
        let mut map = HashMap::new();
        let mut metadata_file = global_dataset_directory();
        metadata_file.push("metadata.bin");
        if metadata_file.exists() {
            let reader = File::open(metadata_file).expect("error opening metadata file");
            let values: Vec<(String, Metadata)> =
                bincode::deserialize_from(reader).expect("problem decoding metadata file. Possibly an old version, try to delete it to force recomputation");
            map.extend(values.into_iter());
        }
        map
    }

    fn update_metadata(new_map: HashMap<String, Metadata>) {
        let mut metadata_file = global_dataset_directory();
        metadata_file.push("metadata.bin");
        let writer = File::create(metadata_file).expect("error creating metadata file");
        let values: Vec<(String, Metadata)> = new_map.into_iter().collect();
        bincode::serialize_into(writer, &values).expect("problem serializing metadata");
    }

    pub fn metadata(&self) -> Metadata {
        let key = self.metadata_key();
        let mut meta_map = Self::metadata_map();
        if meta_map.contains_key(&key) {
            meta_map.get(&key).unwrap().clone()
        } else {
            println!("Missing metadata computing it");
            let mut num_nodes = 0;
            let mut num_edges = 0;
            let mut min_weight = std::u32::MAX;
            let mut max_weight = 0;
            self.for_each(|u, v, w| {
                num_nodes = std::cmp::max(num_nodes, std::cmp::max(u, v));
                num_edges += 1;
                min_weight = std::cmp::min(min_weight, w);
                max_weight = std::cmp::max(max_weight, w);
            });
            num_nodes += 1;
            let meta = Metadata {
                num_edges,
                num_nodes,
                min_weight,
                max_weight,
            };
            println!("{:?}", meta);

            // Add it to the map and update the file
            meta_map.insert(key, meta.clone());
            Self::update_metadata(meta_map);

            meta
        }
    }

    pub fn prepare(&self) {
        match self {
            Self::Dimacs(url) => {
                // TODO: Write also the weights!!
                let edges_dir = self.edges_directory();
                if !edges_dir.is_dir() {
                    println!("Compressing into {:?}", edges_dir);
                    std::fs::create_dir_all(&edges_dir);
                    let mut remapper = Remapper::default();
                    let raw = maybe_download_file(url, self.dataset_directory());
                    let mut compressor = CompressedPairsWriter::to_file(edges_dir, 1_000_000);
                    read_dimacs_file(&raw, |(u, v, w)| {
                        let mut src = remapper.remap(u);
                        let mut dst = remapper.remap(v);
                        if src > dst {
                            std::mem::swap(&mut src, &mut dst);
                        }
                        compressor.write((src, dst));
                    });
                }
            }
            Self::Snap(url) => {
                let edges_dir = self.edges_directory();
                if !edges_dir.is_dir() {
                    println!("Compressing into {:?}", edges_dir);
                    std::fs::create_dir_all(&edges_dir);
                    let mut remapper = Remapper::default();
                    let raw = maybe_download_file(url, self.dataset_directory());
                    let mut compressor = CompressedPairsWriter::to_file(edges_dir, 1_000_000);
                    read_text_edge_file_unweighted(&raw, |(u, v)| {
                        let mut src = remapper.remap(u);
                        let mut dst = remapper.remap(v);
                        if src > dst {
                            std::mem::swap(&mut src, &mut dst);
                        }
                        compressor.write((src, dst));
                    });
                }
            }
            Self::WebGraph(name) => {
                let dir = self.dataset_directory();
                println!("Destination directory is {:?}", dir);
                let graph_url = format!(
                    "http://data.law.di.unimi.it/webdata/{}/{}-hc.graph",
                    name, name
                );
                let properties_url = format!(
                    "http://data.law.di.unimi.it/webdata/{}/{}-hc.properties",
                    name, name
                );
                let graph_fname = format!("{}-hc.graph", name);
                let properties_fname = format!("{}-hc.properties", name);

                let mut graph_path = dir.clone();
                let mut properties_path = dir.clone();
                graph_path.push(graph_fname);
                properties_path.push(properties_fname);
                let mut tool_graph_path = dir.clone();
                tool_graph_path.push(format!("{}-hc", name));

                // Download the files
                bvconvert::maybe_download_file(&graph_url, graph_path);
                bvconvert::maybe_download_file(&properties_url, properties_path);

                // Convert the file
                let mut compressed_path = self.edges_directory();
                if !compressed_path.is_dir() {
                    let timer = std::time::Instant::now();
                    bvconvert::convert(&tool_graph_path, &compressed_path);
                    println!("Compression took {:?}", timer.elapsed());
                }
            }
        }
    }

    pub fn for_each<F>(&self, mut action: F)
    where
        F: FnMut(u32, u32, u32),
    {
        match self {
            Self::Dimacs(url) => {
                self.prepare();
                read_text_edge_file_weighted(
                    &remapped_dataset_file_path(url, self.dataset_directory()),
                    |(src, dst, w)| {
                        action(src, dst, w);
                    },
                );
            }
            Self::Snap(url) => {
                self.prepare();
                read_text_edge_file_unweighted(
                    &remapped_dataset_file_path(url, self.dataset_directory()),
                    |(src, dst)| {
                        action(src, dst, 1);
                    },
                );
            }
            Self::WebGraph(name) => {
                self.prepare();
                let mut edges_path = self.dataset_directory();
                edges_path.push("edges");
                let edges = CompressedEdgesBlockSet::from_dir(edges_path, |_| true)
                    .expect("error loading compressed edges");
                let timer = std::time::Instant::now();
                let mut cnt = 0;
                for (u, v, w) in edges.iter() {
                    cnt += 1;
                    action(u, v, w);
                }
                println!(
                    "Iterating over the {} z-order compressed edges took {:?}",
                    cnt,
                    timer.elapsed()
                );
            }
        };
    }

    fn edges_directory(&self) -> PathBuf {
        let mut path = self.dataset_directory();
        path.push("edges");
        path
    }

    pub fn dataset_directory(&self) -> PathBuf {
        use std::fmt::Write;

        let to_hash = match self {
            Self::Dimacs(url) => url.clone(),
            Self::Snap(url) => url.clone(),
            Self::WebGraph(name) => {
                let graph_url = format!(
                    "http://data.law.di.unimi.it/webdata/{}/{}-hc.graph",
                    name, name
                );
                graph_url
            }
        };

        let mut hasher = Sha256::new();
        hasher.input(to_hash);
        let result = hasher.result();
        let mut hash_string = String::new();
        write!(hash_string, "{:X}", result).expect("problem writing hash string");
        let mut path = global_dataset_directory();
        path.push(hash_string);
        fs::create_dir_all(&path).expect("problem creating directory");
        path
    }

    fn binary_edge_files(&self) -> impl Iterator<Item = (usize, PathBuf)> {
        let rex = regex::Regex::new(r"\d+").expect("error building regex");
        let mut edges_directory = self.edges_directory();
        std::fs::read_dir(edges_directory)
            .expect("problem reading directory")
            .flat_map(move |entry| {
                // let entry = entry?;
                let path = entry.expect("problem getting entry").path();
                if let Some(digits) = rex.find(
                    path.file_name()
                        .expect("unable to get file name")
                        .to_str()
                        .expect("unable to convert to string"),
                ) {
                    let chunk_id: usize = digits.as_str().parse().expect("problem parsing");
                    Some((chunk_id, path))
                } else {
                    None
                }
            })
    }

    /// Sets up a small dataflow to load a static set of edges, distributed among the workers
    pub fn load_static<A: Allocate>(&self, worker: &mut Worker<A>) -> DistributedEdges {
        use timely::dataflow::operators::Input as TimelyInput;

        if worker.index() == 0 {
            // TODO: Find a way of distributing work on the cluster
            self.prepare();
        }

        let (mut input, probe, builder) = worker.dataflow::<usize, _, _>(|scope| {
            let (input, stream) = scope.new_input();

            let (builder, probe) = DistributedEdgesBuilder::new(&stream);

            (input, probe, builder)
        });

        println!("loading input");
        self.binary_edge_files().for_each(|(id, path)| {
            if id % worker.peers() == worker.index() {
                input.send(
                    path.to_str()
                        .expect("couldn't convert path to string")
                        .to_owned(),
                );
            }
        });
        input.close();
        worker.step_while(|| !probe.done());
        println!("loaded input {:?}", worker.timer().elapsed());

        builder.build()
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
    use std::fmt::Write;
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

fn dataset_file_path(url: &str, mut dir: PathBuf) -> PathBuf {
    let parsed = Url::parse(url).expect("not a valid url");
    let basename = parsed
        .path_segments()
        .expect("could not get path segments")
        .last()
        .expect("empty path?");
    dir.push(basename);
    dir
}

fn remapped_dataset_file_path(url: &str, mut dir: PathBuf) -> PathBuf {
    let parsed = Url::parse(url).expect("not a valid url");
    let basename = parsed
        .path_segments()
        .expect("could not get path segments")
        .last()
        .expect("empty path?");
    dir.push(basename);
    let dir_clone = dir.clone(); // this is to appease the borrow checker
    let orig_extension = dir_clone
        .extension()
        .expect("missing extension")
        .to_string_lossy();
    dir.set_extension(format!(".remapped.{}", orig_extension));
    dir
}

fn maybe_download_file(url: &str, dir: PathBuf) -> PathBuf {
    let dataset_path = dataset_file_path(url, dir);
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

fn maybe_download_and_remap_file(url: &str, dir: PathBuf) -> PathBuf {
    let remapped_path = remapped_dataset_file_path(url, dir.clone());
    if !remapped_path.exists() {
        println!("Remapping the file");
        text_edge_file_remap(&maybe_download_file(url, dir), &remapped_path);
    }
    remapped_path
}

fn maybe_download_and_remap_dimacs_file(url: &str, dir: PathBuf) -> PathBuf {
    let remapped_path = remapped_dataset_file_path(url, dir.clone());
    if !remapped_path.exists() {
        println!("Remapping the file");
        dimacs_file_remap(&maybe_download_file(url, dir), &remapped_path);
    }
    remapped_path
}

#[derive(Default)]
struct Remapper {
    cnt: u32,
    node_map: HashMap<u32, u32>,
}

impl Remapper {
    fn remap(&mut self, node: u32) -> u32 {
        let mut cnt = self.cnt;
        let remapped = *self.node_map.entry(node).or_insert_with(|| {
            let remapped = cnt;
            cnt += 1;
            remapped
        });
        self.cnt = cnt;
        remapped
    }
}

fn dimacs_file_remap(input_path: &PathBuf, output_path: &PathBuf) {
    use std::io::{BufWriter, Write};

    let mut cnt = 0;
    let mut node_map = std::collections::HashMap::new();
    let mut output = BufWriter::new(GzEncoder::new(
        File::create(output_path).expect("problem creating output file"),
        Compression::best(),
    ));
    read_dimacs_file(input_path, |(src, dst, w)| {
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
        writeln!(output, "{} {} {}", src, dst, w).expect("error writing line");
    });
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

fn read_text_edge_file_weighted<F>(path: &PathBuf, mut action: F)
where
    F: FnMut((u32, u32, u32)),
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
            let weight = tokens
                .next()
                .expect("no weight")
                .parse::<u32>()
                .expect("could not parse weight");
            action((src, dst, weight));
        }
    }
}

fn read_dimacs_file<F>(path: &PathBuf, mut action: F)
where
    F: FnMut((u32, u32, u32)),
{
    use std::io::{BufRead, BufReader};
    let reader = BufReader::new(GzDecoder::new(
        File::open(path).expect("problems opening file"),
    ));
    for line in reader.lines() {
        let line = line.expect("error reading line");
        if line.starts_with("a") {
            let mut tokens = line.split_whitespace().skip(1);
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
            let weight = tokens
                .next()
                .expect("no weight")
                .parse::<u32>()
                .expect("could not parse weight");
            action((src, dst, weight));
        }
    }
}
