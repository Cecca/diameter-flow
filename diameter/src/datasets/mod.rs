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
    pub num_edges: u64,
    pub min_weight: u32,
    pub max_weight: u32,
}

pub struct DatasetBuilder {
    data_dir: PathBuf,
}

impl DatasetBuilder {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    pub fn snap<S: Into<String>>(&self, s: S) -> Dataset {
        Dataset {
            data_dir: self.data_dir.clone(),
            kind: DatasetKind::Snap(s.into()),
        }
    }
    pub fn dimacs<S: Into<String>>(&self, s: S) -> Dataset {
        Dataset {
            data_dir: self.data_dir.clone(),
            kind: DatasetKind::Dimacs(s.into()),
        }
    }
    pub fn webgraph<S: Into<String>>(&self, s: S) -> Dataset {
        Dataset {
            data_dir: self.data_dir.clone(),
            kind: DatasetKind::WebGraph(s.into()),
        }
    }

    pub fn lcc(&self, inner: Dataset) -> Dataset {
        Dataset {
            data_dir: self.data_dir.clone(),
            kind: DatasetKind::LCC(Box::new(inner)),
        }
    }

    pub fn mesh(&self, side: u32) -> Dataset {
        Dataset {
            data_dir: self.data_dir.clone(),
            kind: DatasetKind::Mesh(side),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Dataset {
    data_dir: PathBuf,
    kind: DatasetKind,
}

#[derive(Debug, Clone)]
pub enum DatasetKind {
    Snap(String),
    Dimacs(String),
    WebGraph(String),
    LCC(Box<Dataset>),
    Mesh(u32),
}

impl Dataset {
    pub fn as_vec(&self) -> Vec<((u32, u32), u32)> {
        let mut edges = Vec::new();
        self.for_each(|u, v, w| {
            edges.push(((u, v), w));
        });
        edges
    }

    fn metadata_key(&self) -> String {
        match &self.kind {
            DatasetKind::Dimacs(url) => format!("dimacs::{}", url),
            DatasetKind::Snap(url) => format!("snap::{}", url),
            DatasetKind::WebGraph(name) => format!("webgraph::{}", name),
            DatasetKind::LCC(inner) => format!("lcc::{}", inner.metadata_key()),
            DatasetKind::Mesh(side) => format!("mesh::{}", side),
        }
    }

    fn metadata_map(&self) -> HashMap<String, Metadata> {
        let mut map = HashMap::new();
        let mut metadata_file = self.data_dir.clone();
        metadata_file.push("metadata.bin");
        if metadata_file.exists() {
            let reader = File::open(metadata_file).expect("error opening metadata file");
            let values: Vec<(String, Metadata)> =
                bincode::deserialize_from(reader).expect("problem decoding metadata file. Possibly an old version, try to delete it to force recomputation");
            map.extend(values.into_iter());
        }
        map
    }

    fn update_metadata(&self, new_map: HashMap<String, Metadata>) {
        let mut metadata_file = self.data_dir.clone();
        metadata_file.push("metadata.bin");
        let writer = File::create(metadata_file).expect("error creating metadata file");
        let values: Vec<(String, Metadata)> = new_map.into_iter().collect();
        bincode::serialize_into(writer, &values).expect("problem serializing metadata");
    }

    pub fn metadata(&self) -> Metadata {
        let key = self.metadata_key();
        let mut meta_map = self.metadata_map();
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

            // Add it to the map and update the file
            meta_map.insert(key, meta.clone());
            self.update_metadata(meta_map);

            meta
        }
    }

    pub fn is_prepared(&self) -> bool {
        self.edges_directory().is_dir()
    }

    pub fn clean_edges(&self) {
        std::fs::remove_dir_all(self.edges_directory()).expect("Problem removing edges directory");
    }

    pub fn prepare(&self) {
        if self.is_prepared() {
            return;
        }
        match &self.kind {
            DatasetKind::Dimacs(url) => {
                let edges_dir = self.edges_directory();
                if !edges_dir.is_dir() {
                    println!("Compressing into {:?}", edges_dir);
                    std::fs::create_dir_all(&edges_dir);
                    let mut remapper = Remapper::default();
                    let raw = maybe_download_file(&url, self.dataset_directory());
                    let mut compressor = CompressedTripletsWriter::to_file(edges_dir, 1_000_000);
                    read_dimacs_file(&raw, |(u, v, w)| {
                        let mut src = remapper.remap(u);
                        let mut dst = remapper.remap(v);
                        if src > dst {
                            std::mem::swap(&mut src, &mut dst);
                        }
                        compressor.write((src, dst, w));
                    });
                }
            }
            DatasetKind::Snap(url) => {
                let edges_dir = self.edges_directory();
                if !edges_dir.is_dir() {
                    println!("Compressing into {:?}", edges_dir);
                    std::fs::create_dir_all(&edges_dir);
                    let mut remapper = Remapper::default();
                    let raw = maybe_download_file(&url, self.dataset_directory());
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
            DatasetKind::WebGraph(name) => {
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
                let compressed_path = self.edges_directory();
                if !compressed_path.is_dir() {
                    let timer = std::time::Instant::now();
                    bvconvert::convert(&tool_graph_path, &compressed_path);
                    println!("Compression took {:?}", timer.elapsed());
                }
            }

            DatasetKind::LCC(inner) => {
                inner.prepare();
                let edges_dir = self.edges_directory();
                println!(
                    "Compressing the largest connected component into {:?}",
                    edges_dir
                );
                std::fs::create_dir_all(&edges_dir);
                let inner_meta = inner.metadata();

                let mut uf = UnionFind::new(inner_meta.num_nodes as usize);
                inner.for_each(|x, y, _| {
                    uf.union(x, y);
                });
                let lcc = uf.lcc();

                let mut remapper = Remapper::default();
                if inner_meta.max_weight == 1 {
                    let mut compressor = CompressedPairsWriter::to_file(edges_dir, 1_000_000);
                    inner.for_each(|u, v, _| {
                        if lcc.is_in_lcc(u) {
                            let mut src = remapper.remap(u);
                            let mut dst = remapper.remap(v);
                            if src > dst {
                                std::mem::swap(&mut src, &mut dst);
                            }
                            compressor.write((src, dst));
                        }
                    });
                } else {
                    let mut compressor = CompressedTripletsWriter::to_file(edges_dir, 1_000_000);
                    inner.for_each(|u, v, w| {
                        if lcc.is_in_lcc(u) {
                            let mut src = remapper.remap(u);
                            let mut dst = remapper.remap(v);
                            if src > dst {
                                std::mem::swap(&mut src, &mut dst);
                            }
                            compressor.write((src, dst, w));
                        }
                    });
                }
            }
            DatasetKind::Mesh(side) => {
                let edges_dir = self.edges_directory();
                std::fs::create_dir_all(edges_dir.clone());
                let mut compressor = CompressedPairsWriter::to_file(edges_dir, 1_000_000);
                for i in 0..*side {
                    for j in 0..*side {
                        let node = i * side + j;
                        if i + 1 < *side {
                            let bottom = (i + 1) * side + j;
                            compressor.write((node, bottom));
                        }
                        if j + 1 < *side {
                            let right = i * side + j + 1;
                            compressor.write((node, right));
                        }
                    }
                }
            }
        }
    }

    pub fn for_each<F>(&self, mut action: F)
    where
        F: FnMut(u32, u32, u32),
    {
        let files = self
            .binary_edge_files()
            .map(|triplet| (triplet.1, triplet.2));
        files.for_each(|(pe, pw)| {
            CompressedEdges::from_file(pe, pw)
                .expect("problem creating compressed edges from file")
                .for_each(&mut |u, v, w| action(u, v, w));
        });
        // CompressedEdgesBlockSet::from_files(files)
        //     .expect("error building the edge set")
        //     // .iter()
        //     .for_each(|u, v, w| action(u, v, w));
    }

    pub fn edges_directory(&self) -> PathBuf {
        let mut path = self.dataset_directory();
        path.push("edges");
        path
    }

    pub fn dataset_directory(&self) -> PathBuf {
        use std::fmt::Write;

        let to_hash = match &self.kind {
            DatasetKind::Dimacs(url) => url.clone(),
            DatasetKind::Snap(url) => url.clone(),
            DatasetKind::WebGraph(name) => {
                let graph_url = format!(
                    "http://data.law.di.unimi.it/webdata/{}/{}-hc.graph",
                    name, name
                );
                graph_url
            }
            DatasetKind::LCC(inner) => format!("lcc::{}", inner.metadata_key()),
            DatasetKind::Mesh(side) => format!("mesh::{}", side),
        };

        let mut hasher = Sha256::new();
        hasher.input(to_hash);
        let result = hasher.result();
        let mut hash_string = String::new();
        write!(hash_string, "{:X}", result).expect("problem writing hash string");
        let mut path = self.data_dir.clone();
        path.push(hash_string);
        fs::create_dir_all(&path).expect("problem creating directory");
        path
    }

    fn binary_edge_files(&self) -> impl Iterator<Item = (usize, PathBuf, Option<PathBuf>)> {
        let rex = regex::Regex::new(r"part-(\d+)").expect("error building regex");
        let rex_weights = regex::Regex::new(r"weights-(\d+)").expect("error building regex");
        let edges_directory = self.edges_directory().clone();
        let mut edges_files = HashMap::new();
        let mut weights_files = HashMap::new();
        for entry in std::fs::read_dir(edges_directory).expect("problem reading files") {
            let path = entry.expect("problem getting entry").path();
            let str_name = path
                .file_name()
                .expect("unable to get file name")
                .to_str()
                .expect("unable to convert to string");
            if let Some(captures) = rex.captures(str_name) {
                let digits = captures.get(1).unwrap();
                let chunk_id: usize = digits.as_str().parse().expect("problem parsing");
                edges_files.insert(chunk_id, path);
            } else if let Some(captures) = rex_weights.captures(str_name) {
                let digits = captures.get(1).unwrap();
                let chunk_id: usize = digits.as_str().parse().expect("problem parsing");
                weights_files.insert(chunk_id, path);
            }
        }

        edges_files.into_iter().map(move |(id, edge_path)| {
            if weights_files.len() == 0 {
                (id, edge_path, None)
            } else {
                (
                    id,
                    edge_path,
                    Some(
                        weights_files
                            .remove(&id)
                            .expect("missing weights for chunk"),
                    ),
                )
            }
        })
    }

    /// Sets up a small dataflow to load a static set of edges, distributed among the workers
    pub fn load_static<A: Allocate>(&self, worker: &mut Worker<A>) -> DistributedEdges {
        use timely::dataflow::operators::Input as TimelyInput;

        let (mut input, probe, builder) = worker.dataflow::<usize, _, _>(|scope| {
            let (input, stream) = scope.new_input();

            let (builder, probe) = DistributedEdgesBuilder::new(&stream);

            (input, probe, builder)
        });

        // let mut identifiers: Vec<usize> =
        //     self.binary_edge_files().map(|triplet| triplet.0).collect();
        // identifiers.sort();
        // let mut groups = identifiers.chunks(1 + identifiers.len() / worker.peers());
        // let this_identifiers: &[usize] = groups.nth(worker.index()).unwrap_or_else(|| {
        //     panic!(
        //         "no parts to load for worker {}, possibly there are too few parts",
        //         worker.index()
        //     )
        // });
        // println!(
        //     "worker {} will load parts {:?}",
        //     worker.index(),
        //     this_identifiers
        // );

        println!("loading input");
        self.binary_edge_files()
            .for_each(|(id, path, weights_path)| {
                if id % worker.peers() == worker.index() {
                    // if this_identifiers.contains(&id) {
                    input.send((
                        path.to_str()
                            .expect("couldn't convert path to string")
                            .to_owned(),
                        weights_path.map(|p| {
                            p.to_str()
                                .expect("couldn't convert path to string")
                                .to_owned()
                        }),
                    ));
                }
            });
        input.close();
        worker.step_while(|| !probe.done());
        println!("loaded input {:?}", worker.timer().elapsed());

        builder.build()
    }
}

// pub fn global_dataset_directory() -> PathBuf {
//     let path = std::env::var("GRAPH_DATA_DIR")
//         .map(|p| PathBuf::from(p))
//         .unwrap_or_else(|e| {
//             println!("error getting graph data dir from env: {:?}", e);
//             std::env::home_dir().unwrap().join(".graph-datasets")
//         });
//     println!("Graph data directory is {:?}", path);
//     if !path.exists() {
//         fs::create_dir_all(&path).expect("Problem creating dataset directory");
//     }
//     path
// }

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

struct UnionFindNode {
    parent: u32,
    rank: u32,
    size: u32,
}

struct UnionFind {
    components: Vec<UnionFindNode>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        let mut components = Vec::with_capacity(n);
        for i in 0..n {
            components.push(UnionFindNode {
                parent: i as u32,
                rank: 0,
                size: 1 as u32,
            });
        }
        Self { components }
    }

    fn find(&self, x: u32) -> u32 {
        let mut root = self.components[x as usize].parent;
        while self.components[root as usize].parent != root {
            root = self.components[root as usize].parent;
        }
        root
    }

    fn union(&mut self, x: u32, y: u32) {
        let mut root_x = self.find(x);
        let mut root_y = self.find(y);

        if root_x == root_y {
            return;
        }

        if self.components[root_x as usize].rank < self.components[root_y as usize].rank {
            std::mem::swap(&mut root_x, &mut root_y);
        }

        self.components[root_y as usize].parent = root_x;
        if self.components[root_x as usize].rank == self.components[root_y as usize].rank {
            self.components[root_x as usize].rank += 1;
            self.components[root_x as usize].size += self.components[root_y as usize].size;
        }
    }

    fn lcc(self) -> LargestConnectedComponent {
        let idx = self
            .components
            .iter()
            .enumerate()
            .filter(|(id, ufn)| *id == ufn.parent as usize)
            .max_by_key(|(_, ufn)| ufn.size)
            .unwrap()
            .0 as u32;
        LargestConnectedComponent { idx, uf: self }
    }
}

struct LargestConnectedComponent {
    idx: u32,
    uf: UnionFind,
}

impl LargestConnectedComponent {
    fn is_in_lcc(&self, x: u32) -> bool {
        self.idx == self.uf.find(x)
    }
}
