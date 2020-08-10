extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
#[macro_use]
extern crate log;
extern crate base64;
extern crate flate2;
extern crate regex;
extern crate reqwest;
extern crate sha2;
extern crate tar;
extern crate timely;
extern crate url;

mod bfs;
mod datasets;
mod delta_stepping;
mod distributed_adjacencies;
mod distributed_graph;
mod hyperball;
mod logging;
mod operators;
mod rand_cluster;
mod reporter;
mod sequential;

use anyhow::Result;
use argh::FromArgs;
use bytes::*;
use datasets::*;
use delta_stepping::*;
use distributed_adjacencies::DistributedAdjacencies;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use timely::communication::{Allocator, Configuration as TimelyConfig, WorkerGuards};

use timely::worker::Worker;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Host {
    name: String,
    port: String,
}

impl Host {
    fn to_string(&self) -> String {
        format!("{}:{}", self.name, self.port)
    }

    fn rsync<P: AsRef<Path>>(&self, path: P) -> Child {
        let path_str = path
            .as_ref()
            .to_str()
            .expect("problem converting path to string");
        let path_with_slash = if path_str.ends_with("/") {
            path_str.to_owned().clone()
        } else {
            let mut p = path_str.clone().to_owned();
            p.push('/');
            p
        };
        let path_no_slash = if path_str.ends_with("/") {
            let mut p = path_str.clone().to_owned();
            p.pop().unwrap();
            p
        } else {
            path_str.to_owned().clone()
        };
        // info!("Rsync from {:?} to {:?}", path_with_slash, path_no_slash);
        Command::new("rsync")
            .arg("--update")
            .arg("-r")
            // .arg("--progress")
            .arg(path_with_slash)
            .arg(format!("{}:{}", self.name, path_no_slash))
            .spawn()
            .expect("error spawning rsync")
    }
}

impl TryFrom<&str> for Host {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut tokens = value.split(":");
        let name = tokens.next().ok_or("missing host part")?.to_owned();
        let port = tokens.next().ok_or("missing port part")?.to_owned();
        Ok(Self { name, port })
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct Hosts {
    hosts: Vec<Host>,
}

impl Hosts {
    fn to_strings(&self) -> Vec<String> {
        self.hosts.iter().map(|h| h.to_string()).collect()
    }

    fn rsync<P: AsRef<Path> + Debug>(&self, path: P) -> () {
        let p = path.as_ref().to_path_buf();
        let procs: Vec<Child> = self.hosts.iter().map(|h| h.rsync(p.clone())).collect();
        procs.into_iter().for_each(|mut p| {
            let status = p.wait().expect("Problem waiting the rsync process");
            assert!(status.success(), "rsync command failed");
        });
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
enum Algorithm {
    Sequential,
    DeltaStepping(u32),
    HyperBall(usize),
    RandCluster(u32, f64),
    /// Parameterized by the maximum size of the auxiliary graph, the initial radius, and the multiplicative step
    RandClusterGuess(u32, u32, u32),
    Bfs,
}

impl Algorithm {
    fn is_sequential(&self) -> bool {
        match self {
            Self::Sequential => true,
            _ => false,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Sequential => "Sequential".to_owned(),
            Self::DeltaStepping(_) => "DeltaStepping".to_owned(),
            Self::HyperBall(_) => "HyperBall".to_owned(),
            Self::RandCluster(_, _) => "RandCluster".to_owned(),
            Self::RandClusterGuess(_, _, _) => "RandClusterGuess".to_owned(),
            Self::Bfs => "Bfs".to_owned(),
        }
    }

    pub fn parameters_string(&self) -> String {
        match self {
            Self::Sequential => "".to_owned(),
            Self::DeltaStepping(delta) => format!("{}", delta),
            Self::HyperBall(p) => format!("{}", p),
            Self::RandCluster(radius, base) => format!("{}:{}", radius, base),
            Self::RandClusterGuess(memory, init, step) => format!("{}:{},{}", memory, init, step),
            Self::Bfs => "".to_owned(),
        }
    }
}

impl TryFrom<&str> for Algorithm {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        use regex::Regex;
        let re_sequential = Regex::new(r"sequential").unwrap();
        let re_delta_stepping = Regex::new(r"delta-stepping\((\d+)\)").unwrap();
        let re_hyperball = Regex::new(r"hyperball\((\d+)\)").unwrap();
        let re_rand_cluster = Regex::new(r"rand-cluster\((\d+), *(\d+)\)").unwrap();
        let re_rand_cluster_guess =
            Regex::new(r"rand-cluster-guess\((\d+), *(\d+), *(\d+)\)").unwrap();
        let re_bfs = Regex::new(r"bfs").unwrap();
        if let Some(_captures) = re_sequential.captures(value) {
            return Ok(Self::Sequential);
        }
        if let Some(captures) = re_delta_stepping.captures(value) {
            let delta = captures
                .get(1)
                .ok_or_else(|| format!("unable to get first capture"))?
                .as_str()
                .parse::<u32>()
                .or_else(|e| Err(format!("error parsing number: {:?}", e)))?;
            return Ok(Self::DeltaStepping(delta));
        }
        if let Some(captures) = re_hyperball.captures(value) {
            let p_str = captures
                .get(1)
                .ok_or_else(|| format!("unable to get first group"))?
                .as_str();
            let p = p_str
                .parse::<usize>()
                .or_else(|e| Err(format!("unable to parse {:?} as integer: {:?}", p_str, e)))?;
            if p < 4 || p > 16 {
                return Err(format!(
                    "Hyperball parameter should be between 4 and 16, got {} instead",
                    p
                ));
            }
            return Ok(Self::HyperBall(p));
        }
        if let Some(captures) = re_rand_cluster.captures(value) {
            let radius = captures
                .get(1)
                .ok_or_else(|| format!("unable to get first capture"))?
                .as_str()
                .parse::<u32>()
                .or_else(|e| Err(format!("error parsing number: {:?}", e)))?;
            let base = captures
                .get(2)
                .ok_or_else(|| format!("unable to get first capture"))?
                .as_str()
                .parse::<f64>()
                .or_else(|e| Err(format!("error parsing number: {:?}", e)))?;
            return Ok(Self::RandCluster(radius, base));
        }
        if let Some(captures) = re_rand_cluster_guess.captures(value) {
            let memory = captures
                .get(1)
                .ok_or_else(|| format!("unable to get first capture"))?
                .as_str()
                .parse::<u32>()
                .or_else(|e| Err(format!("error parsing number: {:?}", e)))?;
            let init = captures
                .get(2)
                .ok_or_else(|| format!("unable to get first capture"))?
                .as_str()
                .parse::<u32>()
                .or_else(|e| Err(format!("error parsing number: {:?}", e)))?;
            let step = captures
                .get(3)
                .ok_or_else(|| format!("unable to get first capture"))?
                .as_str()
                .parse::<u32>()
                .or_else(|e| Err(format!("error parsing number: {:?}", e)))?;
            return Ok(Self::RandClusterGuess(memory, init, step));
        }
        if let Some(_captures) = re_bfs.captures(value) {
            return Ok(Self::Bfs);
        }
        Err(format!("Unrecognized algorithm: {}", value))
    }
}

#[derive(Debug, FromArgs, Serialize, Deserialize, Clone)]
#[argh(description = "")]
pub struct Config {
    #[argh(option, description = "number of threads per process")]
    threads: Option<usize>,
    #[argh(option, description = "random seed for the algorithm")]
    seed: Option<u64>,
    #[argh(
        option,
        description = "hosts: either a file or a comma separated list of strings",
        from_str_fn(parse_hosts)
    )]
    hosts: Option<Hosts>,
    #[argh(option, description = "set automatically. Don't set manually")]
    process_id: Option<usize>,
    #[argh(option, description = "the data directory")]
    ddir: PathBuf,
    #[argh(
        switch,
        description = "rerun the experiment, appending in the database"
    )]
    rerun: bool,
    #[argh(switch, description = "toggle verbose logging", short = 'v')]
    verbose: bool,
    #[argh(switch, description = "keep the datasets on disk")]
    offline: bool,
    #[argh(
        positional,
        description = "algortihm to use",
        from_str_fn(parse_algorithm)
    )]
    algorithm: Algorithm,
    #[argh(positional, description = "dataset to use")]
    dataset: String,
}

impl Config {
    pub fn hosts_string(&self) -> String {
        self.hosts
            .as_ref()
            .map(|hosts| hosts.to_strings().join("__"))
            .unwrap_or(String::new())
    }
}

fn parse_algorithm(arg: &str) -> Result<Algorithm, String> {
    Algorithm::try_from(arg)
}

fn parse_hosts(arg: &str) -> Result<Hosts, String> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;

    let path = PathBuf::from(arg);
    if path.is_file() {
        let f = File::open(path).or(Err("error opening hosts file"))?;
        let reader = BufReader::new(f);
        let mut hosts = Vec::new();
        for line in reader.lines() {
            let line = line.or(Err("error reading line"))?;
            if line.len() > 0 {
                let host = Host::try_from(line.as_str())?;
                hosts.push(host);
            }
        }
        Ok(Hosts { hosts })
    } else {
        let tokens = arg.split(",");
        let mut hosts = Vec::new();
        for token in tokens {
            let host = Host::try_from(token)?;
            hosts.push(host);
        }
        Ok(Hosts { hosts })
    }
}

#[derive(Debug)]
enum ExecError {
    /// Not actually an error
    RemoteExecution,
    /// Actually an error, with message
    Error(String),
}

impl Config {
    fn encode(&self) -> String {
        base64::encode(&bincode::serialize(&self).unwrap())
    }

    fn decode(string: &str) -> Option<Self> {
        let bytes = base64::decode(string).ok()?;
        bincode::deserialize(&bytes).ok()
    }

    fn with_process_id(&self, process_id: usize) -> Self {
        Self {
            process_id: Some(process_id),
            ..self.clone()
        }
    }

    /// If the command line contains a single argument (other than the command name)
    /// tries to decode the first argument into a `Config` struct. If this fails,
    /// proceeds to reading the command line arguments.
    fn create() -> Self {
        match std::env::args().nth(1) {
            Some(arg1) => match Config::decode(&arg1) {
                Some(config) => config,
                None => argh::from_env(),
            },
            None => argh::from_env(),
        }
    }

    fn seed(&self) -> u64 {
        self.seed.unwrap_or(1234u64)
    }

    fn execute<T, F>(&self, func: F) -> Result<WorkerGuards<T>, ExecError>
    where
        T: Send + 'static,
        F: Fn(&mut Worker<Allocator>) -> T + Send + Sync + 'static,
    {
        if self.hosts.is_some() && self.process_id.is_none() {
            let timeout = std::env::var("DIAMETER_TIMEOUT")
                .unwrap_or("3600".to_owned())
                .parse::<u64>()
                .expect("failed to parse the timeout");
            let exec = std::env::args().nth(0).unwrap();
            info!("spawning executable {:?}", exec);
            // This is the top level invocation, which should spawn the processes with ssh
            let mut handles: Vec<std::process::Child> = self
                .hosts
                .as_ref()
                .unwrap()
                .hosts
                .iter()
                .enumerate()
                .map(|(pid, host)| {
                    let encoded_config = self.with_process_id(pid).encode();
                    info!("Connecting to {}", host.name);
                    Command::new("ssh")
                        .arg(&host.name)
                        .arg(&exec)
                        .arg(encoded_config)
                        .spawn()
                        .expect("problem spawning the ssh process")
                })
                .collect();

            info!("Busy wait for ssh process to finish");
            let timer = std::time::Instant::now();
            let timeout = std::time::Duration::from_secs(timeout);
            let step = std::time::Duration::from_secs(1);
            let mut killed = false;
            let mut should_exit = false;
            let terminated = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let terminated2 = std::sync::Arc::clone(&terminated);

            ctrlc::set_handler(move || {
                terminated2.store(true, Ordering::SeqCst);
            })
            .expect("Error setting Ctrl-C handler");

            while !should_exit {
                std::thread::sleep(step);
                let should_kill = timer.elapsed() > timeout;
                if should_kill {
                    warn!("TIMEOUT: Killing subprocesses");
                }
                for h in handles.iter_mut() {
                    match h.try_wait().expect("problem waiting for the ssh process") {
                        Some(status) => {
                            info!("subprocess terminated with {:?}", status);
                            should_exit = true;
                        }
                        None => {
                            if should_kill || terminated.load(Ordering::SeqCst) {
                                info!("Killing subprocess");
                                h.kill().expect("problem killing subprocess");
                                killed = should_kill;
                                should_exit = true;
                            }
                        }
                    }
                }
            }

            if killed {
                let mut rep = reporter::Reporter::new(self.clone());
                rep.killed(timer.elapsed());
                rep.report();
            }

            Err(ExecError::RemoteExecution)
        } else {
            let c = match &self.hosts {
                None => match self.threads {
                    None => TimelyConfig::Thread,
                    Some(threads) => TimelyConfig::Process(threads),
                },
                Some(hosts) => TimelyConfig::Cluster {
                    threads: self.threads.unwrap_or(1),
                    process: self.process_id.expect("missing process id"),
                    addresses: hosts.to_strings(),
                    report: false,
                    log_fn: Box::new(|_| None),
                },
            };
            timely::execute(c, func).or_else(|e| Err(ExecError::Error(e)))
        }
    }
}

macro_rules! map(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($key.to_owned(), $value);
            )+
            m
        }
     };
);

fn list_datasets(datasets: &HashMap<String, Dataset>) {
    let mut table: Vec<(String, Option<u32>, Option<u64>, Option<u32>, Option<u32>)> = datasets
        .iter()
        .map(|(name, dataset)| {
            if dataset.is_prepared() {
                // info!("{} ({:?})", name, dataset.edges_directory());
                let meta = dataset.metadata();
                (
                    name.clone(),
                    Some(meta.num_nodes),
                    Some(meta.num_edges),
                    Some(meta.min_weight),
                    Some(meta.max_weight),
                )
            } else {
                (name.clone(), None, None, None, None)
            }
        })
        .collect();

    table.sort_by_cached_key(|tuple| (tuple.2, tuple.0.clone()));
    println!(
        "{:20}|{:>15}|{:>15}|{:>15}|{:>15}",
        "name", "nodes", "edges", "minimum weight", "maximum weight"
    );
    println!(
        "{}|{}|{}|{}|{}",
        "--------------------",
        "---------------",
        "---------------",
        "---------------",
        "---------------"
    );
    for (name, nodes, edges, minw, maxw) in table.into_iter() {
        println!(
            "{:20}|{:>15}|{:>15}|{:>15}|{:>15}",
            name,
            nodes.map(|n| format!("{}", n)).unwrap_or(String::new()),
            edges.map(|n| format!("{}", n)).unwrap_or(String::new()),
            minw.map(|n| format!("{}", n)).unwrap_or(String::new()),
            maxw.map(|n| format!("{}", n)).unwrap_or(String::new())
        );
    }
}

fn datasets_map(ddir: PathBuf) -> HashMap<String, Dataset> {
    let builder = DatasetBuilder::new(ddir);
    let mut datasets = map! {
        "mesh-10" => builder.mesh(10),
        "mesh-100" => builder.mesh(100),
        "mesh-1000" => builder.mesh(1000),
        "mesh-2048" => builder.mesh(2048),
        "mesh-biw-1000" => builder.mesh_biweight(1000, 0.1, 1_000_000, 1, 4361356),
        "mesh-biw-2048" => builder.mesh_biweight(2048, 0.1, 1_000_000, 1, 4361356),
        "mesh-rw-1000" => builder.mesh_rweight(1000, 1, 1_000_000, 4361356),
        "mesh-rw-2048" => builder.mesh_rweight(2048, 1, 1_000_000, 4361356),
        "clueweb12" => builder.webgraph("clueweb12"),
        "gsh-2015" => builder.webgraph("gsh-2015"),
        "cnr-2000" => builder.webgraph("cnr-2000"),
        "it-2004" => builder.webgraph("it-2004"),
        "uk-2005" => builder.webgraph("uk-2005"),
        "sk-2005" => builder.webgraph("sk-2005"),
        "uk-2014-tpd" => builder.webgraph("uk-2014-tpd"),
        "uk-2014-host" => builder.webgraph("uk-2014-host"),
        "uk-2007-05-small" => builder.webgraph("uk-2007-05@100000"),
        "twitter-2010" => builder.webgraph("twitter-2010"),
        "friendster" => builder.snap("http://snap.stanford.edu/data/bigdata/communities/com-friendster.ungraph.txt.gz"),
        "facebook" => builder.snap("http://snap.stanford.edu/data/facebook_combined.txt.gz"),
        "twitter" => builder.snap("http://snap.stanford.edu/data/twitter_combined.txt.gz"),
        "livejournal" => builder.snap("http://snap.stanford.edu/data/soc-LiveJournal1.txt.gz"),
        "orkut" => builder.snap("http://snap.stanford.edu/data/bigdata/communities/com-orkut.ungraph.txt.gz"),
        "colorado" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.COL.gr.gz"),
        "USA" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.USA.gr.gz"),
        "USA-CTR" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.CTR.gr.gz"),
        "USA-W" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.W.gr.gz"),
        "USA-E" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.E.gr.gz"),
        "rome" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/rome/rome99.gr"),
        "ny" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.NY.gr.gz")
    };

    for (k, v) in datasets.clone().into_iter() {
        datasets.insert(format!("{}-lcc", k), builder.lcc(v));
    }

    datasets.insert(
        "sk-2005-lcc-rweight".to_owned(),
        builder.rweight(13451845, datasets["sk-2005-lcc"].clone()),
    );
    datasets.insert(
        "twitter-2010-lcc-rweight".to_owned(),
        builder.rweight(13451845, builder.lcc(datasets["twitter-2010"].clone())),
    );
    datasets.insert(
        "USA-x5".to_owned(),
        builder.layered(10, datasets["USA"].clone()),
    );
    datasets.insert(
        "USA-x10".to_owned(),
        builder.layered(10, datasets["USA"].clone()),
    );
    datasets
}

fn main() -> Result<()> {
    if let Some("list") = std::env::args().nth(1).as_ref().map(|s| s.as_str()) {
        if let Some(ddir) = std::env::args().nth(2) {
            let ddir = PathBuf::from(ddir);
            let datasets = datasets_map(ddir);
            list_datasets(&datasets);
        } else {
            info!("Specify a directory containing the datasets");
        }
        return Ok(());
    }
    if let Some("clean-edges") = std::env::args().nth(1).as_ref().map(|s| s.as_str()) {
        if let Some(ddir) = std::env::args().nth(2) {
            let ddir = PathBuf::from(ddir);
            let datasets = datasets_map(ddir);
            if let Some(dataset) = std::env::args().nth(3) {
                datasets
                    .get(&dataset)
                    .expect("Missing dataset from configuration!")
                    .clean_edges();
            }
        } else {
            info!("Specify a directory containing the datasets");
        }
        return Ok(());
    }

    let config = Config::create();
    logging::init_logging(config.verbose);
    if let Some(sha) = reporter::Reporter::new(config.clone()).already_run() {
        info!("Parameter configuration already run (sha {}), exiting", sha);
        return Ok(());
    }

    let mut datasets = datasets_map(config.ddir.clone());

    let dataset = datasets
        .remove(&config.dataset) // And not `get`, so we get ownership
        .expect("missing dataset in configuration");
    dataset.prepare();
    let meta = dataset.metadata();
    let n = meta.num_nodes;
    info!("Input graph stats: {:?}", meta);

    if config.hosts.is_some() && config.process_id.is_none() {
        info!("Syncing the dataset to the other hosts, if needed");
        config.hosts.as_ref().unwrap().rsync(config.ddir.clone());
    }

    let algorithm = config.algorithm;
    let seed = config.seed();
    let config2 = config.clone();

    if algorithm.is_sequential() {
        let mut reporter = reporter::Reporter::new(config2.clone());
        let edges = dataset.as_vec();
        let timer = std::time::Instant::now();
        let (eccentricities, diam_elapsed) = sequential::approx_diameter(edges, n);
        let (diam, _) = eccentricities
            .into_iter()
            .max_by_key(|pair| pair.0)
            .unwrap();
        let elapsed = timer.elapsed();
        info!(
            "Diameter {}, computed in {:?} ({:?} with data rearrangement)",
            diam, diam_elapsed, elapsed
        );
        reporter.set_result(diam, diam_elapsed);
        reporter.report();
    } else {
        let ret_status = config.execute(move |worker| {
            let reporter = Rc::new(RefCell::new(reporter::Reporter::new(config2.clone())));

            // let (logging_probe, logging_input_handle) =
            //     logging::init_count_logging(worker, Rc::clone(&reporter));

            let load_type = if config2.offline {
                info!("keeping dataset on disk");
                LoadType::Offline
            } else {
                info!("reading dataset in memory");
                LoadType::InMemory
            };

            // let static_edges = dataset.load_static(worker, load_type);
            let adj_timer = std::time::Instant::now();
            let adjacencies = DistributedAdjacencies::from_edges(
                worker.index() as u32,
                worker.peers() as u32,
                &dataset,
            );
            // Barrier to synchronize on dataset loading
            info!("Waiting for others to load the dataset");
            let (mut input, probe) = worker.dataflow::<(), _, _>(|scope| {
                use timely::dataflow::operators::input::Input;
                use timely::dataflow::operators::*;
                let (input, stream) = scope.new_input::<()>();
                let probe = stream.broadcast().probe();
                (input, probe)
            });
            input.send(());
            input.close();
            worker.step_while(|| !probe.done());

            info!("loaded adjacencies statically ({:?})", adj_timer.elapsed());
            let mut final_approx_probe = None;
            let mut iteration_info = Vec::new();

            let (diameter, elapsed): (Option<u32>, Duration) = match algorithm {
                Algorithm::DeltaStepping(delta) => {
                    delta_stepping(adjacencies, worker, delta, n, seed)
                }
                Algorithm::HyperBall(p) => hyperball::hyperball(adjacencies, worker, p, seed),
                Algorithm::Bfs => bfs::bfs(adjacencies, worker, n, seed),
                Algorithm::RandCluster(radius, base) => rand_cluster::rand_cluster(
                    adjacencies,
                    worker,
                    radius,
                    base,
                    n,
                    seed,
                    &mut final_approx_probe,
                ),
                Algorithm::RandClusterGuess(memory, init, step) => {
                    rand_cluster::rand_cluster_guess(
                        adjacencies,
                        worker,
                        memory,
                        init,
                        step,
                        n,
                        seed,
                        &mut final_approx_probe,
                        &mut iteration_info,
                    )
                }
                Algorithm::Sequential => panic!("sequential algorithm not supported in dataflow"),
            };

            if worker.index() == 0 {
                reporter
                    .borrow_mut()
                    .set_result(diameter.expect("missing diameter"), elapsed);
                if let Some(final_approx_time) = final_approx_probe.take() {
                    reporter
                        .borrow_mut()
                        .set_final_approx_time(final_approx_time);
                }
                for (iteration, (guess_radius, duration, size)) in
                    iteration_info.into_iter().enumerate()
                {
                    info!("    {}: {:?} for {} centers", guess_radius, duration, size);
                    reporter.borrow_mut().append_rand_cluster_iteration(
                        iteration as u32,
                        guess_radius,
                        duration,
                        size,
                    );
                }
                reporter.borrow().report();
            }
        });
        match ret_status {
            Ok(_) => info!(""),
            Err(ExecError::RemoteExecution) => info!(""),
            Err(e) => panic!("{:?}", e),
        }
    }

    Ok(())
}
