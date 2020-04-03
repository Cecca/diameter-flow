extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
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
mod distributed_graph;
mod hyperball;
mod logging;
mod operators;
mod rand_cluster;
mod reporter;
mod sequential;

use argh::FromArgs;
use datasets::*;
use delta_stepping::*;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::rc::Rc;
use std::time::Instant;
use timely::communication::{Allocator, Configuration as TimelyConfig, WorkerGuards};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::Probe;
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
        println!("Rsync from {:?} to {:?}", path_with_slash, path_no_slash);
        let _parent_path_str = path
            .as_ref()
            .parent()
            .expect("missing parent")
            .to_str()
            .expect("problem converting path to string");
        Command::new("rsync")
            .arg("--ignore-existing")
            .arg("-r")
            .arg("--progress")
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
    RandCluster(u32),
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
            Self::RandCluster(_) => "RandCluster".to_owned(),
            Self::Bfs => "Bfs".to_owned(),
        }
    }

    pub fn parameters_string(&self) -> String {
        match self {
            Self::Sequential => "".to_owned(),
            Self::DeltaStepping(delta) => format!("{}", delta),
            Self::HyperBall(p) => format!("{}", p),
            Self::RandCluster(radius) => format!("{}", radius),
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
        let re_rand_cluster = Regex::new(r"rand-cluster\((\d+)\)").unwrap();
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
            return Ok(Self::RandCluster(radius));
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
            let exec = std::env::args().nth(0).unwrap();
            println!("spawning executable {:?}", exec);
            // This is the top level invocation, which should spawn the processes with ssh
            let handles: Vec<std::process::Child> = self
                .hosts
                .as_ref()
                .unwrap()
                .hosts
                .iter()
                .enumerate()
                .map(|(pid, host)| {
                    let encoded_config = self.with_process_id(pid).encode();
                    println!("Connecting to {}", host.name);
                    Command::new("ssh")
                        .arg(&host.name)
                        .arg(&exec)
                        .arg(encoded_config)
                        .spawn()
                        .expect("problem spawning the ssh process")
                })
                .collect();

            for mut h in handles {
                println!("Waiting for ssh process to finish");
                h.wait().expect("problem waiting for the ssh process");
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

fn main() {
    let config = Config::create();

    let builder = DatasetBuilder::new(config.ddir.clone());
    let mut datasets = map! {
        "clueweb12" => builder.webgraph("clueweb12"),
        "gsh-2015" => builder.webgraph("gsh-2015"),
        "cnr-2000" => builder.webgraph("cnr-2000"),
        "it-2004" => builder.webgraph("it-2004"),
        "uk-2005" => builder.webgraph("uk-2005"),
        "sk-2005" => builder.webgraph("sk-2005"),
        "uk-2014-tpd" => builder.webgraph("uk-2014-tpd"),
        "uk-2014-host" => builder.webgraph("uk-2014-host"),
        "uk-2007-05-small" => builder.webgraph("uk-2007-05@100000"),
        "facebook" => builder.snap("https://snap.stanford.edu/data/facebook_combined.txt.gz"),
        "twitter" => builder.snap("https://snap.stanford.edu/data/twitter_combined.txt.gz"),
        "livejournal" => builder.snap("https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz"),
        "colorado" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.COL.gr.gz"),
        "USA" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.USA.gr.gz"),
        "USA-east" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.E.gr.gz"),
        "rome" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/rome/rome99.gr"),
        "ny" => builder.dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.NY.gr.gz")
    };

    let dataset = datasets
        .remove(&config.dataset) // And not `get`, so we get ownership
        .expect("missing dataset in configuration");
    let meta = dataset.metadata();
    let n = meta.num_nodes;
    println!("Input graph stats: {:?}", meta);

    dataset.prepare();
    if config.hosts.is_some() && config.process_id.is_none() {
        println!("Syncing the dataset to the other hosts, if needed");
        config.hosts.as_ref().unwrap().rsync(config.ddir.clone());
    }

    let algorithm = config.algorithm;
    let seed = config.seed();
    let config2 = config.clone();

    if algorithm.is_sequential() {
        let edges = dataset.as_vec();
        let timer = std::time::Instant::now();
        let (diam, _) = sequential::approx_diameter(edges, n)
            .into_iter()
            .max_by_key(|pair| pair.0)
            .unwrap();
        println!("Diameter {}, computed in {:?}", diam, timer.elapsed());
    } else {
        let ret_status = config.execute(move |worker| {
            let reporter = Rc::new(RefCell::new(reporter::Reporter::new(config2.clone())));

            let (logging_probe, logging_input_handle) =
                logging::init_count_logging(worker, Rc::clone(&reporter));

            let static_edges = dataset.load_static(worker);
            let diameter_result = Rc::new(RefCell::new(None));
            let diameter_result_ref = Rc::clone(&diameter_result);

            let probe = worker.dataflow::<usize, _, _>(move |scope| {
                let mut probe = Handle::new();

                let diameter_stream = match algorithm {
                    Algorithm::DeltaStepping(delta) => {
                        delta_stepping(static_edges, scope, delta, n, 1, 123)
                    }
                    Algorithm::HyperBall(p) => hyperball::hyperball(static_edges, scope, p, 123),
                    Algorithm::RandCluster(radius) => {
                        rand_cluster::rand_cluster(static_edges, scope, radius, n, 123)
                    }
                    Algorithm::Bfs => bfs::bfs(static_edges, scope),
                    Algorithm::Sequential => {
                        panic!("sequential algorithm not supported in dataflow")
                    }
                };

                diameter_stream
                    .inspect_batch(|t, d| println!("[{:?}] The diameter lower bound is {:?}", t, d))
                    .probe_with(&mut probe)
                    .sink(Pipeline, "diameter collect", move |input| {
                        input.for_each(|_t, data| {
                            //
                            diameter_result_ref.borrow_mut().replace(data[0]);
                        });
                    });

                probe
            });

            // Run the dataflow and record the time
            let timer = Instant::now();
            worker.step_while(|| !probe.done());
            println!("{:?}\tcomputed diameter", timer.elapsed());
            let elapsed = timer.elapsed();

            // close the logging input and perform any outstanding work
            logging_input_handle
                .replace(None)
                .expect("missing logging input handle")
                .close();
            worker.step_while(|| !logging_probe.done());

            if worker.index() == 0 {
                let diameter: u32 = diameter_result.borrow().expect("missing result");
                reporter.borrow_mut().set_result(diameter, elapsed);
                reporter.borrow().report("reports");
            }
        });
        match ret_status {
            Ok(_) => println!(""),
            Err(ExecError::RemoteExecution) => println!(""),
            Err(e) => panic!("{:?}", e),
        }
    }
}
