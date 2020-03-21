extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate base64;
extern crate differential_dataflow;
extern crate dirs;
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
mod min_sum;
mod operators;
mod rand_cluster;
mod sequential;

use argh::FromArgs;
use datasets::*;
use delta_stepping::*;

use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::process::Command;
use timely::communication::{Allocator, Configuration as TimelyConfig, WorkerGuards};
use timely::dataflow::operators::probe::Handle;

use timely::dataflow::operators::Input as TimelyInput;
use timely::dataflow::operators::Inspect;
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
        if let Some(captures) = re_sequential.captures(value) {
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
struct Config {
    #[argh(option, description = "number of threads per process")]
    threads: Option<usize>,
    #[argh(
        option,
        description = "hosts: either a file or a comma separated list of strings",
        from_str_fn(parse_hosts)
    )]
    hosts: Option<Hosts>,
    #[argh(option, description = "set automatically. Don't set manually")]
    process_id: Option<usize>,
    #[argh(
        positional,
        description = "algortihm to use",
        from_str_fn(parse_algorithm)
    )]
    algorithm: Algorithm,
    #[argh(positional, description = "dataset to use")]
    dataset: String,
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
            let host = Host::try_from(line.as_str())?;
            hosts.push(host);
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

    fn execute<T, F>(&self, func: F) -> Result<WorkerGuards<T>, ExecError>
    where
        T: Send + 'static,
        F: Fn(&mut Worker<Allocator>) -> T + Send + Sync + 'static,
    {
        if self.hosts.is_some() && self.process_id.is_none() {
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
                        .arg("diameter-flow")
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

    let mut datasets = map! {
        "cnr-2000" => Dataset::webgraph("cnr-2000"),
        "it-2004" => Dataset::webgraph("it-2004"),
        "sk-2005" => Dataset::webgraph("sk-2005"),
        "uk-2014-tpd" => Dataset::webgraph("uk-2014-tpd"),
        "uk-2014-host" => Dataset::webgraph("uk-2014-host"),
        "uk-2007-05-small" => Dataset::webgraph("uk-2007-05@100000"),
        "facebook" => Dataset::snap("https://snap.stanford.edu/data/facebook_combined.txt.gz"),
        "twitter" => Dataset::snap("https://snap.stanford.edu/data/twitter_combined.txt.gz"),
        "livejournal" => Dataset::snap("https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz"),
        "colorado" => Dataset::dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.COL.gr.gz"),
        "USA" => Dataset::dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.USA.gr.gz"),
        "USA-east" => Dataset::dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.E.gr.gz"),
        "rome" => Dataset::dimacs("http://users.diag.uniroma1.it/challenge9/data/rome/rome99.gr"),
        "ny" => Dataset::dimacs("http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.NY.gr.gz")
    };

    let dataset = datasets
        .remove(&config.dataset) // And not `get`, so we get ownership
        .expect("missing dataset in configuration");
    let meta = dataset.metadata();
    let n = meta.num_nodes;
    println!("Input graph stats: {:?}", meta);

    let timer = std::time::Instant::now();
    let algorithm = config.algorithm;

    if algorithm.is_sequential() {
        let edges = dataset.as_vec();
        println!("Graph loaded in {:?}", timer.elapsed());
        let timer = std::time::Instant::now();
        let (diam, _) = sequential::approx_diameter(edges, n);
        println!("Diameter {}, computed in {:?}", diam, timer.elapsed());
    } else {
        let ret_status = config.execute(move |worker| {
            let (logging_probe, logging_input_handle) = logging::init_count_logging(worker);

            let static_edges = dataset.load_static(worker);

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
                    .probe_with(&mut probe);

                probe
            });

            worker.step_while(|| {
                // probe.with_frontier(|f| println!("frontier {:?}", f.to_vec()));
                !probe.done()
            });
            println!("{:?}\tcomputed diameter", timer.elapsed());

            // close the logging input and perform any outstanding work
            logging_input_handle
                .replace(None)
                .expect("missing logging input handle")
                .close();
            worker.step_while(|| {
                // logging_probe.with_frontier(|f| println!("logging vfrontier {:?}", f.to_vec()));
                logging_probe.done()
            })
        });
        match ret_status {
            Ok(_) => println!(""),
            Err(ExecError::RemoteExecution) => println!(""),
            Err(e) => panic!("{:?}", e),
        }
    }
}
