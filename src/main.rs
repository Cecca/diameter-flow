extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate base64;
extern crate differential_dataflow;
extern crate dirs;
extern crate flate2;
extern crate reqwest;
extern crate sha2;
extern crate tar;
extern crate timely;
extern crate url;

mod datasets;
mod delta_stepping;
mod hyperball;
mod logging;
mod min_sum;

use argh::FromArgs;
use datasets::*;
use delta_stepping::*;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use min_sum::*;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use timely::communication::{
    allocator::AllocateBuilder, initialize_from, Allocator, Configuration as TimelyConfig,
    WorkerGuards,
};
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::Accumulate;
use timely::dataflow::operators::Filter;
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
    #[argh(positional, description = "algortihm to use")]
    algorithm: String,
    #[argh(positional, description = "dataset to use")]
    dataset: String,
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

impl Config {
    fn encode(&self) -> String {
        base64::encode(&bincode::serialize(&self).unwrap())
    }

    fn decode(string: &str) -> Self {
        bincode::deserialize(&base64::decode(string).expect("Error reading base64"))
            .expect("error deserializing the struct")
    }

    fn execute<T, F>(&self, func: F) -> Result<WorkerGuards<T>, String>
    where
        T: Send + 'static,
        F: Fn(&mut Worker<Allocator>) -> T + Send + Sync + 'static,
    {
        if self.hosts.is_some() && self.process_id.is_none() {
            // This is the top level invocation, which should spawn the processes with ssh

            unimplemented!();
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
            timely::execute(c, func)
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
    let config: Config = argh::from_env();

    let mut datasets = map! {
        "cnr-2000" => Dataset::webgraph("cnr-2000"),
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

    let timer = std::time::Instant::now();

    config
        .execute(move |worker| {
            let (logging_probe, logging_input_handle) = logging::init_count_logging(worker);

            let (mut edges, probe) = worker.dataflow::<usize, _, _>(move |scope| {
                let mut probe = Handle::new();
                let delta = 10;
                let (edge_input, edges) = scope.new_input::<(u32, u32, u32)>();

                delta_stepping(&edges, delta, 1, 123)
                    .inspect_batch(|t, d| println!("[{:?}] The diameter lower bound is {:?}", t, d))
                    .probe_with(&mut probe);
                // hyperball::hyperball(&edges, 4, 123)
                //     .inspect_batch(|t, d| println!("[{:?}] The diameter lower bound is {:?}", t, d))
                //     .probe_with(&mut probe);

                (edge_input, probe)
            });

            if worker.index() == 0 {
                dataset.load_stream(&mut edges);
                // edges.send((0, 1, 1));
                // edges.send((0, 2, 2));
                // edges.send((0, 3, 3));
                // edges.send((0, 4, 4));
                // edges.send((1, 5, 1));
                // edges.send((2, 5, 1));
                // edges.send((3, 5, 1));
                // edges.send((4, 5, 1));
                // edges.send((0, 10, 10));
                // edges.send((10, 11, 1));

                println!("{:?}\tread data from file", timer.elapsed());
            }
            edges.close();
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
        })
        .expect("problems executing the dataflow");
    println!("returned from the timely execution");
}
