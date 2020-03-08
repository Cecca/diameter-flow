extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
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

use datasets::*;
use delta_stepping::*;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use min_sum::*;
use timely::dataflow::operators::probe::Handle;
// use timely::dataflow::operators::*;
use timely::dataflow::operators::Accumulate;
use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Input as TimelyInput;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::Probe;

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

    if std::env::args().count() == 1 {
        println!("USAGE: diameter-flow DATASET DELTA -w NUM_THREADS");
        println!("Legal values for DATASET are\n");
        for d in datasets.keys() {
            println!("  {}", d);
        }
        return;
    }

    let dataset = std::env::args()
        .nth(1)
        .expect("missing dataset on the command line");
    let delta = std::env::args()
        .nth(2)
        .expect("missing delta on the command line")
        .parse::<u32>()
        .expect("fail to parse delta");
    println!("running on dataset {}", dataset);
    let dataset = datasets
        .remove(&dataset) // And not `get`, so we get ownership
        .expect("missing dataset in configuration");

    let timer = std::time::Instant::now();

    timely::execute_from_args(std::env::args(), move |worker| {
        let (logging_probe, logging_input_handle) = logging::init_count_logging(worker);

        let (mut edges, probe) = worker.dataflow::<usize, _, _>(move |scope| {
            let mut probe = Handle::new();
            let delta = delta;
            let (edge_input, edges) = scope.new_input::<(u32, u32, u32)>();

            // delta_stepping(&edges, delta, 1, 123)
            //     .inspect_batch(|t, d| println!("[{:?}] The diameter lower bound is {:?}", t, d))
            //     .probe_with(&mut probe);
            hyperball::hyperball(&edges, 4, 123)
                .inspect_batch(|t, d| println!("[{:?}] The diameter lower bound is {:?}", t, d))
                .probe_with(&mut probe);

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
