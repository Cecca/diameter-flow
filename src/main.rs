extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate differential_dataflow;
extern crate dirs;
extern crate flate2;
extern crate reqwest;
extern crate sha2;
extern crate timely;
extern crate url;

mod bfs;
mod datasets;
mod min_sum;

use bfs::*;
use datasets::*;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::*;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use min_sum::*;
use timely::dataflow::operators::probe::Handle;
// use timely::dataflow::operators::*;
use timely::dataflow::operators::Accumulate;
use timely::dataflow::operators::Exchange;
use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Input as TimelyInput;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::Map;
use timely::dataflow::operators::Probe;

fn main() {
    let mut datasets = std::collections::HashMap::new();
    datasets.insert(
        "facebook".to_owned(),
        Dataset::Snap("https://snap.stanford.edu/data/facebook_combined.txt.gz".to_owned()),
    );
    datasets.insert(
        "twitter".to_owned(),
        Dataset::Snap("https://snap.stanford.edu/data/twitter_combined.txt.gz".to_owned()),
    );
    datasets.insert(
        "livejournal".to_owned(),
        Dataset::Snap("https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz".to_owned()),
    );
    datasets.insert(
        "colorado".to_owned(),
        Dataset::Dimacs(
            "http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.COL.gr.gz"
                .to_owned(),
        ),
    );
    datasets.insert(
        "USA".to_owned(),
        Dataset::Dimacs(
            "http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.USA.gr.gz"
                .to_owned(),
        ),
    );
    datasets.insert(
        "USA-east".to_owned(),
        Dataset::Dimacs(
            "http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.E.gr.gz"
                .to_owned(),
        ),
    );

    let dataset = std::env::args()
        .nth(1)
        .expect("missing dataset on the command line");
    println!("running on dataset {}", dataset);
    let dataset = datasets
        .remove(&dataset)
        .expect("missing dataset in configuration");

    let timer = std::time::Instant::now();

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = Handle::new();
        let mut edges = worker.dataflow::<usize, _, _>(|scope| {
            let (edge_input, edges) = scope.new_input::<(u32, u32, u32)>();

            bfs(&edges, 1, 123)
                .inspect_batch(|t, d| println!("[{:?}] The diameter lower bound is {:?}", t, d))
                .probe_with(&mut probe);

            edge_input
        });

        if worker.index() == 0 {
            dataset.load_stream(&mut edges);
            println!("{:?}\tread data from file", timer.elapsed());
        }
        edges.close();
        worker.step_while(|| !probe.done());
        println!("{:?}\tcomputed diameter", timer.elapsed());
    })
    .expect("problems executing the dataflow");
}
