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

mod datasets;
mod delta_stepping;
mod min_sum;

use datasets::*;
use delta_stepping::*;
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
    datasets.insert(
        "rome".to_owned(),
        Dataset::Dimacs("http://users.diag.uniroma1.it/challenge9/data/rome/rome99.gr".to_owned()),
    );
    datasets.insert(
        "ny".to_owned(),
        Dataset::Dimacs(
            "http://users.diag.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.NY.gr.gz"
                .to_owned(),
        ),
    );

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
        .remove(&dataset)
        .expect("missing dataset in configuration");

    let timer = std::time::Instant::now();

    timely::execute_from_args(std::env::args(), move |worker| {
        let (mut roots, mut edges, probe) = worker.dataflow::<usize, _, _>(move |scope| {
            let mut probe = Handle::new();
            let delta = delta;
            let (edge_input, edges) = scope.new_input::<(u32, u32, u32)>();
            let (root_input, roots) = scope.new_collection::<u32, MinSum>();
            let roots = roots.map(|x| (x, ()));

            // let roots = get_roots(&edges, 1, 112323).map(|x| (x, ()));
            let light = edges.filter(move |trip| trip.2 <= delta);
            let heavy = edges.filter(move |trip| trip.2 > delta);
            light.count().inspect(|c| println!("light {}", c));
            heavy.count().inspect(|c| println!("heavy {}", c));

            let light = light.as_min_sum_collection().arrange_by_key();
            let heavy = heavy.as_min_sum_collection().arrange_by_key();

            delta_step(&roots, &light, &heavy, delta)
                // .inspect_batch(|t, d| println!("[{:?}] The diameter lower bound is {:?}", t, d))
                .probe_with(&mut probe);

            (root_input, edge_input, probe)
        });

        // 8104
        if worker.index() == 0 {
            roots.update(0, MinSum { value: 0 });
            dataset.load_stream(&mut edges);
            println!("{:?}\tread data from file", timer.elapsed());
        }
        roots.close();
        edges.close();
        worker.step_while(|| !probe.done());
        println!("{:?}\tcomputed diameter", timer.elapsed());
    })
    .expect("problems executing the dataflow");
}
