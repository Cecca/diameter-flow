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
    let facebook =
        Dataset::Snap("https://snap.stanford.edu/data/facebook_combined.txt.gz".to_owned());
    let twitter =
        Dataset::Snap("https://snap.stanford.edu/data/twitter_combined.txt.gz".to_owned());
    let livejournal =
        Dataset::Snap("https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz".to_owned());

    let timer = std::time::Instant::now();

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = Handle::new();
        let mut edges = worker.dataflow::<usize, _, _>(|scope| {
            let (edge_input, edges) = scope.new_input::<(u32, u32)>();

            let edges = edges.filter(|e| e.0 < 10 && e.1 < 10);

            bfs(&edges, 1, 123)
                .inspect(|d| println!("The diameter range is {:?}", d))
                .probe_with(&mut probe);

            edge_input
        });

        if worker.index() == 0 {
            facebook.load_stream(&mut edges);
            println!("{:?}\tread data from file", timer.elapsed());
        }
        edges.close();
        worker.step_while(|| !probe.done());
        println!("{:?}\tcomputed diameter", timer.elapsed());
    })
    .expect("problems executing the dataflow");
}
