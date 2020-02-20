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

use datasets::*;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::*;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::*;

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
        let (mut roots, mut edges) = worker.dataflow::<usize, _, _>(|scope| {
            let (root_input, roots) = scope.new_collection::<u32, isize>();
            let (edge_input, edges) = scope.new_collection::<(u32, WeightedEdge), isize>();

            let edges = edges.arrange_by_key();
            let nodes = roots.map(|src| (src, 0)); // initialize distances at zero

            let distances = nodes
                .iterate(|inner| {
                    let edges = edges.enter(&inner.scope());
                    let nodes = nodes.enter(&inner.scope());

                    edges
                        .join_map(&inner, |_src, edge, dist| (edge.dst, dist + edge.weight))
                        .concat(&nodes)
                        .reduce(|_node, input, output| {
                            let res = (*input.iter().map(|p| p.0).min().unwrap(), 1isize);
                            output.push(res);
                        })
                        .consolidate()
                })
                .consolidate();

            let diameter = distances
                .map(|(_node, dist)| dist)
                .inner
                .map(|triplet| triplet.0)
                .accumulate(0, |max, data| {
                    *max = *data.iter().max().expect("empty collection")
                })
                .exchange(|_| 0)
                .accumulate(0, |max, data| {
                    *max = *data.iter().max().expect("empty collection")
                })
                .inspect(|d| println!("Diameter in [{}, {}]", d, 2 * d))
                .probe_with(&mut probe);

            (root_input, edge_input)
        });

        if worker.index() == 0 {
            livejournal.load_dataflow(&mut edges);
            println!("{:?}\tread data from file", timer.elapsed());
            // edges.insert((0, WeightedEdge { dst: 1, weight: 4 }));
            // edges.insert((0, WeightedEdge { dst: 2, weight: 4 }));
            // edges.insert((1, WeightedEdge { dst: 3, weight: 4 }));
            // edges.insert((2, WeightedEdge { dst: 3, weight: 4 }));
            // edges.insert((3, WeightedEdge { dst: 4, weight: 4 }));
            // edges.insert((0, WeightedEdge { dst: 5, weight: 1 }));
            // edges.insert((5, WeightedEdge { dst: 6, weight: 1 }));
            // edges.insert((6, WeightedEdge { dst: 3, weight: 1 }));
            roots.insert(0);
        }
        edges.close();
        roots.close();
        worker.step_while(|| !probe.done());
        println!("{:?}\tcomputed diameter", timer.elapsed());
    })
    .expect("problems executing the dataflow");
}
