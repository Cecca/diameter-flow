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
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::*;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::*;

#[derive(Abomonation, Copy, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Hash)]
pub struct MinSum {
    value: u32,
}

use differential_dataflow::difference::Semigroup;
use std::ops::{AddAssign, Mul};

impl<'a> AddAssign<&'a Self> for MinSum {
    fn add_assign(&mut self, rhs: &'a Self) {
        self.value = std::cmp::min(self.value, rhs.value);
    }
}

impl Mul<Self> for MinSum {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        MinSum {
            value: self.value + rhs.value,
        }
    }
}

impl Semigroup for MinSum {
    fn is_zero(&self) -> bool {
        false
    }
}

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
            let (root_input, roots) = scope.new_collection::<u32, MinSum>();
            let (edge_input, edges) = scope.new_collection::<(u32, u32), MinSum>();

            let edges = edges.arrange_by_key();
            let nodes = roots.map(|src| (src, ())); // initialize distances at zero

            let distances = nodes
                .scope()
                .iterate(|inner| {
                    let edges = edges.enter(&inner.scope());
                    let nodes = nodes.enter(&inner.scope());

                    edges
                        .join_map(&inner, |_src, dst, ()| (*dst, ()))
                        .concat(&nodes)
                        .reduce_core::<_, OrdKeySpine<_, _, _>>(
                            "Reduce",
                            |_node, input, output, updates| {
                                if output.is_empty() || input[0].1 < output[0].1 {
                                    updates.push(((), input[0].1));
                                }
                            },
                        )
                        .as_collection(|k, ()| (*k, ()))
                })
                .consolidate()
                .map(|pair| pair.0);

            let diameter = distances
                .inner
                .map(|triplet| triplet.2.value)
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

            // edges.update((0, 1), MinSum { value: 4 });
            // edges.update((0, 2), MinSum { value: 4 });
            // edges.update((2, 3), MinSum { value: 4 });
            // edges.update((1, 3), MinSum { value: 4 });
            // edges.update((3, 4), MinSum { value: 4 });
            // edges.update((0, 5), MinSum { value: 1 });
            // edges.update((5, 6), MinSum { value: 1 });
            // edges.update((6, 3), MinSum { value: 1 });

            roots.update(0, MinSum { value: 0 });
        }
        edges.close();
        roots.close();
        worker.step_while(|| !probe.done());
        println!("{:?}\tcomputed diameter", timer.elapsed());
    })
    .expect("problems executing the dataflow");
}
