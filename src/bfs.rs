use crate::min_sum::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::*;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand_xoshiro::Xoshiro256StarStar;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Timestamp;

fn get_roots<G: Scope>(
    edges: &Stream<G, (u32, u32)>,
    num_roots: usize,
    seed: u64,
) -> Collection<G, u32, MinSum> {
    edges
        .map(|(src, dst)| std::cmp::max(src, dst))
        .accumulate(0, |max, data| {
            *max = *data.iter().max().expect("empty collection")
        })
        .exchange(|_| 0)
        .accumulate(0, |max, data| {
            *max = *data.iter().max().expect("empty collection")
        })
        .flat_map(move |n| {
            let rng = Xoshiro256StarStar::seed_from_u64(seed);
            let dist = Uniform::new(0, n + 1);
            dist.sample_iter(rng).take(num_roots)
        })
        .unary(Pipeline, "roots builder", |_, _| {
            |input, output| {
                input.for_each(|t, data| {
                    let mut session = output.session(&t);
                    let mut data = data.replace(Vec::new());
                    for node in data.drain(..) {
                        session.give((node, t.time().clone(), MinSum { value: 0 }))
                    }
                });
            }
        })
        .as_collection()
        .inspect(|r| println!("Root node {:?}", r))
}

pub fn bfs<G: Scope>(edges: &Stream<G, (u32, u32)>, num_roots: usize, seed: u64) -> Stream<G, u32>
where
    G::Timestamp: Timestamp + Clone + Lattice,
{
    let roots: Collection<G, (u32, ()), MinSum> =
        get_roots(edges, num_roots, seed).map(|src| (src, ()));

    let edges = edges.as_min_sum_collection().arrange_by_key();

    let distances = roots
        .scope()
        .iterate(|inner| {
            let edges = edges.enter(&inner.scope());
            let roots = roots.enter(&inner.scope());

            edges
                .join_map(&inner, |_src, dst, ()| (*dst, ()))
                .concat(&roots)
                .reduce_core::<_, OrdKeySpine<_, _, _>>(
                    "Reduce",
                    |_node, input, output, updates| {
                        if output.is_empty() || input[0].1 < output[0].1 {
                            updates.push(((), input[0].1));
                        }
                    },
                )
                .as_collection(|k, ()| (*k, ()))
                .inspect(|x| println!("{:?}", x))
        })
        .consolidate()
        .map(|pair| pair.0);

    distances
        .inner
        .map(|triplet| triplet.2.value)
        .accumulate(0, |max, data| {
            *max = *data.iter().max().expect("empty collection")
        })
        .exchange(|_| 0)
        .accumulate(0, |max, data| {
            *max = *data.iter().max().expect("empty collection")
        })
}
