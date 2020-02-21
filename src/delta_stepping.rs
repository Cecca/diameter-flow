use crate::min_sum::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::*;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::*;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand_xoshiro::Xoshiro256StarStar;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::Product;
use timely::progress::timestamp::Refines;
use timely::progress::PathSummary;
use timely::progress::Timestamp;

pub fn get_roots<G: Scope<Timestamp = usize>>(
    edges: &Stream<G, (u32, u32, u32)>,
    num_roots: usize,
    seed: u64,
) -> Collection<G, u32, MinSum> {
    edges
        .map(|(src, dst, _w)| std::cmp::max(src, dst))
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
                    let mut data = data.replace(Vec::new());
                    for (i, node) in data.drain(..).enumerate() {
                        let new_time = t.time().clone() + i;
                        let new_cap = t.delayed(&new_time);
                        let mut session = output.session(&new_cap);
                        session.give((node, new_time, MinSum { value: 0 }))
                    }
                });
            }
        })
        .as_collection()
}

// pub fn bfs<G: Scope<timestamp = usize>>(
//     edges: &Stream<G, (u32, u32, u32)>,
//     num_roots: usize,
//     seed: u64,
// ) -> Stream<G, u32>
// where
//     G::Timestamp: Timestamp + Clone + Lattice,
// {
//     let roots: Collection<G, (u32, ()), MinSum> =
//         get_roots(edges, num_roots, seed).map(|src| (src, ()));

//     let edges = edges.as_min_sum_collection().arrange_by_key();

//     let distances = roots
//         .scope()
//         .iterate(|inner| {
//             let edges = edges.enter(&inner.scope());
//             let roots = roots.enter(&inner.scope());

//             edges
//                 .join_map(&roots, |_src, dst, ()| (*dst, ()))
//                 .concat(&roots)
//                 .reduce_core::<_, OrdKeySpine<_, _, _>>(
//                     "Reduce",
//                     |_node, input, output, updates| {
//                         if output.is_empty() || input[0].1 < output[0].1 {
//                             updates.push(((), input[0].1));
//                         }
//                     },
//                 )
//                 .as_collection(|k, ()| (*k, ()))
//         })
//         .consolidate()
//         .map(|pair| pair.0);

//     distances
//         .inner
//         .map(|triplet| triplet.2.value)
//         .accumulate(0, |max, data| {
//             *max = *data.iter().max().expect("empty collection")
//         })
//         .exchange(|_| 0)
//         .accumulate(0, |max, data| {
//             *max = *data.iter().max().expect("empty collection")
//         })
// }

fn filter_active_2<G: Scope<Timestamp = Product<Product<usize, u64>, u64>>>(
    coll: &Collection<G, (u32, ()), MinSum>,
    delta: u32,
) -> Collection<G, (u32, ()), MinSum> {
    coll.inner
        .filter(move |(_, time, dist)| dist.value < (time.outer.inner as u32 + 1) * delta)
        .as_collection()
}

#[allow(dead_code)]
pub fn delta_step<G, Tr>(
    nodes: &Collection<G, (u32, ()), MinSum>,
    light_edges: &Arranged<G, Tr>,
    heavy_edges: &Arranged<G, Tr>,
    delta: u32,
) -> Collection<G, (u32, ()), MinSum>
where
    G: Scope<Timestamp = Product<usize, u64>>,
    Tr: TraceReader<Key = u32, Val = u32, Time = G::Timestamp, R = MinSum> + Clone + 'static,
    Tr::Batch: BatchReader<u32, u32, G::Timestamp, MinSum>,
    Tr::Cursor: Cursor<u32, u32, G::Timestamp, MinSum>,
{
    // Iteratively perform the light updates
    let lightly_updated = nodes.scope().iterate(|inner| {
        let edges = light_edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner
            .inner
            .count()
            .inspect_batch(|t, c| println!("[{:?}] iteration input {:?}", t, c));

        // let active = inner.filter_active(delta);
        let active = filter_active_2(&inner, delta);
        active
            .inner
            .count()
            .inspect_batch(|t, c| println!("[{:?}] active nodes {:?}", t, c));

        edges
            .join_map(&active, |_src, dst, ()| (*dst, ()))
            .concat(&nodes)
            .reduce_core::<_, OrdKeySpine<_, _, _>>("Reduce", |_node, input, output, updates| {
                if output.is_empty() || input[0].1 < output[0].1 {
                    updates.push(((), input[0].1));
                }
            })
            .as_collection(|k, ()| (*k, ()))
            .consolidate()
            .inspect_batch(|t, x| println!("[{:?}] light {:?}", t, x.len()))
    });

    // perform the heavy updates
    heavy_edges
        .join_map(&lightly_updated, |_src, dst, ()| (*dst, ()))
        .concat(&lightly_updated)
        .reduce_core::<_, OrdKeySpine<_, _, _>>("Reduce", |_node, input, output, updates| {
            if output.is_empty() || input[0].1 < output[0].1 {
                updates.push(((), input[0].1));
            }
        })
        .as_collection(|k, ()| (*k, ()))
        .inspect_batch(|t, x| println!("[{:?}] heavy {:?}", t, x.len()))
}

pub fn delta_stepping<G: Scope<Timestamp = usize>>(
    edges: &Stream<G, (u32, u32, u32)>,
    delta: u32,
    num_roots: usize,
    seed: u64,
) -> Stream<G, u32> {
    // Separate light and heavy edges, and arrage them
    let light = edges.filter(move |trip| trip.2 <= delta);
    let heavy = edges.filter(move |trip| trip.2 > delta);
    light.count().inspect(|c| println!("light {}", c));
    heavy.count().inspect(|c| println!("heavy {}", c));
    let light = light.as_min_sum_collection().arrange_by_key();
    let heavy = heavy.as_min_sum_collection().arrange_by_key();

    // Get the random roots
    let roots = get_roots(&edges, num_roots, seed).map(|x| (x, ()));

    let distances = roots
        .scope()
        .iterate(|inner| {
            let light = light.enter(&inner.scope());
            let heavy = heavy.enter(&inner.scope());
            let roots = roots.enter(&inner.scope());

            delta_step(&inner, &light, &heavy, delta)
                .concat(&roots)
                .inspect_batch(|t, k| println!("Inner batch at time {:?}", t))
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
