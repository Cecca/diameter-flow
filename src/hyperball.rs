use crate::distributed_graph::*;
use differential_dataflow::difference::Monoid;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::*;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::{AddAssign, Mul};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::Product;
use timely::progress::Timestamp;

#[derive(Clone, PartialOrd, Ord, Eq, PartialEq, Abomonation, Debug, Hash)]
struct HyperLogLogCounter {
    registers: Vec<u8>,
}

impl HyperLogLogCounter {
    fn new<V: Hash + std::fmt::Debug, H: Hasher>(value: &V, mut hasher: H, p: usize) -> Self {
        assert!(p >= 4 && p <= 16);
        let m = 2usize.pow(p as u32);
        let mut registers = vec![0u8; m];

        value.hash(&mut hasher);
        let h: u64 = hasher.finish();

        let mut mask = 0u64;
        for _ in 0..p {
            mask = (mask << 1) | 1;
        }
        let idx = h & mask;
        let w = h >> p;

        registers[idx as usize] = Self::rho(w);

        HyperLogLogCounter { registers }
    }

    fn rho(hash: u64) -> u8 {
        (hash.trailing_zeros() + 1) as u8
    }

    fn merge_inplace(&mut self, other: &Self) {
        let m = self.registers.len();
        for i in 0..m {
            self.registers[i] = std::cmp::max(self.registers[i], other.registers[i]);
        }
    }

    fn merge(&self, other: &Self) -> Self {
        let mut c = self.clone();
        c.merge_inplace(&other);
        c
    }
}

// impl<'a> AddAssign<&'a Self> for HyperLogLogCounter {
//     fn add_assign(&mut self, rhs: &'a Self) {
//         self.merge_inplace(&rhs);
//     }
// }

// impl Mul<Self> for HyperLogLogCounter {
//     type Output = Self;
//     fn mul(self, rhs: Self) -> Self {
//         self.merge(rhs)
//     }
// }

// impl Semigroup for HyperLogLogCounter {
//     fn is_zero(&self) -> bool {
//         false
//     }
// }

fn init_nodes<G: Scope<Timestamp = usize>>(
    edges: &Stream<G, (u32, u32, u32)>,
    p: usize,
    seed: u64,
) -> Collection<G, (u32, HyperLogLogCounter), isize> {
    let worker = edges.scope().index();
    let workers = edges.scope().peers();
    let mut ns = HashMap::new();
    edges
        .map(|(src, dst, _w)| std::cmp::max(src, dst))
        .accumulate(0, |max, data| {
            *max = *data.iter().max().expect("empty collection")
        })
        .exchange(|_| 0)
        .accumulate(0, |max, data| {
            *max = *data.iter().max().expect("empty collection")
        })
        .broadcast()
        .unary_notify(
            Pipeline,
            "nodes initializer",
            None,
            move |input, output, notificator| {
                input.for_each(|t, data| {
                    ns.entry(t.time().clone()).or_insert(data[0]);
                    notificator.notify_at(t.retain());
                });

                notificator.for_each(|time, _, _| {
                    let n = ns.remove(time.time()).unwrap();
                    let n_per_worker = (n as f32 / workers as f32).ceil() as usize;
                    let lower = (worker * n_per_worker) as u32;
                    let upper = std::cmp::min(n, ((worker + 1) * n_per_worker) as u32);

                    let hasher = DefaultHasher::new();
                    let mut session = output.session(&time);
                    for i in lower..upper {
                        let counter = HyperLogLogCounter::new(&i, hasher.clone(), p);
                        session.give(((i, counter), *time.time(), 1));
                    }
                });
            },
        )
        .as_collection()
}

/// The weight will be disregarded
pub fn hyperball_old<G: Scope<Timestamp = usize>>(
    edges: &Stream<G, (u32, u32, u32)>,
    p: usize,
    seed: u64,
) -> Stream<G, u32> {
    let nodes = init_nodes(edges, p, seed);

    let edges = edges
        .map(|(u, v, _)| ((u, v), 0, 1isize)) // FIXME: Should take the actual time
        .as_collection()
        .arrange_by_key();

    nodes
        .scope()
        .iterative::<u64, _, _>(|subscope| {
            let edges = edges.enter(&subscope);

            let unstable = Variable::<_, (u32, HyperLogLogCounter), isize>::new_from(
                nodes.enter(&subscope),
                Product::new(Default::default(), 1u64),
            );

            let (unstable_result, stable_result) = edges
                .join_map(&unstable, |_src, dst, counter| (*dst, counter.clone()))
                .reduce::<_, HyperLogLogCounter, isize>(|_key, input, output| {
                    assert!(output.len() == 0);
                    let mut accum: HyperLogLogCounter = input[0].0.clone();
                    for (counter, _) in input.iter() {
                        accum.merge_inplace(counter);
                    }
                    output.push((accum, 1));
                })
                .join_map(&unstable, |node, old_counter, new_counter| {
                    let counter = new_counter.merge(old_counter);
                    let stabilized = &counter == old_counter;
                    (*node, counter, stabilized)
                })
                .inner
                .branch(|time, ((node, counter, stabilized), _, _)| *stabilized);

            unstable.set(
                &unstable_result
                    .as_collection()
                    .map(|(node, counter, _)| (node, counter))
                    .consolidate(),
            );

            stable_result
                .map(|((node, counter, _), time, delta)| ((node, time.inner as u32), time, delta))
                .as_collection()
                .leave()
        })
        .consolidate()
        .inner
        // .inspect(|x| println!("{:?}", x))
        .map(|((_node, time), _, _)| time)
        .accumulate(0u32, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
        .inspect(|partial| println!("Partial maximum {:?}", partial))
        .exchange(|_| 0)
        .accumulate(0u32, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
}

#[derive(Debug, Clone, Abomonation)]
struct State {
    counter: HyperLogLogCounter,
    updated: bool,
}

impl State {
    fn new(p: usize, id: u32) -> Self {
        Self {
            counter: HyperLogLogCounter::new(&id, DefaultHasher::new(), p),
            updated: true,
        }
    }

    fn update(&self, counter: &HyperLogLogCounter) -> Self {
        let new_counter = self.counter.merge(counter);
        let updated = new_counter != self.counter;
        Self {
            counter: new_counter,
            updated,
        }
    }

    fn deactivate(&self) -> Self {
        Self {
            updated: false,
            ..self.clone()
        }
    }
}

pub fn hyperball<G: Scope<Timestamp = usize>>(
    edges: DistributedEdges,
    scope: &mut G,
    p: usize,
    seed: u64,
) -> Stream<G, u32> {
    // Init nodes
    let nodes = edges
        .nodes::<_, ()>(scope)
        .map(move |(id, ())| (id, State::new(p, id)));

    let stop_times: Stream<G, u32> = nodes.scope().iterative::<u32, _, _>(|subscope| {
        let nodes = nodes.enter(subscope);
        let (handle, cycle) = subscope.feedback(Product::new(Default::default(), 1));

        let (stable, updated) = edges
            .send(
                &nodes.concat(&cycle),
                // Should send?
                |_, _| true,
                // create message
                |_time, state, _weight| Some(state.counter.clone()),
                // Combine messages
                HyperLogLogCounter::merge,
                // Update state for nodes with messages
                |state, message| state.update(message),
                // Update the state for nodes without messages
                |state| state.deactivate(),
            )
            .branch(|t, (_id, state)| state.updated);

        updated.connect_loop(handle);

        let mut counters = HashMap::new();
        // Get the iteration we are returning from, and output just that
        stable
            .unary_notify(
                Pipeline,
                "get_iteration",
                None,
                move |input, output, notificator| {
                    input.for_each(|t, data| {
                        counters
                            .entry(t.time().clone())
                            .and_modify(|c| *c += data.len())
                            .or_insert(data.len());
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        println!(
                            "{:?} are exiting at time {:?}",
                            counters.remove(t.time()),
                            t.time()
                        );
                        output.session(&t).give(t.time().inner);
                    });
                },
            )
            .leave()
    });

    stop_times
        .accumulate(0u32, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
        .inspect(|partial| println!("Partial maximum {:?}", partial))
        .exchange(|_| 0)
        .accumulate(0u32, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
}

#[test]
fn test_rho() {
    assert_eq!(HyperLogLogCounter::rho(1 << 2), 3);
}
