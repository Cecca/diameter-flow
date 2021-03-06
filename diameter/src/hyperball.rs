use crate::distributed_adjacencies::*;
use crate::distributed_graph::*;
use crate::operators::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;

use timely::order::Product;

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

impl Default for State {
    fn default() -> Self {
        panic!("here just to type check, no meaningful instantiation can happen with Default")
    }
}

pub fn hyperball<A: timely::communication::Allocate>(
    adjacencies: DistributedAdjacencies,
    worker: &mut timely::worker::Worker<A>,
    p: usize,
    _seed: u64,
) -> (Option<u32>, std::time::Duration) {
    let (diameter_box, probe) = worker.dataflow::<(), _, _>(|scope| {
        // Init nodes
        let nodes = adjacencies
            .nodes::<_, ()>(scope)
            .map(move |(id, ())| (id, State::new(p, id)));

        // let l1 = nodes.scope().count_logger().expect("missing logger");

        let stop_times = nodes.scope().iterative::<u32, _, _>(|subscope| {
            let nodes = nodes.enter(subscope);
            let (handle, cycle) = subscope.feedback(Product::new(Default::default(), 1));

            let (stable, updated) = adjacencies
                .send(
                    &nodes.concat(&cycle),
                    // .inspect_batch(move |t, data| {
                    //     l1.log((CountEvent::Active(t.inner), data.len() as u64))
                    // }),
                    false,
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
                // .branch(|_t, (_id, state)| state.updated);
                // We have to keep cycling nodes because they may get re-activated
                // at a later iteration
                .branch_all("updated", |_, (_id, state)| state.updated, 0);

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
                            info!(
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
            .inspect(|partial| info!("Partial maximum {:?}", partial))
            .exchange(|_| 0)
            .accumulate(0u32, |max, data| {
                *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
            })
            .collect_single()
    });

    let elapsed = run_to_completion(worker, probe);
    let diameter = diameter_box.borrow_mut().take();

    (diameter, elapsed)
}

#[test]
fn test_rho() {
    assert_eq!(HyperLogLogCounter::rho(1 << 2), 3);
}
