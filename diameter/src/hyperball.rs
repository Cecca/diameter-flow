use crate::distributed_graph::*;
use crate::logging::*;
use crate::operators::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
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

pub fn hyperball<G: Scope<Timestamp = usize>>(
    edges: DistributedEdges,
    scope: &mut G,
    p: usize,
    _seed: u64,
) -> Stream<G, u32> {
    // Init nodes
    let nodes = edges
        .nodes::<_, ()>(scope)
        .map(move |(id, ())| (id, State::new(p, id)));

    let l1 = nodes.scope().count_logger().expect("missing logger");

    let stop_times: Stream<G, u32> = nodes.scope().iterative::<u32, _, _>(|subscope| {
        let nodes = nodes.enter(subscope);
        let (handle, cycle) = subscope.feedback(Product::new(Default::default(), 1));

        let (stable, updated) = edges
            .send(
                &nodes.concat(&cycle).inspect_batch(move |t, data| {
                    l1.log((CountEvent::Active(t.inner), data.len() as u64))
                }),
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
            .branch(|_t, (_id, state)| state.updated);
        // .branch_all(|(_id, state)| state.updated);

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
