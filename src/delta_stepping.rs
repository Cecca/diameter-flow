use crate::distributed_graph::*;
use crate::logging::*;
use crate::min_sum::*;
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::iterate::SemigroupVariable;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::*;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::*;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand_xoshiro::Xoshiro256StarStar;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::Product;
use timely::progress::timestamp::Refines;
use timely::progress::PathSummary;
use timely::progress::Timestamp;

trait AggregateMin<G: Scope> {
    fn aggregate_min(&self) -> Collection<G, (u32, ()), MinSum>;
}

impl<G: Scope> AggregateMin<G> for Collection<G, (u32, ()), MinSum>
where
    G::Timestamp: Timestamp + Lattice + Ord,
{
    fn aggregate_min(&self) -> Collection<G, (u32, ()), MinSum> {
        self.reduce_core::<_, OrdKeySpine<_, _, _>>(
            "aggregate_min",
            |_node, input, output, updates| {
                if output.is_empty() || input[0].1 < output[0].1 {
                    updates.push(((), input[0].1));
                }
            },
        )
        .as_collection(|k, ()| (*k, ()))
    }
}

trait PropagateDistances<G: Scope> {
    fn propagate_distances(
        &self,
        nodes: &Collection<G, (u32, ()), MinSum>,
    ) -> Collection<G, (u32, ()), MinSum>;
}

impl<G: Scope> PropagateDistances<G> for Collection<G, (u32, u32), MinSum>
where
    G::Timestamp: Timestamp + Lattice + Ord,
{
    fn propagate_distances(
        &self,
        nodes: &Collection<G, (u32, ()), MinSum>,
    ) -> Collection<G, (u32, ()), MinSum> {
        self.join_map(&nodes, |_src, dst, ()| {
            // println!("    light propagating {} to {}", _src, dst);
            (*dst, ())
        })
    }
}

impl<G: Scope, Tr> PropagateDistances<G> for Arranged<G, Tr>
where
    G::Timestamp: Timestamp + Lattice + Ord,
    Tr: TraceReader<Key = u32, Val = u32, Time = G::Timestamp, R = MinSum> + Clone + 'static,
    Tr::Batch: BatchReader<u32, u32, G::Timestamp, MinSum>,
    Tr::Cursor: Cursor<u32, u32, G::Timestamp, MinSum>,
{
    fn propagate_distances(
        &self,
        nodes: &Collection<G, (u32, ()), MinSum>,
    ) -> Collection<G, (u32, ()), MinSum> {
        self.join_map(&nodes, |_src, dst, ()| {
            // println!("    light propagating {} to {}", _src, dst);
            (*dst, ())
        })
    }
}

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
                        println!("Selected {:?} as center", node);
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

pub fn delta_step_old<G, Tr>(
    initial_input: &Collection<G, (u32, ()), MinSum>,
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
    use crate::logging::CountEvent::*;
    let l1 = initial_input
        .scope()
        .count_logger()
        .expect("missing logger");
    let l2 = l1.clone();
    let l3 = l1.clone();
    let l4 = l1.clone();
    let l5 = l1.clone();
    // Iteratively perform the light updates
    let lightly_updated = initial_input
        .scope()
        .iterative(|subscope| {
            let edges = light_edges.enter(&subscope);
            let initial_input = initial_input.enter(&subscope);

            let delta_step_nodes: SemigroupVariable<_, (u32, ()), MinSum> =
                SemigroupVariable::new(subscope, Product::new(Default::default(), 1));

            // delta_step_nodes.inspect(move |c| println!("   light iteration input {:?}", c));
            delta_step_nodes.inspect_batch(move |t, c| {
                l1.log((LightIterInput(t.outer.inner, t.inner), c.len().into()))
            });

            let light_iteration_output = edges
                .propagate_distances(&delta_step_nodes)
                .concat(&initial_input)
                .aggregate_min()
                .consolidate();

            // The nodes in the collection `_too_far` are output in the
            // last line of this function
            let (further, _too_far) = light_iteration_output.inner.branch(move |time, triplet| {
                triplet.2.value >= (time.outer.inner as u32 + 1) * delta
            });

            delta_step_nodes.set(&further.as_collection());

            light_iteration_output.leave()
        })
        .inspect_batch(move |t, c| l4.log((LightUpdates(t.inner), c.len().into())));

    // perform the heavy updates
    let heavy_updated = heavy_edges
        .propagate_distances(&lightly_updated)
        .aggregate_min()
        // .inspect(move |c| println!("heavy updated {:?}", c))
        .inspect_batch(move |t, c| l3.log((HeavyUpdates(t.inner), c.len().into())));

    lightly_updated
        // .inspect(move |c| println!("lightly updated {:?}", c))
        .concat(&heavy_updated)
        .aggregate_min()
        .inspect_batch(move |t, c| l5.log((IterationOutput(t.inner), c.len().into())))
}

pub fn delta_stepping_old<G: Scope<Timestamp = usize>>(
    edges: &Stream<G, (u32, u32, u32)>,
    delta: u32,
    num_roots: usize,
    seed: u64,
) -> Stream<G, u32> {
    use crate::logging::CountEvent::*;
    let l1 = edges.scope().count_logger().expect("missing logger");
    let l2 = l1.clone();
    let l3 = l1.clone();

    // Separate light and heavy edges, and arrage them
    let light = edges
        .filter(move |trip| trip.2 <= delta)
        .inspect_batch(move |_, x| l1.log((CountEvent::LightEdges, x.len().into())))
        .as_min_sum_collection()
        .arrange_by_key();
    let heavy = edges
        .filter(move |trip| trip.2 > delta)
        .inspect_batch(move |_, x| l2.log((CountEvent::HeavyEdges, x.len().into())))
        .as_min_sum_collection()
        .arrange_by_key();

    // Get the random roots
    let roots = get_roots(&edges, num_roots, seed).map(|x| (x, ()));

    let distances = roots
        .scope()
        .iterative(move |subgraph| {
            let light = light.enter(&subgraph);
            let heavy = heavy.enter(&subgraph);
            let roots = roots.enter(&subgraph);

            let active = SemigroupVariable::new(subgraph, Product::new(Default::default(), 1));
            let later = SemigroupVariable::new(subgraph, Product::new(Default::default(), 1));

            let iteration_input = active.concat(&roots);
            // .inspect(|v| println!("delta step input {:?}", v));

            let iteration_output =
                delta_step_old(&iteration_input, &light, &heavy, delta).consolidate();

            // Split the result of the iteration into three collections:
            //  - nodes at distance less than (buck+1)*delta, which should be output
            //  - nodes at distance between (buck+1)*delta and (buck+2)*delta,
            //    which should be the active set of the next iteration
            //  - the other nodes, whose processing should be postponed to later iterations
            //
            // This setup should also take care of iterations where there are no active nodes:
            // in any case we keep forwarding to the next iteration the nodes that are to be
            // processed later
            let (iteration_result, others) = iteration_output
                .concat(&later)
                .inner
                .branch(move |time, triplet| triplet.2.value >= (time.inner as u32 + 1) * delta);
            let (next_active, process_later) = others
                .branch(move |time, triplet| triplet.2.value >= (time.inner as u32 + 2) * delta);

            // iteration_result.inspect(|v| println!("iteration result {:?}", v));
            // next_active.inspect(|v| println!("next active {:?}", v));
            // process_later.inspect(|v| println!("process later {:?}", v));

            active.set(&next_active.as_collection());
            later.set(&process_later.as_collection());

            // Return the result of the current iteration, without further considering it,
            // along with the initial nodes of the next iteration: their distance is set,
            // so we can safely output them
            iteration_result
                .concat(&next_active)
                .as_collection()
                .leave()
        })
        .consolidate()
        .map(|pair| pair.0);

    distances
        .inner
        .map(|triplet| triplet.2.value)
        // .inspect(|c| println!("{:?}", c))
        .accumulate(0, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
        .inspect(|partial| println!("Partial maximum {:?}", partial))
        .exchange(|_| 0)
        .accumulate(0, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
}

#[derive(Debug, Clone, Abomonation)]
struct State {
    distance: Option<u32>,
    updated: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            distance: None,
            updated: false,
        }
    }
}

impl State {
    fn root() -> Self {
        Self {
            distance: Some(0),
            updated: true,
        }
    }

    fn should_send(&self) -> bool {
        self.updated && self.distance.is_some()
    }

    fn deactivate(&self) -> Self {
        Self {
            updated: false,
            ..self.clone()
        }
    }

    fn reactivate(&self) -> Self {
        if self.distance.is_some() {
            Self {
                updated: true,
                ..self.clone()
            }
        } else {
            self.clone()
        }
    }

    fn update_distance(&self, distance: u32) -> Self {
        if let Some(prev_distance) = self.distance {
            if distance < prev_distance {
                Self {
                    distance: Some(distance),
                    updated: true,
                }
            } else {
                Self {
                    distance: Some(prev_distance),
                    updated: false,
                }
            }
        } else {
            Self {
                distance: Some(distance),
                updated: true,
            }
        }
    }

    fn send_light(step: u32, delta: u32, state: &Self, weight: u32) -> Option<u32> {
        let bucket_limit = delta * (step + 1);
        if weight <= delta && state.distance.expect("missing distance") <= bucket_limit {
            Some(weight + state.distance.expect("missing distance"))
        } else {
            None
        }
    }

    fn send_heavy(step: u32, delta: u32, state: &Self, weight: u32) -> Option<u32> {
        if weight > delta {
            Some(weight + state.distance.expect("missing distance"))
        } else {
            None
        }
    }
}

/// Branch out when the stream of nodes has stabilized, that is,
/// when there are no more updates in the current bucket
fn branch_stabilized<G: Scope<Timestamp = Product<Product<usize, u32>, u32>>>(
    nodes: &Stream<G, (u32, State)>,
    delta: u32,
) -> (Stream<G, (u32, State)>, Stream<G, (u32, State)>) {
    use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
    use timely::dataflow::operators::*;

    let stash = Rc::new(RefCell::new(HashMap::new()));
    let stash2 = Rc::clone(&stash);

    let updated_counts = nodes
        .unary(Pipeline, "count updated", move |_, _| {
            move |input, output| {
                input.for_each(|t, data| {
                    let data = data.replace(Vec::new());
                    output
                        .session(&t)
                        .give(data.iter().filter(|(_, state)| state.updated).count());
                    stash
                        .borrow_mut()
                        .entry(t.time().clone())
                        .or_insert_with(Vec::new)
                        .extend(data.into_iter());
                })
            }
        })
        .broadcast();

    let mut stash_counts = HashMap::new();
    let (stable, some_updated) = updated_counts
        .unary_notify(
            Pipeline,
            "splitter",
            None,
            move |input, output, notificator| {
                input.for_each(|t, data| {
                    let data = data.replace(Vec::new());
                    let total: usize = data.into_iter().sum();
                    stash_counts
                        .entry(t.time().clone())
                        .and_modify(|c| *c += total)
                        .or_insert(total);
                    notificator.notify_at(t.retain());
                });
                notificator.for_each(|time, _, _| {
                    if let Some(updated_count) = stash_counts.remove(&time) {
                        // println!("[{:?}] {} updated nodes", time.time(), updated_count);
                        let branch = updated_count > 0;
                        if let Some(nodes) = stash2.borrow_mut().remove(&time) {
                            output
                                .session(&time)
                                .give_iterator(nodes.into_iter().map(|x| (branch, x)));
                        }
                    }
                });
            },
        )
        .branch(|_t, (some_updated, _pair)| *some_updated);

    (stable.map(|pair| pair.1), some_updated.map(|pair| pair.1))
}

fn delta_step<G: Scope<Timestamp = Product<usize, u32>>>(
    edges: &DistributedEdges,
    nodes: &Stream<G, (u32, State)>,
    delta: u32,
) -> Stream<G, (u32, State)> {
    use timely::dataflow::operators::*;

    let lightly_updated = nodes.scope().iterative::<u32, _, _>(|subscope| {
        let nodes = nodes
            .enter(subscope)
            .map(|(id, state)| (id, state.reactivate()));
        let (handle, cycle) = subscope.feedback(Product::new(Default::default(), 1));

        // FIXME: we cannot retire old configuration
        let nodes2 = edges.send(
            &nodes.concat(&cycle),
            |_, state| state.should_send(),
            move |time, state, weight| State::send_light(time.outer.inner, delta, state, weight),
            |d1, d2| std::cmp::min(*d1, *d2), // aggregate messages
            |state, message| state.update_distance(*message),
            |state| state.deactivate(), // deactivate nodes with no messages
        );
        let (output, further) = branch_stabilized(&nodes2, delta);

        further.connect_loop(handle);

        output.leave()
    });

    // do the heavy updates and return
    edges.send(
        &lightly_updated,
        |_, state| state.distance.is_some(),
        move |time, state, weight| State::send_heavy(time.inner, delta, state, weight),
        |d1, d2| std::cmp::min(*d1, *d2), // aggregate messages
        |state, message| state.update_distance(*message),
        // |state| state.deactivate(), // deactivate nodes with no messages
        |state| state.clone(),
    )
}

pub fn delta_stepping<G: Scope<Timestamp = usize>>(
    edges: DistributedEdges,
    scope: &mut G,
    delta: u32,
    n: u32,
    num_roots: usize,
    seed: u64,
) -> Stream<G, u32> {
    use std::collections::HashSet;

    let rng = Xoshiro256StarStar::seed_from_u64(seed);
    let dist = Uniform::new(0u32, n + 1);
    let roots: HashSet<u32> = dist.sample_iter(rng).take(num_roots).collect();

    // Initialize nodes
    let nodes = edges.nodes::<_, State>(scope).map(move |(id, state)| {
        if roots.contains(&id) {
            (id, State::root())
        } else {
            (id, state)
        }
    });

    // Perform the delta steps, retiring at the end of each
    // delta step the stable nodes
    let distances = nodes.scope().iterative::<u32, _, _>(move |inner_scope| {
        let nodes = nodes.enter(inner_scope);
        let (handle, cycle) = inner_scope.feedback(Product::new(Default::default(), 1));

        let (stable, further) =
            delta_step(&edges, &nodes.concat(&cycle), delta).branch(move |t, (id, state)| {
                state
                    .distance
                    .map(|d| d > delta * (t.inner + 1))
                    .unwrap_or(true)
            });

        further.connect_loop(handle);

        stable.leave()
    });

    distances
        .map(|(_id, state)| state.distance.expect("unreached node"))
        .accumulate(0, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
        .inspect(|partial| println!("Partial maximum {:?}", partial))
        .exchange(|_| 0)
        .accumulate(0, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
}
