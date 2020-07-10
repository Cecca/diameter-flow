use crate::distributed_graph::*;

use crate::operators::BranchAll;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand_xoshiro::Xoshiro256StarStar;
use timely::dataflow::Scope;
use timely::dataflow::Stream;

#[derive(Debug, Clone, Abomonation)]
struct State {
    distance: Option<u32>,
    active: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            distance: None,
            active: true,
        }
    }
}

impl State {
    fn should_send(&self) -> bool {
        self.active && self.distance.is_some()
    }
}

pub fn bfs<G: Scope<Timestamp = usize>>(
    edges: DistributedEdges,
    scope: &mut G,
    n: u32,
    seed: u64,
) -> Stream<G, u32> {
    use timely::dataflow::operators::*;
    use timely::order::Product;

    let nodes = if scope.index() == 0 {
        let mut rng = Xoshiro256StarStar::seed_from_u64(seed);
        let dist = Uniform::new(0u32, n + 1);
        let root: u32 = dist.sample(&mut rng);
        info!("Root is {}", root);
        vec![(
            root,
            State {
                active: true,
                distance: Some(0),
            },
        )]
    } else {
        vec![]
    }
    .to_stream(scope)
    .exchange(|p| p.0 as u64);

    let distances = nodes.scope().iterative::<u32, _, _>(move |inner_scope| {
        let (handle, cycle) = inner_scope.feedback(Product::new(Default::default(), 1));
        let nodes = nodes.enter(inner_scope);
        let (inactive_nodes, active_nodes) = edges
            .send(
                &nodes.concat(&cycle),
                true,
                // Filter nodes that have something to say
                |_, state| state.should_send(),
                // Create messages
                |_time, state, _weight| Some(1 + state.distance.expect("missing distance")),
                // Aggregate messages
                |d1, d2| std::cmp::min(*d1, *d2),
                // Update nodes that got messages
                |state, message| {
                    if state.active {
                        let was_uncovered = state.distance.is_none();
                        if was_uncovered {
                            State {
                                active: true,
                                distance: Some(*message),
                            }
                        } else {
                            State {
                                active: false,
                                ..state.clone()
                            }
                        }
                    } else {
                        state.clone()
                    }
                },
                // Deactivate already reached nodes that got no messages
                |state| {
                    if state.active && state.distance.is_some() {
                        State {
                            active: false,
                            ..state.clone()
                        }
                    } else {
                        state.clone()
                    }
                },
            )
            .branch_all("active", |_t, pair| pair.1.active, 0);

        active_nodes.connect_loop(handle);

        inactive_nodes.leave()
    });

    distances
        .map(|(_, state)| state.distance.expect("missing distance"))
        // .inspect(|c| info!("{:?}", c))
        .accumulate(0, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
        .inspect(|partial| info!("Partial maximum {:?}", partial))
        .exchange(|_| 0)
        .accumulate(0, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
}
