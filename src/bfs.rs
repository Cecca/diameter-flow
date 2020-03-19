use crate::distributed_graph::*;
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

pub fn bfs<G: Scope>(edges: DistributedEdges, scope: &mut G) -> Stream<G, u32> {
    use timely::dataflow::operators::*;
    use timely::order::Product;

    let nodes = edges.nodes::<_, State>(scope).map(|(id, state)| {
        if id == 0 {
            (
                id,
                State {
                    active: true,
                    distance: Some(0),
                },
            )
        } else {
            (id, state)
        }
    });

    let distances = nodes.scope().iterative::<u32, _, _>(move |inner_scope| {
        let (handle, cycle) = inner_scope.feedback(Product::new(Default::default(), 1));
        let nodes = nodes.enter(inner_scope);
        let (inactive_nodes, active_nodes) = edges
            .send(
                &nodes.concat(&cycle),
                // Filter nodes that have something to say
                |_, state| state.should_send(),
                // Create messages
                |_id, state, _weight| 1 + state.distance.expect("missing distance"),
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
            .branch(|_t, pair| pair.1.active);

        active_nodes.connect_loop(handle);

        inactive_nodes.leave()
    });

    distances
        .map(|(_, state)| state.distance.expect("missing distance"))
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
