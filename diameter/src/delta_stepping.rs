use crate::distributed_graph::*;
use crate::logging::*;
use crate::operators::*;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand_xoshiro::Xoshiro256StarStar;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::Product;

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

    fn send_heavy(_step: u32, delta: u32, state: &Self, weight: u32) -> Option<u32> {
        if weight > delta {
            Some(weight + state.distance.expect("missing distance"))
        } else {
            None
        }
    }
}

fn delta_step<G: Scope<Timestamp = Product<(), u32>>>(
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

        let (output, further) = edges
            .send(
                &nodes.concat(&cycle),
                true,
                |_, state| state.should_send(),
                move |time, state, weight| {
                    State::send_light(time.outer.inner, delta, state, weight)
                },
                |d1, d2| std::cmp::min(*d1, *d2), // aggregate messages
                |state, message| state.update_distance(*message),
                |state| state.deactivate(), // deactivate nodes with no messages
            )
            .branch_all("updated", |_, pair| pair.1.updated, 0);

        further
            // .inspect(|p| info!("{:?}", p))
            .connect_loop(handle);

        output.leave()
    });

    // do the heavy updates and return
    edges.send(
        &lightly_updated,
        true,
        |_, state| state.distance.is_some(),
        move |time, state, weight| State::send_heavy(time.inner, delta, state, weight),
        |d1, d2| std::cmp::min(*d1, *d2), // aggregate messages
        |state, message| state.update_distance(*message),
        // |state| state.deactivate(), // deactivate nodes with no messages
        |state| state.clone(),
    )
}

pub fn delta_stepping<G: Scope<Timestamp = ()>>(
    edges: DistributedEdges,
    scope: &mut G,
    delta: u32,
    n: u32,
    seed: u64,
) -> Stream<G, u32> {
    use std::collections::HashSet;

    // let l1 = scope.count_logger().expect("missing logger");
    let nodes = if scope.index() == 0 {
        let mut rng = Xoshiro256StarStar::seed_from_u64(seed);
        let dist = Uniform::new(0u32, n + 1);
        let root: u32 = dist.sample(&mut rng);

        vec![(root, State::root())]
    } else {
        vec![]
    }
    .to_stream(scope)
    .exchange(|p| p.0 as u64);

    // Perform the delta steps, retiring at the end of each
    // delta step the stable nodes
    let distances = nodes.scope().iterative::<u32, _, _>(move |inner_scope| {
        let nodes = nodes.enter(inner_scope);
        let (handle, cycle) = inner_scope.feedback(Product::new(Default::default(), 1));

        let (stable, further) = delta_step(
            &edges,
            &nodes.concat(&cycle),
            // .inspect_batch(move |t, data| {
            //     l1.log((CountEvent::Active(t.inner), data.len() as u64))
            // }),
            delta,
        )
        // We should also keep the old states because otherwise we enter
        // and endless loop
        .branch_all(
            "updated",
            move |t, (_id, state)| {
                state
                    .distance
                    .map(|d| d > delta * (t.inner + 1))
                    .unwrap_or(true)
            },
            0,
        );

        further.connect_loop(handle);

        stable.leave()
    });

    distances
        .map(|(_id, state)| state.distance.expect("unreached node"))
        .accumulate(0, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
        .inspect(|partial| info!("Partial maximum {:?}", partial))
        .exchange(|_| 0)
        .accumulate(0, |max, data| {
            *max = std::cmp::max(*data.iter().max().expect("empty collection"), *max)
        })
}
