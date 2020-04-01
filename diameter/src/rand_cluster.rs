use crate::distributed_graph::*;
use crate::logging::*;
use crate::operators::*;
use crate::sequential::*;
use rand::Rng;
use rand::SeedableRng;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::Product;

#[derive(Debug, Clone, Copy, Abomonation, Hash, Ord, PartialOrd, Eq, PartialEq)]
struct Message {
    distance: u32,
    root: u32,
    generation: u32,
}

impl Message {
    fn merge(msg1: &Self, msg2: &Self) -> Self {
        if msg1.generation < msg2.generation {
            // The newest wins
            *msg2
        } else if msg1.generation > msg2.generation {
            *msg1
        } else if msg1.distance < msg2.distance {
            *msg1
        } else if msg1.distance > msg2.distance {
            *msg2
        } else if msg1.root < msg2.root {
            *msg1
        } else {
            *msg2
        }
    }
}

#[derive(Debug, Clone, Abomonation, Hash, Ord, PartialOrd, Eq, PartialEq)]
enum NodeState {
    Uncovered,
    Covered {
        root: u32,
        distance: u32,
        generation: u32,
        updated: bool,
    },
}

impl Default for NodeState {
    fn default() -> Self {
        Self::Uncovered
    }
}

impl NodeState {
    fn is_uncovered(&self) -> bool {
        match self {
            Self::Uncovered => true,
            _ => false,
        }
    }

    fn can_send(&self) -> bool {
        match *self {
            Self::Covered {
                root,
                distance,
                generation,
                updated,
            } => updated,
            _ => false,
        }
    }

    fn propagate(&self, weight: u32, radius: u32, round: u32) -> Option<Message> {
        match *self {
            Self::Covered {
                root,
                distance,
                generation,
                updated,
            } => {
                assert!(updated, "no reason to send from non-updated nodes");
                if distance + weight > radius * (1 + round - generation) {
                    None
                } else {
                    Some(Message {
                        root,
                        distance: distance + weight,
                        generation,
                    })
                }
            }
            Self::Uncovered => panic!("cannot send from an uncovered node"),
        }
    }

    fn distance(&self) -> u32 {
        match self {
            Self::Covered {
                root,
                distance,
                generation,
                updated,
            } => *distance,
            Self::Uncovered => panic!("Cannot get distance from uncovered node"),
        }
    }

    fn root(&self) -> u32 {
        match self {
            Self::Covered {
                root,
                distance,
                generation,
                updated,
            } => *root,
            Self::Uncovered => panic!("Cannot get distance from uncovered node"),
        }
    }

    fn as_center(&self, id: u32, generation: u32) -> Self {
        match &self {
            Self::Uncovered => Self::Covered {
                root: id,
                distance: 0,
                generation,
                updated: true,
            },
            _ => panic!("only uncovered nodes can become centers"),
        }
    }

    fn deactivate(&self) -> Self {
        match *self {
            Self::Covered {
                root,
                distance,
                generation,
                updated,
            } => Self::Covered {
                root,
                distance,
                generation,
                updated: false,
            },
            Self::Uncovered => Self::Uncovered,
        }
    }

    fn updated(&self, message: Message) -> Self {
        match *self {
            Self::Uncovered => Self::Covered {
                root: message.root,
                distance: message.distance,
                generation: message.generation,
                updated: true,
            },
            Self::Covered {
                root,
                distance,
                generation,
                updated,
            } => {
                if message.distance < distance
                    && (message.generation > generation || message.root == root)
                {
                    Self::Covered {
                        root: message.root,
                        distance: message.distance,
                        generation: message.generation,
                        updated: true,
                    }
                } else {
                    Self::Covered {
                        root,
                        distance,
                        generation,
                        updated: false,
                    }
                }
            }
        }
    }
}

fn sample_centers<G: Scope<Timestamp = Product<usize, u32>>, R: Rng + 'static>(
    nodes: &Stream<G, (u32, NodeState)>,
    n: u32,
    rand: Rc<RefCell<R>>,
) -> Stream<G, (u32, NodeState)> {
    let mut stash = HashMap::new();
    let l1 = nodes.scope().count_logger().expect("missing logger");

    // we have to stash the nodes and sort them to make the center sampling
    // deterministic, for a fixed random generator.
    nodes.unary_notify(
        Pipeline,
        "sample",
        None,
        move |input, output, notificator| {
            input.for_each(|t, data| {
                let data = data.replace(Vec::new());
                stash
                    .entry(t.time().clone())
                    .or_insert_with(Vec::new)
                    .extend(data.into_iter());
                notificator.notify_at(t.retain());
            });
            notificator.for_each(|t, _, _| {
                if let Some(mut nodes) = stash.remove(&t) {
                    let generation = t.time().inner;
                    nodes.sort_by_key(|pair| pair.0);
                    l1.log((CountEvent::Active(t.inner), nodes.len() as u64));
                    let mut out = output.session(&t);
                    let population_size = nodes.len();
                    let mut cnt = 0;
                    let p = 2_f64.powi(t.time().inner as i32) / n as f64;
                    if p <= 1.0 {
                        for (id, state) in nodes.into_iter() {
                            if state.is_uncovered() && rand.borrow_mut().gen_bool(p) {
                                out.give((id, state.as_center(id, generation)));
                                cnt += 1;
                            } else {
                                out.give((id, state));
                            }
                        }
                    } else {
                        cnt = nodes.len();
                        out.give_iterator(nodes.into_iter().map(|(id, state)| {
                            if state.is_uncovered() {
                                (id, state.as_center(id, generation))
                            } else {
                                (id, state)
                            }
                        }));
                    }
                    l1.log((CountEvent::Centers(t.inner), cnt as u64));
                    println!(
                        "{} centers out of {} sampled at iteration {}",
                        cnt, population_size, t.inner
                    );
                }
            });
        },
    )
}

fn expand_clusters<G>(
    edges: &DistributedEdges,
    nodes: &Stream<G, (u32, NodeState)>,
    radius: u32,
    _n: u32,
) -> Stream<G, (u32, NodeState)>
where
    G: Scope<Timestamp = Product<usize, u32>>,
{
    let l1 = nodes.scope().count_logger().expect("missing logger");
    let _l2 = l1.clone();
    let _l3 = l1.clone();
    let _l4 = l1.clone();
    let _l5 = l1.clone();

    nodes.scope().iterative::<u32, _, _>(move |subscope| {
        let nodes = nodes.enter(subscope);

        let (handle, cycle) = subscope.feedback(Product::new(Default::default(), 1));

        let (output, further) = edges
            .send(
                &nodes.concat(&cycle),
                |_, state| state.can_send(),
                move |time, state, weight| {
                    // if weight < radius {
                    state.propagate(weight, radius, time.outer.inner)
                    // } else {
                    // None
                    // }
                },
                |d1, d2| Message::merge(d1, d2), // aggregate messages
                |state, message| state.updated(*message),
                |state| state.deactivate(), // deactivate nodes with no messages
            )
            .branch_all(|pair| pair.1.can_send()); // Circulate while someone has something to say

        further.connect_loop(handle);

        output.leave()
    })
}

fn remap_edges<G: Scope>(
    edges: &DistributedEdges,
    clustering: &Stream<G, (u32, NodeState)>,
) -> Stream<G, ((u32, u32), u32)> {
    use std::collections::hash_map::DefaultHasher;

    edges
        .triplets(&clustering, |((_u, state_u), (_v, state_v), w)| {
            let d_u = state_u.distance();
            let d_v = state_v.distance();
            let c_u = state_u.root();
            let c_v = state_v.root();
            let out_edge = if c_u < c_v {
                Some(((c_u, c_v), w + d_u + d_v))
            } else if c_u > c_v {
                Some(((c_v, c_u), w + d_u + d_v))
            } else {
                None
            };
            out_edge
        })
        .aggregate(
            |_key, val, min_weight: &mut Option<u32>| {
                *min_weight = min_weight.map(|min| std::cmp::min(min, val)).or(Some(val));
            },
            // we must use Option<u32> because the state is initialized according to Default,
            // which for u32 is 0, that doesn't play well with the minimum we want to compute
            |key, min_weight: Option<u32>| {
                (key, min_weight.expect("no weights received for this node"))
            },
            move |key| {
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                hasher.finish()
            },
        )
}

pub fn rand_cluster<G: Scope<Timestamp = usize>>(
    edges: DistributedEdges,
    scope: &mut G,
    radius: u32,
    n: u32,
    seed: u64,
) -> Stream<G, u32> {
    use rand_xoshiro::Xoroshiro128StarStar;

    let nodes = edges.nodes::<_, NodeState>(scope);

    let mut rand = Xoroshiro128StarStar::seed_from_u64(seed);
    for _ in 0..nodes.scope().index() {
        rand.jump();
    }
    let rand = Rc::new(RefCell::new(rand));

    let clustering = nodes.scope().iterative::<u32, _, _>(|inner_scope| {
        let nodes = nodes.enter(&inner_scope);

        let summary = Product::new(Default::default(), 1);
        let (handle, cycle) = inner_scope.feedback(summary);

        let (stable, further) = expand_clusters(
            &edges,
            &sample_centers(&nodes.concat(&cycle), n, rand),
            radius,
            n,
        )
        // .branch(move |_t, (_id, state)| state.is_uncovered());
        .branch_all(move |(id, state)| state.is_uncovered());

        further.connect_loop(handle);

        stable.leave()
    });

    let auxiliary_graph = remap_edges(&edges, &clustering);
    let clusters_radii = clustering
        .map(|(_id, state)| (state.root(), state.distance()))
        .aggregate(
            |_center, distance, agg| {
                *agg = std::cmp::max(*agg, distance);
            },
            |center, agg: u32| (center, agg),
            |key| *key as u64,
        );

    // Collect the auxiliary graph and compute the diameter on it
    let mut stash_auxiliary = HashMap::new();
    let mut stash_radii = HashMap::new();
    let mut remapping = HashMap::new();
    let mut node_count = 0;
    auxiliary_graph.binary_notify(
        &clusters_radii,
        ExchangePact::new(|_| 0),
        ExchangePact::new(|_| 0),
        "diameter computation",
        None,
        move |graph_input, radii_input, output, notificator| {
            graph_input.for_each(|t, data| {
                let data = data.replace(Vec::new());
                let entry = stash_auxiliary
                    .entry(t.time().clone())
                    .or_insert_with(HashMap::new);
                for ((u, v), w) in data.into_iter() {
                    let u1: u32 = *remapping.entry(u).or_insert_with(|| {
                        let x = node_count;
                        node_count += 1;
                        x
                    });
                    let v1: u32 = *remapping.entry(v).or_insert_with(|| {
                        let x = node_count;
                        node_count += 1;
                        x
                    });
                    entry.insert((u1, v1), w);
                }
                notificator.notify_at(t.retain());
            });
            radii_input.for_each(|t, data| {
                let data = data.replace(Vec::new());
                stash_radii
                    .entry(t.time().clone())
                    .or_insert_with(HashMap::<u32, u32>::new)
                    .extend(data.into_iter().map(|(root, radius)| {
                        (
                            *remapping.entry(root).or_insert_with(|| {
                                let x = node_count;
                                node_count += 1;
                                x
                            }),
                            radius,
                        )
                    }));
                notificator.notify_at(t.retain());
            });

            notificator.for_each(|t, _, _| {
                if let Some(edges) = stash_auxiliary.remove(t.time()) {
                    let radii = stash_radii.remove(t.time()).expect("missing radii");
                    let max_radius = *radii.values().max().expect("empty radii collection");
                    let n = 1 + *edges
                        .keys()
                        .map(|(u, v)| std::cmp::max(u, v))
                        .max()
                        .expect("could not compute n from edge stream")
                        as usize;
                    println!(
                        "Size of the auxiliary graph: {} nodes and {} edges",
                        n,
                        edges.len()
                    );

                    let (aux_diam, (u, v)) = approx_diameter(edges, n as u32);
                    println!(
                        "Maximum cluster radius {}, radii used {} and {}",
                        max_radius,
                        radii[&(u as u32)],
                        radii[&(v as u32)]
                    );
                    let diameter = aux_diam + radii[&(u as u32)] + radii[&(v as u32)];
                    if max_radius > diameter {
                        output.session(&t).give(max_radius);
                    } else {
                        output.session(&t).give(diameter);
                    }
                }
            });
        },
    )
}
