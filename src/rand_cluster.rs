use crate::logging::*;
use differential_dataflow::difference::Monoid;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::*;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::*;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use rand::Rng;
use rand::SeedableRng;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::{AddAssign, Mul};
use std::rc::Rc;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::Product;
use timely::progress::Timestamp;

fn init_nodes<G: Scope<Timestamp = usize>>(
    edges: &Stream<G, (u32, u32, u32)>,
) -> Collection<G, (u32, NodeState), isize> {
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

                    let mut session = output.session(&time);
                    for i in lower..upper {
                        session.give(((i, NodeState::default()), *time.time(), 1));
                    }
                });
            },
        )
        .as_collection()
}

fn sample_centers<G: Scope<Timestamp = Product<usize, u32>>, R: Rng + 'static>(
    nodes: &Collection<G, (u32, NodeState), isize>,
    n: usize,
    rand: Rc<RefCell<R>>,
) -> Collection<G, (u32, NodeState), isize> {
    nodes
        .inner
        .map(move |((node, state), time, dist)| {
            let p = 2_f64.powi(time.inner as i32) / n as f64;
            if state.is_uncovered() && rand.borrow_mut().gen_bool(p) {
                ((node, state.as_center(node)), time, dist)
            } else {
                ((node, state), time, dist)
            }
        })
        .as_collection()
}

#[derive(Debug, Clone, Abomonation, Hash, Ord, PartialOrd, Eq, PartialEq)]
struct NodeState {
    active: bool,
    distance: Option<(u32, u32)>,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            active: true,
            distance: None,
        }
    }
}

impl NodeState {
    fn is_uncovered(&self) -> bool {
        self.active && self.distance.is_none()
    }

    fn can_send(&self) -> bool {
        self.active && self.distance.is_some()
    }

    fn propagate(&self, weight: u32) -> Either {
        assert!(self.can_send());
        self.distance
            .map(|(root, distance)| Either::Message((root, distance + weight)))
            .unwrap()
    }

    fn as_center(&self, id: u32) -> Self {
        assert!(self.is_uncovered());
        Self {
            active: true,
            distance: Some((id, 0)),
        }
    }

    fn updated(&self, distance: (u32, u32)) -> Self {
        if self.active {
            let dist = self
                .distance
                .map(|(cur_root, cur_dist)| {
                    if distance.1 < cur_dist {
                        distance
                    } else {
                        (cur_root, cur_dist)
                    }
                })
                .or_else(|| Some(distance));
            Self {
                active: true,
                distance: dist,
            }
        } else {
            self.clone()
        }
    }
}

#[derive(Debug, Clone, Abomonation, Hash, Ord, PartialOrd, Eq, PartialEq)]
enum Either {
    Message((u32, u32)),
    State(NodeState),
}

impl Either {
    fn is_state(&self) -> bool {
        match self {
            Self::State(_) => true,
            _ => false,
        }
    }

    fn as_state(self) -> NodeState {
        match self {
            Self::State(state) => state,
            _ => panic!("attemped to get a state from a message"),
        }
    }
    fn state<'a>(&'a self) -> &'a NodeState {
        match self {
            Self::State(state) => state,
            _ => panic!("attemped to get a state from a message"),
        }
    }
    fn message<'a>(&'a self) -> (u32, u32) {
        match self {
            Self::Message(pair) => *pair,
            _ => panic!("attemped to get a message from a state"),
        }
    }
}

fn expand_clusters<G, Tr>(
    nodes: &Collection<G, (u32, NodeState), isize>,
    light_edges: &Arranged<G, Tr>,
    radius: u32,
) -> Collection<G, (u32, NodeState), isize>
where
    G: Scope<Timestamp = Product<usize, u32>>,
    Tr: TraceReader<Key = u32, Val = (u32, u32), Time = G::Timestamp, R = isize> + Clone + 'static,
    Tr::Batch: BatchReader<u32, Tr::Val, G::Timestamp, Tr::R>,
    Tr::Cursor: Cursor<u32, Tr::Val, G::Timestamp, Tr::R>,
{
    use crate::logging::CountEvent::*;
    use differential_dataflow::operators::iterate::Variable;

    let l1 = nodes.scope().count_logger().expect("missing logger");
    let l2 = l1.clone();
    let l3 = l1.clone();
    let l4 = l1.clone();
    let l5 = l1.clone();

    nodes
        .scope()
        .iterative::<u32, _, _>(move |subscope| {
            let edges = light_edges.enter(&subscope);
            // let nodes = nodes.enter(&subscope);
            let nodes = Variable::new_from(
                nodes
                    .enter(subscope)
                    .map(|(id, state)| (id, Either::State(state))),
                Product::new(Default::default(), 1_u32),
            );

            let iter_res = nodes
                // Propagate distances from active nodes with actual distances
                .filter(|(id, state)| state.state().can_send())
                .join_core(&edges, |src, state, (dst, weight)| {
                    Some((*dst, state.state().propagate(*weight)))
                })
                .concat(&nodes)
                // Compute the closest root among the messages
                .reduce(move |_k, inputs, outputs| {
                    let old_state: &NodeState = inputs
                        .iter()
                        .find(|(either, _)| either.is_state())
                        .expect("missing old state")
                        .0
                        .state();
                    let closest: (u32, u32) = inputs
                        .iter()
                        .filter_map(move |(either, diff)| {
                            if either.is_state() || either.message().1 > radius {
                                None
                            } else {
                                Some((either.message(), diff))
                            }
                        })
                        .min_by_key(|((root, dist), _)| *dist)
                        .unwrap()
                        .0
                        .clone();
                    outputs.push((Either::State(old_state.updated(closest)), 1))
                });

            nodes.set(&iter_res.consolidate());

            iter_res.leave()
        })
        .map(|(id, either)| (id, either.as_state()))
}

#[derive(Debug, Clone, Copy)]
struct Distributor {
    side: u32,
}

impl Distributor {
    fn new<G: Scope>(scope: &G) -> Self {
        let side = ((scope.peers() as f64).sqrt().ceil() as u32);
        Self { side }
    }

    /// Gets the processor for an edge
    fn proc_edge(&self, src: u32, dst: u32) -> u8 {
        assert!(src > dst);
        let cell_i = src % self.side;
        let cell_j = dst % self.side;
        let pos_i = src / self.side;
        let pos_j = dst / self.side;
        if pos_i >= pos_j {
            (cell_i * self.side + cell_j) as u8
        } else {
            (cell_j * self.side + cell_i) as u8
        }
    }

    fn procs_node(&self, node: u32) -> impl Iterator<Item = u8> {
        let mut destinations = std::collections::HashSet::new();
        let mod_p = node % self.side;
        for j in 0..self.side {
            let p = mod_p * self.side + j;
            destinations.insert(p as u8);
        }
        for i in 0..self.side {
            let p = i * self.side + mod_p;
            destinations.insert(p as u8);
        }
        assert!(destinations.len() <= 2 * self.side as usize);
        destinations.into_iter()
    }
}

fn remap_edges<G: Scope>(
    clustering: &Stream<G, (u32, NodeState)>,
    edges: &Stream<G, (u32, u32, u32)>,
) -> Stream<G, ((u32, u32), u32)> {
    use timely::dataflow::channels::pact::Exchange;
    let distrib = Distributor::new(&clustering.scope());
    let edges = edges.flat_map(move |(src, dst, weight)| {
        if src > dst {
            Some((distrib.proc_edge(src, dst), (src, dst, weight)))
        } else {
            None
        }
    });
    let nodes = clustering.flat_map(move |(node, state)| {
        distrib
            .procs_node(node)
            .map(move |p| (p, (node, state.clone())))
    });

    let mut stash_edges = HashMap::new();
    let mut stash_nodes = HashMap::new();
    let mut stash_deduplicator = HashMap::new();

    edges
        .binary_notify(
            &nodes,
            Exchange::new(|(p, _)| *p as u64),
            Exchange::new(|(p, _)| *p as u64),
            "graph contraction",
            None,
            move |in1, in2, out, notificator| {
                in1.for_each(|t, data| {
                    let mut data = data.replace(Vec::new());
                    stash_edges
                        .entry(t.time().clone())
                        .or_insert_with(Vec::new)
                        .extend(data.drain(..).map(|pair| pair.1));
                    notificator.notify_at(t.retain());
                });
                in2.for_each(|t, data| {
                    let mut data = data.replace(Vec::new());
                    stash_nodes
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::new)
                        .extend(data.drain(..).map(|pair| pair.1));
                    notificator.notify_at(t.retain());
                });

                notificator.for_each(|time, _, _| {
                    if let Some(edges) = stash_edges.remove(&time.time()) {
                        let nodes = stash_nodes.remove(&time.time()).unwrap();
                        let mut session = out.session(&time);
                        for (src, dst, weight) in edges {
                            let (center_src, distance_src) = nodes
                                .get(&src)
                                .expect("missing source")
                                .distance
                                .expect("uncovered source!");
                            let (center_dst, distance_dst) = nodes
                                .get(&dst)
                                .expect("missing destination")
                                .distance
                                .expect("uncovered destination!");
                            session.give((
                                (center_src, center_dst),
                                distance_src + weight + distance_dst,
                            ));
                        }
                    }
                });
            },
        )
        // Remove duplicates keeping just the minimum weight edge for each key
        .unary_notify(
            Exchange::new(move |((src, dst), _weight)| distrib.proc_edge(*src, *dst) as u64),
            "deduplicate",
            None,
            move |input, output, notificator| {
                input.for_each(|t, data| {
                    let mut data = data.replace(Vec::new());
                    for (edge, weight) in data.drain(..) {
                        stash_deduplicator
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new)
                            .entry(edge)
                            .and_modify(|w| *w = std::cmp::min(*w, weight))
                            .or_insert(weight);
                    }
                    notificator.notify_at(t.retain());
                });

                notificator.for_each(|time, _, _| {
                    if let Some(mut edges) = stash_deduplicator.remove(&time.time()) {
                        output.session(&time).give_iterator(edges.drain());
                    }
                });
                unimplemented!()
            },
        )
}

#[allow(dead_code)]
pub fn rand_cluster<G: Scope<Timestamp = usize>>(
    edges: &Stream<G, (u32, u32, u32)>,
    radius: u32,
    n: usize,
    seed: u64,
) -> Stream<G, u32> {
    use differential_dataflow::operators::iterate::SemigroupVariable;
    use rand_xoshiro::Xoroshiro128StarStar;

    let nodes = init_nodes(edges);
    let l1 = edges.scope().count_logger().expect("missing logger");
    let l2 = l1.clone();
    let l3 = l1.clone();

    let mut rand = Xoroshiro128StarStar::seed_from_u64(seed);
    for _ in 0..edges.scope().index() {
        rand.jump();
    }
    let rand = Rc::new(RefCell::new(rand));

    // Separate light and heavy edges, and arrage them
    let light = edges
        .filter(move |trip| trip.2 <= radius)
        .inspect_batch(move |_, x| l1.log((CountEvent::LightEdges, x.len().into())))
        .map(|(src, dst, weight)| ((src, (dst, weight)), 0, 1)) // FIXME: Time
        .as_collection()
        .arrange_by_key();

    let clustering = nodes.scope().iterative::<u32, _, _>(|inner_scope| {
        let light = light.enter(inner_scope);

        let summary = Product::new(Default::default(), 1);
        let nodes = Variable::new_from(nodes.enter(&inner_scope), summary);

        let updated =
            expand_clusters(&sample_centers(&nodes, n, rand), &light, radius).consolidate();

        nodes.set(&updated);

        updated.leave()
    });

    let auxiliary_graph = remap_edges(&clustering.inner.map(|triplet| triplet.0), &edges);

    unimplemented!()
}

#[test]
fn tryout() {
    timely::execute_directly(|worker| {
        use differential_dataflow::input::Input;
        use differential_dataflow::trace::implementations::spine_fueled_neu::Spine;

        let mut input = worker.dataflow::<usize, _, _>(|scope| {
            let (input, coll) = scope.new_collection::<(u32, Option<(u32, u32)>), isize>();

            // coll.inspect(|triplet| eprintln!("{:?}", triplet));

            coll.reduce(|key, input, output| {
                eprintln!("key: {:?}, input: {:?}, output: {:?}", key, input, output);
                output.push((input[0].0.clone(), input[0].1.clone()));
            });

            input
        });

        input.insert((1, Some((0, 1))));
        input.insert((1, Some((0, 3))));
        input.close();
        worker.step();
    });
}
