use bytes::CompressedEdgesBlockSet;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;

use timely::ExchangeData;

#[derive(Debug, Clone, Copy)]
struct Distributor {
    side: u32,
}

impl Distributor {
    fn new<G: Scope>(scope: &G) -> Self {
        let side = (scope.peers() as f64).sqrt().ceil() as u32;
        Self { side }
    }

    /// Gets the processor for an edge
    fn proc_edge(&self, src: u32, dst: u32) -> u8 {
        assert!(src < dst, "src >= dst! {} >= {}", src, dst);
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

pub struct DistributedEdgesBuilder {
    edges: Rc<RefCell<Option<CompressedEdgesBlockSet>>>,
    nodes_processors: Rc<RefCell<Option<HashMap<u32, Vec<usize>>>>>,
    distributor: Distributor,
}

impl DistributedEdgesBuilder {
    pub fn new<G: Scope>(stream: &Stream<G, String>) -> (Self, ProbeHandle<G::Timestamp>) {
        use std::collections::HashSet;
        use std::path::PathBuf;
        use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
        use timely::dataflow::operators::generic::operator::Operator;
        use timely::dataflow::operators::*;

        let edges_ref = Rc::new(RefCell::new(None));
        let edges = edges_ref.clone();
        let distributor = Distributor::new(&stream.scope());

        let nodes_ref = Rc::new(RefCell::new(None));
        let nodes_processors = nodes_ref.clone();

        let worker_id = stream.scope().index();
        let mut paths_stash = Vec::new();
        // let mut notificator = FrontierNotificator::new();

        // TODO: load node IDs
        let probe = stream
            .unary_notify(
                Pipeline,
                "edges_builder",
                None,
                move |input, output, notificator| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        paths_stash.extend(data.into_iter());
                        println!("added paths to the stash");
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        let paths = paths_stash.iter().map(|p| PathBuf::from(p));
                        edges_ref.borrow_mut().replace(
                            CompressedEdgesBlockSet::from_files(paths)
                                .expect("problem loading blocks"),
                        );
                        println!("Loaded the edges locally");

                        // exchange the edges to build processor targets
                        let mut nodes = HashSet::new();
                        edges_ref.borrow().iter().for_each(|edges| {
                            edges.iter().for_each(|(u, v, _)| {
                                nodes.insert(u);
                                nodes.insert(v);
                            });
                        });
                        output
                            .session(&t)
                            .give_iterator(nodes.into_iter().map(|u| (u, worker_id)));
                    });
                },
            )
            .unary(
                ExchangePact::new(|pair: &(u32, usize)| pair.0.into()),
                "nodes_builder",
                move |_, _| {
                    move |input, output| {
                        input.for_each(|t, data| {
                            let data = data.replace(Vec::new());
                            let mut opt = nodes_ref.borrow_mut();
                            let map = opt.get_or_insert_with(HashMap::new);
                            for (u, proc_id) in data {
                                map.entry(u).or_insert_with(Vec::new).push(proc_id);
                            }
                            output.session(&t).give(());
                        });
                    }
                },
            )
            .probe();

        // stream.sink(Pipeline, "edges_builder", move |input| {});

        (
            Self {
                edges,
                nodes_processors,
                distributor,
            },
            probe,
        )
    }

    pub fn build(self) -> DistributedEdges {
        let edges = self.edges.borrow_mut().take().expect("Missing edges!");
        let nodes_processors = self
            .nodes_processors
            .borrow_mut()
            .take()
            .expect("Missing nodes processors");
        let mut histogram = std::collections::BTreeMap::new();
        for (_, procs) in nodes_processors.iter() {
            let len = procs.len();
            histogram
                .entry(len)
                .and_modify(|c| *c += len)
                .or_insert(len);
        }
        println!("Distribution of destination sets {:#?}", histogram);
        DistributedEdges {
            edges: Rc::new(edges),
            nodes_processors: Rc::new(nodes_processors),
            distributor: self.distributor,
        }
    }
}

/// A distributed static collection of edges that can be accessed by
pub struct DistributedEdges {
    edges: Rc<CompressedEdgesBlockSet>,
    nodes_processors: Rc<HashMap<u32, Vec<usize>>>,
    distributor: Distributor,
}

impl DistributedEdges {
    pub fn clone(obj: &Self) -> Self {
        Self {
            edges: Rc::clone(&obj.edges),
            nodes_processors: Rc::clone(&obj.nodes_processors),
            distributor: obj.distributor.clone(),
        }
    }

    pub fn for_each<F>(&self, mut action: F)
    where
        F: FnMut(u32, u32, u32),
    {
        for (u, v, w) in self.edges.iter() {
            action(u, v, w);
        }
    }

    pub fn nodes<G: Scope, S: ExchangeData + Default>(&self, scope: &mut G) -> Stream<G, (u32, S)> {
        use timely::dataflow::operators::aggregation::aggregate::Aggregate;
        use timely::dataflow::operators::to_stream::ToStream;
        use timely::dataflow::operators::Map;

        let nodes_processors = Rc::clone(&self.nodes_processors);
        let nodes: Vec<u32> = nodes_processors.keys().cloned().collect();
        nodes.to_stream(scope).map(|u| (u, S::default()))
    }

    fn distributor(&self) -> Distributor {
        self.distributor
    }

    fn for_each_processor<F: FnMut(usize)>(&self, node: u32, mut action: F) {
        for &p in self
            .nodes_processors
            .get(&node)
            .expect("missing procesor for node")
        {
            action(p);
        }
    }

    /// Brings together the states of the endpoints of each edge with the edge itself
    pub fn triplets<G: Scope, S: ExchangeData>(
        &self,
        nodes: &Stream<G, (u32, S)>,
    ) -> Stream<G, ((u32, S), (u32, S), u32)> {
        use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
        use timely::dataflow::operators::{Exchange, Map, Operator};

        let distributor = self.distributor();
        let mut stash = HashMap::new();
        let edges = Self::clone(&self);
        let edges1 = Self::clone(&self);

        nodes
            .unary(Pipeline, "send states", move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        let mut session = output.session(&t);
                        let data = data.replace(Vec::new());
                        for (id, state) in data.into_iter() {
                            edges1
                                .for_each_processor(id, |p| session.give((p, (id, state.clone()))));
                        }
                    })
                }
            })
            // Propagate to the destination
            .unary_notify(
                ExchangePact::new(|pair: &(usize, (u32, S))| pair.0 as u64),
                "create_triplets",
                None,
                move |input, output, notificator| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        stash
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new)
                            .extend(data.into_iter().map(|pair| pair.1));
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        if let Some(states) = stash.remove(&t) {
                            let mut out = output.session(&t);

                            // Accumulate messages going over the edges
                            edges.for_each(|u, v, w| {
                                let state_u = states.get(&u).expect("missing state for u");
                                let state_v = states.get(&v).expect("missing state for v");
                                out.give(((u, state_u.clone()), (v, state_v.clone()), w));
                            });
                        }
                    });
                },
            )
    }

    /// Send messages that can be aggregated along the edges
    pub fn send<G: Scope, S: ExchangeData, M: ExchangeData, P, Fm, Fa, Fu, Fun>(
        &self,
        nodes: &Stream<G, (u32, S)>,
        should_send: P,
        message: Fm,
        aggregate: Fa,
        update: Fu,
        update_no_msg: Fun,
    ) -> Stream<G, (u32, S)>
    where
        P: Fn(u32, &S) -> bool + 'static,
        Fm: Fn(G::Timestamp, &S, u32) -> Option<M> + 'static,
        Fa: Fn(&M, &M) -> M + Copy + 'static,
        Fu: Fn(&S, &M) -> S + 'static,
        Fun: Fn(&S) -> S + 'static,
    {
        use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
        use timely::dataflow::operators::{Exchange, Filter, Map, Operator};

        let distributor = self.distributor();
        let mut stash = HashMap::new();
        let mut node_stash = HashMap::new();
        let mut msg_stash = HashMap::new();
        let edges = Self::clone(&self);
        let edges1 = Self::clone(&self);

        nodes
            // Send states if needed
            .unary(Pipeline, "send states", move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        let mut session = output.session(&t);
                        let data = data.replace(Vec::new());
                        for (id, state) in data.into_iter() {
                            if should_send(id, &state) {
                                edges1.for_each_processor(id, |p| {
                                    session.give((p, (id, state.clone())))
                                });
                            }
                        }
                    })
                }
            })
            // Propagate to the destination
            .unary_notify(
                ExchangePact::new(|pair: &(usize, (u32, S))| pair.0 as u64),
                "msg_propagate",
                None,
                move |input, output, notificator| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        stash
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new)
                            .extend(data.into_iter().map(|pair| pair.1));
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        if let Some(states) = stash.remove(&t) {
                            let mut output_messages = HashMap::new();

                            // Accumulate messages going over the edges
                            edges.for_each(|u, v, w| {
                                if let Some(state_u) = states.get(&u) {
                                    if let Some(msg) = message(t.time().clone(), state_u, w) {
                                        output_messages
                                            .entry(v)
                                            .and_modify(|msg_v| *msg_v = aggregate(msg_v, &msg))
                                            .or_insert(msg);
                                    }
                                }
                                if let Some(state_v) = states.get(&v) {
                                    if let Some(msg) = message(t.time().clone(), state_v, w) {
                                        output_messages
                                            .entry(u)
                                            .and_modify(|msg_u| *msg_u = aggregate(msg_u, &msg))
                                            .or_insert(msg);
                                    }
                                }
                            });
                            // println!(
                            //     "[{:?}] propagating {} messages",
                            //     t.time(),
                            //     output_messages.len()
                            // );

                            // Output the aggregated messages
                            output
                                .session(&t)
                                .give_iterator(output_messages.into_iter());
                        }
                    });
                },
            )
            // Take the messages to the destination and aggregate them
            .binary_notify(
                &nodes,
                ExchangePact::new(|(id, _msg)| *id as u64),
                ExchangePact::new(|(id, _msg)| *id as u64),
                "msg_recv",
                None,
                move |message_input, node_input, output, notificator| {
                    message_input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        let map = msg_stash
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new);
                        for (id, msg) in data.into_iter() {
                            map.entry(id)
                                .and_modify(|acc| *acc = aggregate(acc, &msg))
                                .or_insert(msg);
                        }
                        notificator.notify_at(t.retain());
                    });
                    node_input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        node_stash
                            .entry(t.time().clone())
                            .or_insert_with(Vec::new)
                            .extend(data.into_iter());
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        let mut session = output.session(&t);
                        // For each node, update the state with the received, message, if any
                        if let Some(nodes) = node_stash.remove(t.time()) {
                            let msgs = msg_stash.remove(t.time()).unwrap_or_else(HashMap::new);
                            // println!(
                            //     "[{:?}] got {} messages, and {} nodes",
                            //     t.time(),
                            //     msgs.len(),
                            //     nodes.len()
                            // );
                            let mut cnt_messaged = 0;
                            let mut cnt_no_messaged = 0;
                            for (id, state) in nodes.into_iter() {
                                if let Some(message) = msgs.get(&id) {
                                    session.give((id, update(&state, message)));
                                    cnt_messaged += 1;
                                } else {
                                    session.give((id, update_no_msg(&state)));
                                    cnt_no_messaged += 1;
                                }
                            }
                            // println!(
                            //     "[{:?}], messaged: {}, no messaged: {}",
                            //     t.time(),
                            //     cnt_messaged,
                            //     cnt_no_messaged
                            // );
                        }
                    });
                },
            )
    }
}
