use crate::logging::*;
use bytes::*;
// use bytes::CompressedEdgesBlockSet;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::ExchangeData;

pub struct DistributedEdgesBuilder {
    edges: Rc<RefCell<Option<CompressedEdgesBlockSet>>>,
    nodes_processors: Rc<RefCell<Option<HashMap<u32, Vec<usize>>>>>,
}

impl DistributedEdgesBuilder {
    pub fn new<G: Scope>(
        stream: &Stream<G, (String, Option<String>)>,
    ) -> (Self, ProbeHandle<G::Timestamp>) {
        use std::collections::HashSet;
        use std::path::PathBuf;
        use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
        use timely::dataflow::operators::generic::operator::Operator;
        use timely::dataflow::operators::*;

        let edges_ref = Rc::new(RefCell::new(None));
        let edges = edges_ref.clone();

        let nodes_ref = Rc::new(RefCell::new(None));
        let nodes_processors = nodes_ref.clone();

        let worker_id = stream.scope().index();
        let mut paths_stash = Vec::new();

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
                        let paths = paths_stash.iter().map(|(edges_path, weights_path)| {
                            (
                                PathBuf::from(edges_path),
                                weights_path.as_ref().map(|s| PathBuf::from(s)),
                            )
                        });
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

        (
            Self {
                edges,
                nodes_processors,
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
        }
    }
}

/// A distributed static collection of edges that can be accessed by
pub struct DistributedEdges {
    edges: Rc<CompressedEdgesBlockSet>,
    nodes_processors: Rc<HashMap<u32, Vec<usize>>>,
}

impl DistributedEdges {
    pub fn clone(obj: &Self) -> Self {
        Self {
            edges: Rc::clone(&obj.edges),
            nodes_processors: Rc::clone(&obj.nodes_processors),
        }
    }

    pub fn for_each<F>(&self, mut action: F)
    where
        F: FnMut(u32, u32, u32),
    {
        // for (u, v, w) in self.edges.iter() {
        //     action(u, v, w);
        // }
        self.edges.for_each(action);
    }

    pub fn nodes<G: Scope, S: ExchangeData + Default>(&self, scope: &mut G) -> Stream<G, (u32, S)> {
        use timely::dataflow::operators::to_stream::ToStream;
        use timely::dataflow::operators::Map;

        let nodes_processors = Rc::clone(&self.nodes_processors);
        let nodes: Vec<u32> = nodes_processors.keys().cloned().collect();
        nodes.to_stream(scope).map(|u| (u, S::default()))
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
        use timely::dataflow::operators::{Map, Operator};

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
        G::Timestamp: ToPair,
        P: Fn(u32, &S) -> bool + 'static,
        Fm: Fn(G::Timestamp, &S, u32) -> Option<M> + 'static,
        Fa: Fn(&M, &M) -> M + Copy + 'static,
        Fu: Fn(&S, &M) -> S + 'static,
        Fun: Fn(&S) -> S + 'static,
    {
        use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
        use timely::dataflow::operators::*;

        let l1 = nodes.scope().count_logger().expect("Missing logger");
        let l2 = l1.clone();

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
                        l1.log((
                            CountEvent::load_state_exchange(t.time().clone()),
                            data.len() as u64,
                        ));
                        let node_map = stash
                            .entry(t.time().clone())
                            .or_insert_with(|| edges.edges.node_map());
                        for (_, (u, state)) in data.into_iter() {
                            node_map.set(u, state);
                        }
                        // .extend(data.into_iter().map(|pair| pair.1));
                        // stash
                        //     .entry(t.time().clone())
                        //     .or_insert_with(HashMap::new)
                        //     .extend(data.into_iter().map(|pair| pair.1));
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        if let Some(states) = stash.remove(&t) {
                            // let mut output_messages = HashMap::new();
                            let mut output_messages = edges.edges.node_map();
                            println!(
                                "Allocated vector for {} destination nodes",
                                output_messages.allocated_size()
                            );

                            // Accumulate messages going over the edges
                            // This is the hot loop, where most of the time is spent
                            edges.for_each(|u, v, w| {
                                if let Entry::Occupied(state_u) = states.get(u) {
                                    if let Some(msg) = message(t.time().clone(), state_u, w) {
                                        output_messages
                                            .entry(v)
                                            .and_modify(|msg_v| *msg_v = aggregate(msg_v, &msg))
                                            .or_insert(msg);
                                    }
                                }
                                if let Entry::Occupied(state_v) = states.get(v) {
                                    if let Some(msg) = message(t.time().clone(), state_v, w) {
                                        output_messages
                                            .entry(u)
                                            .and_modify(|msg_u| *msg_u = aggregate(msg_u, &msg))
                                            .or_insert(msg);
                                    }
                                }
                            });
                            println!(
                                "Occupancy of node states: {}\nOccupancy of messages: {}",
                                states.occupancy(),
                                output_messages.occupancy()
                            );
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
                        l2.log((
                            CountEvent::load_message_exchange(t.time().clone()),
                            data.len() as u64,
                        ));
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
