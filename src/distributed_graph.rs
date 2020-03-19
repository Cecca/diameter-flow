use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Timestamp;
use timely::ExchangeData;

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
    edges: Rc<RefCell<Option<Vec<(u32, u32, u32)>>>>,
    distributor: Distributor,
}

impl DistributedEdgesBuilder {
    pub fn new<G: Scope>(stream: &Stream<G, (u32, u32, u32)>) -> Self {
        use std::collections::HashMap;
        use timely::dataflow::channels::pact::Exchange;
        use timely::dataflow::operators::generic::operator::Operator;
        use timely::dataflow::operators::FrontierNotificator;

        let mut edges_ref = Rc::new(RefCell::new(None));
        let edges = edges_ref.clone();
        let distributor = Distributor::new(&stream.scope());

        stream.sink(
            Exchange::new(move |triplet: &(u32, u32, u32)| {
                distributor.proc_edge(triplet.0, triplet.1) as u64
            }),
            "edges_builder",
            move |input| {
                input.for_each(|t, data| {
                    let data = data.replace(Vec::new());
                    edges_ref
                        .borrow_mut()
                        .get_or_insert_with(Vec::new)
                        .extend(data.into_iter());
                });
            },
        );

        Self { edges, distributor }
    }

    pub fn build(self) -> DistributedEdges {
        let edges = self.edges.borrow_mut().take().expect("Missing edges!");
        println!("Processor has {} edges", edges.len());
        DistributedEdges {
            edges: Rc::new(edges),
            //     Rc::try_unwrap(self.edges)
            //         .unwrap_or_else(|rc| {
            //             panic!(
            //                 "more than one pointer to the edges: {}",
            //                 Rc::strong_count(&rc)
            //             )
            //         })
            //         .into_inner(),
            // ),
            distributor: self.distributor,
        }
    }
}

/// A distributed static collection of edges that can be accessed by
pub struct DistributedEdges {
    edges: Rc<Vec<(u32, u32, u32)>>,
    distributor: Distributor,
}

impl DistributedEdges {
    pub fn clone(obj: &Self) -> Self {
        Self {
            edges: Rc::clone(&obj.edges),
            distributor: obj.distributor.clone(),
        }
    }

    pub fn for_each<F>(&self, mut action: F)
    where
        F: FnMut(u32, u32, u32),
    {
        for (u, v, w) in self.edges.iter() {
            action(*u, *v, *w);
        }
    }

    pub fn nodes<G: Scope, S: ExchangeData + Default>(&self, scope: &mut G) -> Stream<G, (u32, S)> {
        use timely::dataflow::operators::aggregation::aggregate::Aggregate;
        use timely::dataflow::operators::exchange::Exchange;
        use timely::dataflow::operators::to_stream::ToStream;
        use timely::dataflow::operators::Map;

        let edges = Rc::clone(&self.edges);
        (*edges) // we have to dereference the Rc pointer
            .clone()
            .to_stream(scope)
            .flat_map(|(u, v, _weight)| vec![(u, ()), (v, ())])
            .aggregate(
                |_key, _val, _agg| {
                    // do nothing, it's just for removing duplicates
                },
                |key, _agg: ()| key,
                |key| *key as u64,
            )
            .map(|u| (u, S::default()))
    }

    fn distributor(&self) -> Distributor {
        self.distributor
    }

    /// Send messages that can be aggregated along the edges
    #[allow(dead_code)]
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

        nodes
            .filter(move |(id, state)| should_send(*id, state))
            .flat_map(move |(id, state)| {
                distributor
                    .procs_node(id)
                    .map(move |p| (p, (id, state.clone())))
            })
            // Send the messages to the appropriate processors
            .exchange(|pair| pair.0 as u64)
            // Propagate to the destination
            .unary_notify(
                Pipeline,
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
