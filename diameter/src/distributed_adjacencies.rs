use crate::Dataset;
use bytes::Matrix;
use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::ExchangeData;

pub struct DistributedAdjacencies {
    n: u32,
    proc_id: u32,
    num_processors: u32,
    adjacencies: Rc<HashMap<u32, Vec<(u32, u32)>>>,
}

impl DistributedAdjacencies {
    pub fn from_edges(proc_id: u32, num_processors: u32, edges: &Dataset) -> Self {
        let mut max_id = 0u32;
        let mut adjacencies = HashMap::new();
        edges.for_each(|u, v, w| {
            max_id = std::cmp::max(max_id, std::cmp::max(u, v));
            if u % num_processors == proc_id {
                adjacencies.entry(u).or_insert_with(Vec::new).push((v, w));
            }
            if v % num_processors == proc_id {
                adjacencies.entry(v).or_insert_with(Vec::new).push((u, w));
            }
        });
        Self {
            n: max_id + 1,
            proc_id,
            num_processors,
            adjacencies: Rc::new(adjacencies),
        }
    }

    pub fn clone(obj: &Self) -> Self {
        Self {
            n: obj.n,
            proc_id: obj.proc_id,
            num_processors: obj.num_processors,
            adjacencies: Rc::clone(&obj.adjacencies),
        }
    }

    pub fn nodes<G: Scope, S: ExchangeData + Default>(&self, scope: &mut G) -> Stream<G, (u32, S)> {
        use timely::dataflow::operators::*;

        // We materialize the iterator to satisfy the borrow checker
        let keys = self.adjacencies.keys().copied().collect::<Vec<u32>>();
        keys.to_stream(scope).map(|id| (id, S::default()))
    }

    #[allow(unused)]
    pub fn send<G: Scope, S: ExchangeData + Default, M: ExchangeData, P, Fm, Fa, Fu, Fun>(
        &self,
        nodes: &Stream<G, (u32, S)>,
        with_default: bool,
        should_send: P,
        message: Fm,
        aggregate: Fa,
        update: Fu,
        update_no_msg: Fun,
    ) -> Stream<G, (u32, S)>
    where
        // G::Timestamp: ToPair,
        P: Fn(G::Timestamp, &S) -> bool + 'static,
        Fm: Fn(G::Timestamp, &S, u32) -> Option<M> + 'static,
        Fa: Fn(&M, &M) -> M + Copy + 'static,
        Fu: Fn(&S, &M) -> S + 'static,
        Fun: Fn(&S) -> S + 'static,
    {
        use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
        use timely::dataflow::operators::*;

        let adjs = Rc::clone(&self.adjacencies);
        let mut message_stash = HashMap::new();
        let mut node_stash = HashMap::new();

        nodes
            .unary(Pipeline, "send messages", move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        let mut session = output.session(&t);
                        let data = data.replace(Vec::new());
                        for (id, state) in data.into_iter() {
                            if should_send(t.time().clone(), &state) {
                                for (dst, w) in adjs[&id].iter() {
                                    if let Some(msg) = message(t.time().clone(), &state, *w) {
                                        session.give((*dst, msg));
                                    }
                                }
                            }
                        }
                    })
                }
            })
            .binary_notify(
                &nodes,
                ExchangePact::new(|(id, _msg)| *id as u64),
                Pipeline,
                "exchange messages",
                None,
                move |message_input, node_input, output, notificator| {
                    notificator.for_each(|t, _, _| {
                        let mut session = output.session(&t);
                        let msgs = message_stash.remove(t.time()).unwrap_or_else(HashMap::new);
                        let mut nodes = node_stash.remove(t.time()).unwrap_or_else(HashMap::new);
                        for (id, message) in msgs.into_iter() {
                            if let Some(state) = nodes.remove(&id) {
                                session.give((id, update(&state, &message)));
                            } else if with_default {
                                let state = Default::default();
                                session.give((id, update(&state, &message)));
                            }
                        }
                        // Exhaust un-messaged nodes
                        for (id, state) in nodes.drain() {
                            session.give((id, update_no_msg(&state)));
                        }
                    });

                    message_input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        let mut stash = message_stash
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new);
                        for (id, msg) in data.into_iter() {
                            stash
                                .entry(id)
                                .and_modify(|acc| *acc = aggregate(acc, &msg))
                                .or_insert(msg);
                        }
                        notificator.notify_at(t.retain());
                    });

                    node_input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        node_stash
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new)
                            .extend(data.into_iter());
                        notificator.notify_at(t.retain());
                    });
                },
            )
    }

    /// Brings together the states of the endpoints of each edge with the edge itself.
    #[allow(unused)]
    pub fn triplets<G: Scope, S: ExchangeData, F, O>(
        &self,
        nodes: &Stream<G, (u32, S)>,
        action: F,
    ) -> Stream<G, O>
    where
        F: Fn(((u32, S), (u32, S), u32)) -> Option<O> + 'static,
        O: ExchangeData,
    {
        use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
        use timely::dataflow::operators::*;

        let adjs = Rc::clone(&self.adjacencies);
        let adjs2 = Rc::clone(&self.adjacencies);
        let mut processor_targets = Vec::new();
        let mut message_stash = HashMap::new();
        let mut node_stash: HashMap<G::Timestamp, Vec<(u32, S)>> = HashMap::new();

        let peers = nodes.scope().peers() as u32;

        nodes
            .unary(Pipeline, "send states", move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        let mut session = output.session(&t);
                        let data = data.replace(Vec::new());
                        for (id, state) in data.into_iter() {
                            processor_targets.clear();
                            processor_targets.extend(adjs[&id].iter().map(|pair| pair.0 % peers));
                            processor_targets.sort();
                            processor_targets.dedup();
                            for proc in &processor_targets {
                                session.give((*proc, (id, state.clone())));
                            }
                        }
                    })
                }
            })
            .binary_notify(
                &nodes,
                ExchangePact::new(|(id, _msg)| *id as u64),
                Pipeline,
                "exchange messages",
                None,
                move |message_input, node_input, output, notificator| {
                    notificator.for_each(|t, _, _| {
                        let mut session = output.session(&t);
                        let other_states: HashMap<u32, S> =
                            message_stash.remove(t.time()).expect("missing state");
                        let mut nodes = node_stash.remove(t.time()).expect("missing nodes");
                        for (src, src_state) in nodes.into_iter() {
                            for (dst, w) in adjs2[&src].iter() {
                                let dst_state = &other_states[dst];
                                if let Some(res) = action((
                                    (src, src_state.clone()),
                                    (*dst, dst_state.clone()),
                                    *w,
                                )) {
                                    session.give(res);
                                }
                            }
                        }
                    });

                    message_input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        let mut stash = message_stash
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new);
                        for (_proc, (id, msg)) in data.into_iter() {
                            stash.insert(id, msg);
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
                },
            )
    }
}
