use crate::logging::*;
use bytes::*;
use timely::communication::Push;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::Bundle;
use timely::progress::Timestamp;
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
        load_type: LoadType,
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
                        debug!("added paths to the stash");
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        let paths = paths_stash.iter().map(|(edges_path, weights_path)| {
                            (
                                PathBuf::from(edges_path),
                                weights_path.as_ref().map(|s| PathBuf::from(s)),
                            )
                        });
                        debug!("Start loading blocks");
                        edges_ref.borrow_mut().replace(
                            CompressedEdgesBlockSet::from_files(load_type, paths)
                                .expect("problem loading blocks"),
                        );
                        debug!("Blocks loaded");

                        // exchange the edges to build processor targets
                        debug!("Get the nodes this processor is responsible for");
                        let mut nodes = HashSet::new();
                        edges_ref.borrow().iter().for_each(|edges| {
                            edges.for_each(|u, v, _| {
                                nodes.insert(u);
                                nodes.insert(v);
                            });
                        });
                        debug!("The edges on this processor touch {} nodes", nodes.len());
                        output
                            .session(&t)
                            .give_iterator(nodes.into_iter().take(100).map(|u| (u, worker_id)));
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
        // let mut histogram = std::collections::BTreeMap::new();
        // for (_, procs) in nodes_processors.iter() {
        //     let len = procs.len();
        //     histogram
        //         .entry(len)
        //         .and_modify(|c| *c += len)
        //         .or_insert(len);
        // }
        // info!("Distribution of destination sets {:#?}", histogram);
        debug!("Loaded edges: {} bytes", edges.byte_size());
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
        self.edges.for_each(action);
        // info!("time to iterate over all the edges {:?}", timer.elapsed());
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
                            let timer = std::time::Instant::now();
                            let mut out = output.session(&t);

                            let mut cnt = 0;
                            // Accumulate messages going over the edges
                            edges.for_each(|u, v, w| {
                                let state_u = states.get(&u).expect("missing state for u");
                                let state_v = states.get(&v).expect("missing state for v");
                                if let Some(o) =
                                    action(((u, state_u.clone()), (v, state_v.clone()), w))
                                {
                                    cnt += 1;
                                    out.give(o);
                                }
                            });
                            if cnt == 0 {
                                warn!("no output triplets");
                            }
                        }
                    });
                },
            )
    }

    /// Send messages that can be aggregated along the edges
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
        G::Timestamp: ToPair,
        P: Fn(G::Timestamp, &S) -> bool + 'static,
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

        let worker_id = nodes.scope().index();

        nodes
            // Send states if needed
            .unary(Pipeline, "send states", move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        let mut session = output.session(&t);
                        let data = data.replace(Vec::new());
                        for (id, state) in data.into_iter() {
                            if should_send(t.time().clone(), &state) {
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
                        stash
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new)
                            .extend(data.into_iter().map(|pair| pair.1));
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        if let Some(states) = stash.remove(&t) {
                            let n_states = states.len();
                            debug!("Trying to allocate stats map for {} states", n_states);
                            let states = ArrayMap::new(states);
                            debug!("Allocated states vector for {} states", states.len());
                            let mut output_messages = MessageBuffer::with_capacity(
                                4_000_000,
                                aggregate,
                                output.session(&t),
                            );
                            let timer = std::time::Instant::now();
                            let mut cnt = 0;
                            let mut cnt_out = 0;

                            // Accumulate messages going over the edges
                            // This is the hot loop, where most of the time is spent
                            edges.for_each(|u, v, w| {
                                cnt += 1;
                                if let Some(state_u) = states.get(u) {
                                    if let Some(msg) = message(t.time().clone(), state_u, w) {
                                        cnt_out += 1;
                                        output_messages.push(v, msg);
                                    }
                                }
                                if let Some(state_v) = states.get(v) {
                                    if let Some(msg) = message(t.time().clone(), state_v, w) {
                                        cnt_out += 1;
                                        output_messages.push(u, msg);
                                    }
                                }
                            });
                            let elapsed = timer.elapsed();
                            let throughput = cnt as f64 / elapsed.as_secs_f64();
                            debug!(
                                "[{}] {} edges traversed in {:.2?} ({:.3?} edges/sec) with {} node states, and {} output messages.",
                                worker_id,
                                cnt,
                                elapsed,
                                throughput,
                                n_states,
                                cnt_out
                            );
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
                            .or_insert_with(HashMap::new)
                            .extend(data.into_iter());
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        let mut session = output.session(&t);
                        // For each node, update the state with the received, message, if any
                        let msgs = msg_stash.remove(t.time()).unwrap_or_else(HashMap::new);
                        let mut nodes = node_stash.remove(t.time()).unwrap_or_else(HashMap::new);
                        let mut cnt_messaged = 0;
                        let mut cnt_no_messaged = 0;
                        for (id, message) in msgs.into_iter() {
                            if let Some(state) = nodes.remove(&id) {
                                session.give((id, update(&state, &message)));
                                cnt_messaged += 1;
                            } else if with_default {
                                let state = Default::default();
                                session.give((id, update(&state, &message)));
                                cnt_messaged += 1;
                            }
                        }
                        // Exhaust un-messaged nodes
                        for (id, state) in nodes.drain() {
                            session.give((id, update_no_msg(&state)));
                            cnt_no_messaged += 1;
                        }
                    });
                },
            )
    }
}

/// Buffers messages and aggregates ones with duplicate keys
struct MessageBuffer<'a, T, M, F, P>
where
    T: Timestamp,
    M: ExchangeData,
    F: Fn(&M, &M) -> M + 'static,
    P: Push<Bundle<T, (u32, M)>>,
{
    capacity: usize,
    buffer: Vec<(u32, M)>,
    merger: F,
    session: Session<'a, T, (u32, M), P>,
}

impl<'a, T, M, F, P> MessageBuffer<'a, T, M, F, P>
where
    T: Timestamp,
    M: ExchangeData,
    F: Fn(&M, &M) -> M + 'static,
    P: Push<Bundle<T, (u32, M)>>,
{
    fn with_capacity(capacity: usize, merger: F, session: Session<'a, T, (u32, M), P>) -> Self {
        Self {
            capacity,
            buffer: Vec::with_capacity(capacity),
            merger,
            session,
        }
    }

    fn push(&mut self, dest: u32, msg: M) {
        self.buffer.push((dest, msg));
        if self.buffer.len() == self.capacity {
            self.flush()
        }
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.buffer.sort_by_key(|pair| pair.0);
        {
            let mut iter = self.buffer.drain(..);
            let mut current_message = iter.next().expect("called flush on empty vector");
            loop {
                if let Some(msg) = iter.next() {
                    if msg.0 == current_message.0 {
                        current_message.1 = (self.merger)(&current_message.1, &msg.1);
                    } else {
                        self.session.give(current_message);
                        current_message = msg;
                    }
                } else {
                    self.session.give(current_message);
                    break;
                }
            }
        }
        assert!(self.buffer.is_empty());
    }
}

impl<'a, T, M, F, P> Drop for MessageBuffer<'a, T, M, F, P>
where
    T: Timestamp,
    M: ExchangeData,
    F: Fn(&M, &M) -> M + 'static,
    P: Push<Bundle<T, (u32, M)>>,
{
    fn drop(&mut self) {
        self.flush()
    }
}

struct ArrayMap<S> {
    data: Vec<(u32, S)>,
}

impl<S> ArrayMap<S> {
    fn new<I: IntoIterator<Item = (u32, S)>>(iter: I) -> Self {
        let mut data: Vec<(u32, S)> = iter.into_iter().collect();
        data.sort_by_key(|pair| pair.0);
        Self { data }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn get(&self, key: u32) -> Option<&S> {
        match self.data.binary_search_by_key(&key, |pair| pair.0) {
            Ok(index) => Some(&self.data[index].1),
            Err(_) => None,
        }
    }
}
