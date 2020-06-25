use crate::logging::*;
use bytes::*;
use timely::communication::Push;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::Bundle;
use timely::progress::Timestamp;
// use bytes::CompressedEdgesBlockSet;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::ExchangeData;

pub struct DistributedEdgesBuilder {
    edges: Rc<RefCell<Option<CompressedEdgesBlockSet>>>,
    proc_neighs: Rc<RefCell<HashMap<u32, Vec<usize>>>>,
    num_procs: usize,
}

impl DistributedEdgesBuilder {
    pub fn new<G: Scope>(
        arrangement: Matrix,
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
        let procs_ref = Rc::new(RefCell::new(HashMap::new()));
        let proc_neighs = procs_ref.clone();

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
                            CompressedEdgesBlockSet::from_files(arrangement, load_type, paths)
                                .expect("problem loading blocks"),
                        );
                        debug!("Blocks loaded");

                        let mut procs = HashSet::new();
                        edges_ref.borrow().as_ref().unwrap().for_each(|u, v, _w| {
                            procs.insert(u);
                            procs.insert(v);
                        });

                        output
                            .session(&t)
                            .give_iterator(procs.into_iter().map(|x| (x, worker_id)));
                    });
                },
            )
            .unary_notify(
                ExchangePact::new(|pair: &(u32, usize)| pair.0 as u64),
                "neighborhoods builder",
                None,
                move |input, output, notificator| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        for (u, proc) in data.into_iter() {
                            procs_ref
                                .borrow_mut()
                                .entry(u)
                                .or_insert_with(Vec::new)
                                .push(proc)
                        }
                        notificator.notify_at(t.retain());
                    });

                    notificator.for_each(|t, _, _| {
                        output.session(&t).give(());
                    });
                },
            )
            .probe();

        (
            Self {
                edges,
                proc_neighs,
                num_procs: stream.scope().peers(),
            },
            probe,
        )
    }

    pub fn build(self) -> DistributedEdges {
        let edges = self.edges.borrow_mut().take().expect("Missing edges!");
        let procs_neighs = self.proc_neighs.replace(HashMap::new());
        debug!("Loaded edges: {} bytes", edges.byte_size());
        DistributedEdges::new(Rc::new(edges), Rc::new(procs_neighs), self.num_procs)
    }
}

/// A distributed static collection of edges that can be accessed by
pub struct DistributedEdges {
    edges: Rc<CompressedEdgesBlockSet>,
    procs_neighs: Rc<HashMap<u32, Vec<usize>>>,
    num_procs: usize,
    procs_sent: RefCell<Vec<bool>>,
}

impl DistributedEdges {
    fn new(
        edges: Rc<CompressedEdgesBlockSet>,
        procs_neighs: Rc<HashMap<u32, Vec<usize>>>,
        num_procs: usize,
    ) -> Self {
        Self {
            edges,
            procs_neighs,
            num_procs,
            procs_sent: RefCell::new(vec![false; num_procs]),
        }
    }

    pub fn clone(obj: &Self) -> Self {
        Self {
            edges: Rc::clone(&obj.edges),
            procs_neighs: Rc::clone(&obj.procs_neighs),
            num_procs: obj.num_procs,
            procs_sent: obj.procs_sent.clone(),
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

        let peers = scope.peers();
        let index = scope.index();
        (0..self.edges.total_nodes())
            .filter(move |v| *v as usize % peers == index)
            .to_stream(scope)
            .map(|u| (u, S::default()))
    }

    fn for_each_processor<F: FnMut(usize)>(&self, node: u32, mut action: F) {
        let mut cnt = 0;
        for &p in self.procs_neighs[&node].iter() {
            action(p);
            cnt += 1;
        }
        // let mut procs_sent = self.procs_sent.borrow_mut();
        // for flag in procs_sent.iter_mut() {
        //     *flag = false;
        // }
        // let mut cnt = 0;
        // for block in self.edges.node_blocks(node) {
        //     let p = block as usize % self.num_procs;
        //     if !procs_sent[p] {
        //         action(p);
        //         procs_sent[p] = true;
        //         cnt += 1;
        //     }
        // }
        debug!("Sent to {} processors", cnt);
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

        let scope = nodes.scope();

        let message_batch = 1024;

        let l1 = nodes.scope().count_logger().expect("Missing logger");
        let l2 = l1.clone();

        let mut stash = HashMap::new();
        let mut node_stash = HashMap::new();
        let mut msg_stash = HashMap::new();
        let edges = Self::clone(&self);
        let edges1 = Self::clone(&self);
        let mut output_stash = HashMap::new();

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
            .unary_frontier(
                ExchangePact::new(|pair: &(usize, (u32, S))| pair.0 as u64),
                "msg_propagate",
                move |_, info| {
                    use timely::scheduling::Scheduler;
                    let activator = scope.activator_for(&info.address[..]);
                    let mut notificator = FrontierNotificator::new();
                    move |input, output| {
                        // Collect the input information
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

                        // Build the output messages
                        notificator.for_each(&[input.frontier()], |t, _| {
                            if let Some(states) = stash.remove(&t) {
                                let n_states = states.len();
                                debug!("Trying to allocate stats map for {} states", n_states);
                                let states = ArrayMap::new(states);
                                debug!("Allocated states vector for {} states", states.len());
                                let mut output_messages = BTreeMap::new();
                                let timer = std::time::Instant::now();
                                let mut cnt = 0;

                                let mut touched = std::collections::BTreeSet::new();

                                // Accumulate messages going over the edges
                                // This is the hot loop, where most of the time is spent
                                edges.for_each(|u, v, w| {
                                    cnt += 1;
                                    if let Some(state_u) = states.get(u) {
                                        touched.insert(u);
                                        if let Some(msg) = message(t.time().clone(), state_u, w) {
                                            // output_messages.push(v, msg);
                                            output_messages.entry(v).and_modify(|cur| *cur = aggregate(cur, &msg)).or_insert(msg);
                                        }
                                    }
                                    if let Some(state_v) = states.get(v) {
                                        touched.insert(v);
                                        if let Some(msg) = message(t.time().clone(), state_v, w) {
                                            // output_messages.push(u, msg);
                                            output_messages.entry(u).and_modify(|cur| *cur = aggregate(cur, &msg)).or_insert(msg);
                                        }
                                    }
                                });
                                let elapsed = timer.elapsed();
                                let throughput = cnt as f64 / elapsed.as_secs_f64();
                                debug!(
                                    "[{}] {} edges traversed in {:.2?} ({:.3?} edges/sec) with {} node states, and {} output messages. ({:.2}% actually touched by edges)",
                                    worker_id,
                                    cnt,
                                    elapsed,
                                    throughput,
                                    n_states,
                                    output_messages.len(),
                                    (touched.len() as f64 / n_states as f64) * 100.0
                                );

                                output_stash.insert(t, output_messages.into_iter().fuse().peekable());
                            }
                        });

                        // Send out the output messages a few at a time
                        for (t, iter) in output_stash.iter_mut() {
                            debug!("Outputting {} messages (out of at least {})", message_batch, iter.size_hint().0);
                            output.session(t).give_iterator(iter.take(message_batch));
                        }

                        // Clean exhausted iterators
                        output_stash.retain(|_t, iterator| iterator.peek().is_some());

                        // Re-schedule the operator, if needed
                        if !output_stash.is_empty() {
                            activator.activate();
                        }
                    }
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
                        debug!("Got {} messages", data.len());
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
#[deprecated = "better implementation available"]
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
