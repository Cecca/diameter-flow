use crate::logging::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Timestamp;
use timely::Data;

pub trait BranchAll<G: Scope, D: Data> {
    /// The right branch contains nodes if and only if
    /// the count of nodes matching the condition is strictly
    /// greater than the given threshold
    fn branch_all<P>(
        &self,
        name: &'static str,
        condition: P,
        threshold: usize,
    ) -> (Stream<G, D>, Stream<G, D>)
    where
        P: Fn(G::Timestamp, &D) -> bool + 'static;
}

impl<G: Scope, D: Data> BranchAll<G, D> for Stream<G, D> {
    fn branch_all<P>(
        &self,
        name: &'static str,
        condition: P,
        threshold: usize,
    ) -> (Stream<G, D>, Stream<G, D>)
    where
        P: Fn(G::Timestamp, &D) -> bool + 'static,
    {
        use timely::dataflow::operators::*;

        // let logger = self.scope().count_logger().expect("missing logger");
        let worker_id = self.scope().index();
        let stash = Rc::new(RefCell::new(HashMap::new()));
        let stash2 = Rc::clone(&stash);

        let matching_counts = self
            .unary(Pipeline, "count updated", move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        let cnt = data
                            .iter()
                            .filter(|x| condition(t.time().clone(), x))
                            .count();
                        // logger.log((CountEvent::updated_nodes(t.time().clone()), cnt as u64));
                        output.session(&t).give(cnt);
                        stash
                            .borrow_mut()
                            .entry(t.time().clone())
                            .or_insert_with(Vec::new)
                            .extend(data.into_iter());
                    })
                }
            })
            .broadcast();

        let mut stash_counts = HashMap::new();
        let (stable, some_matches) = matching_counts
            .unary_notify(
                Pipeline,
                "splitter",
                None,
                move |input, output, notificator| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        let total: usize = data.into_iter().sum();
                        stash_counts
                            .entry(t.time().clone())
                            .and_modify(|c| *c += total)
                            .or_insert(total);
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|time, _, _| {
                        if let Some(matching_count) = stash_counts.remove(&time) {
                            let branch = matching_count > threshold;
                            if worker_id == 0 {
                                info!(
                                    ">> {} nodes passing predicate {} at time {:?} (branching out? {})",
                                    matching_count,
                                    name,
                                    time.time(),
                                    !branch
                                );
                            }
                            if let Some(nodes) = stash2.borrow_mut().remove(&time) {
                                output
                                    .session(&time)
                                    .give_iterator(nodes.into_iter().map(|x| (branch, x)));
                            }
                        }
                    });
                },
            )
            .branch(|_t, (some_updated, _pair)| *some_updated);

        (stable.map(|pair| pair.1), some_matches.map(|pair| pair.1))
    }
}

pub trait MapTimed<G: Scope, D: Data> {
    fn map_timed<O: Data, F: Fn(G::Timestamp, D) -> O + 'static>(&self, f: F) -> Stream<G, O>;
}

impl<G: Scope, D: Data> MapTimed<G, D> for Stream<G, D> {
    fn map_timed<O: Data, F: Fn(G::Timestamp, D) -> O + 'static>(&self, f: F) -> Stream<G, O> {
        use timely::dataflow::operators::Operator;

        self.unary(Pipeline, "map_timed", move |_, _| {
            move |input, output| {
                input.for_each(|t, data| {
                    let time = t.time().clone();
                    let data = data.replace(Vec::new());
                    output
                        .session(&t)
                        .give_iterator(data.into_iter().map(|d| f(time.clone(), d)));
                });
            }
        })
    }
}

pub trait CollectSingle<T: Timestamp> {
    fn collect_single(&self) -> (Rc<RefCell<Option<u32>>>, ProbeHandle<T>);
}

impl<G: Scope> CollectSingle<G::Timestamp> for Stream<G, u32> {
    fn collect_single(&self) -> (Rc<RefCell<Option<u32>>>, ProbeHandle<G::Timestamp>) {
        use timely::dataflow::operators::{Operator, Probe};

        let result = Rc::new(RefCell::new(None));
        let result_ref = Rc::clone(&result);
        let probe = self
            .unary(Pipeline, "collect_single", move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        result_ref.borrow_mut().replace(data[0]);
                        output.session(&t).give(());
                    });
                }
            })
            .probe();

        (result, probe)
    }
}
