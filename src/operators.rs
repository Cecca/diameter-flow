use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;

pub trait BranchAll<G: Scope, D: Data> {
    fn branch_all<P>(&self, condition: P) -> (Stream<G, D>, Stream<G, D>)
    where
        P: Fn(&D) -> bool + 'static;
}

impl<G: Scope, D: Data> BranchAll<G, D> for Stream<G, D> {
    fn branch_all<P>(&self, condition: P) -> (Stream<G, D>, Stream<G, D>)
    where
        P: Fn(&D) -> bool + 'static,
    {
        
        use timely::dataflow::operators::*;

        let stash = Rc::new(RefCell::new(HashMap::new()));
        let stash2 = Rc::clone(&stash);

        let updated_counts = self
            .unary(Pipeline, "count updated", move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        output
                            .session(&t)
                            .give(data.iter().filter(|x| condition(x)).count());
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
        let (stable, some_updated) = updated_counts
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
                        if let Some(updated_count) = stash_counts.remove(&time) {
                            let branch = updated_count > 0;
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

        (stable.map(|pair| pair.1), some_updated.map(|pair| pair.1))
    }
}
