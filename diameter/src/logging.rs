use crate::reporter::Reporter;
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::Input as TimelyInput;
use timely::dataflow::operators::*;
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;
use timely::logging::Logger;
use timely::worker::{AsWorker, Worker};

#[derive(Abomonation, Eq, Ord, PartialEq, PartialOrd, Clone, Hash, Debug)]
pub enum CountEvent {
    Active(u32),
    Centers(u32),
}

impl CountEvent {
    pub fn iterations(&self) -> (u32, u32) {
        match self {
            Self::Active(iter) => (*iter, 0),
            Self::Centers(iter) => (*iter, 0),
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Self::Active(_) => "Active".to_owned(),
            Self::Centers(_) => "Centers".to_owned(),
        }
    }
}

pub trait AsCountLogger {
    fn count_logger(&self) -> Option<Logger<(CountEvent, u64)>>;
}

impl<T> AsCountLogger for T
where
    T: AsWorker,
{
    fn count_logger(&self) -> Option<Logger<(CountEvent, u64)>> {
        self.log_register().get("counts")
    }
}

pub fn init_count_logging<A>(
    worker: &mut Worker<A>,
    reporter: Rc<RefCell<Reporter>>,
) -> (
    ProbeHandle<()>,
    Rc<RefCell<Option<InputHandle<(), ((CountEvent, usize), u64)>>>>,
)
where
    A: timely::communication::Allocate,
{
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::Operator;

    let (input, probe) = worker.dataflow::<(), _, _>(move |scope| {
        let (input, stream) = scope.new_input::<((CountEvent, usize), u64)>();
        let mut probe = ProbeHandle::new();
        let reporting_worker = scope.index();

        stream
            .aggregate(
                |_key, val, agg| {
                    *agg += val;
                },
                |key, agg: u64| (key, agg),
                |_key| 0,
            )
            // .inspect(|(tag, value)| println!("{:?}, {:?}", tag, value))
            .unary_notify(
                Pipeline,
                "report",
                None,
                move |input, output, notificator| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        for ((counter, worker), count) in data.into_iter() {
                            reporter.borrow_mut().append_counter(counter, worker, count);
                        }
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        output.session(&t).give(());
                    });
                },
            )
            .probe_with(&mut probe);

        (input, probe)
    });

    let input = Rc::new(RefCell::new(Some(input)));
    let input_2 = input.clone();

    worker
        .log_register()
        .insert::<(CountEvent, u64), _>("counts", move |_time, data| {
            for (_time_bound, worker_id, (key, value)) in data.drain(..) {
                input
                    .borrow_mut()
                    .as_mut()
                    .map(|input| input.send(((key, worker_id), value)));
            }
        });

    (probe, input_2)
}
