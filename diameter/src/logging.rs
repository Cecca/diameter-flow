use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::Input;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::Count;
use std::cell::RefCell;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::AddAssign;
use std::rc::Rc;
use std::time::Duration;
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
    // HeavyEdges,
    // LightIterInput(u64, u64),
    // LightIterActive(u64, u64),
    // LightUpdates(u64),
    // HeavyUpdates(u64),
    // IterationOutput(u64),
    // IterationInput(u64),
}

#[derive(Abomonation, Eq, Ord, PartialEq, PartialOrd, Clone, Copy, Hash, Default, Debug)]
pub struct CountValue(u64);

impl std::convert::From<usize> for CountValue {
    fn from(value: usize) -> CountValue {
        CountValue(value as u64)
    }
}

impl<'a> AddAssign<&'a Self> for CountValue {
    fn add_assign(&mut self, rhs: &'a Self) {
        self.0 = self.0 + rhs.0
    }
}

impl Semigroup for CountValue {
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

pub trait AsCountLogger {
    fn count_logger(&self) -> Option<Logger<(CountEvent, CountValue)>>;
}

impl<T> AsCountLogger for T
where
    T: AsWorker,
{
    fn count_logger(&self) -> Option<Logger<(CountEvent, CountValue)>> {
        self.log_register().get("counts")
    }
}

pub fn init_count_logging<A>(
    worker: &mut Worker<A>,
) -> (
    ProbeHandle<()>,
    Rc<RefCell<Option<InputHandle<(), ((CountEvent, usize), u64)>>>>,
)
where
    A: timely::communication::Allocate,
{
    use std::collections::hash_map::DefaultHasher;

    let (input, probe) = worker.dataflow::<(), _, _>(move |scope| {
        let (input, stream) = scope.new_input::<((CountEvent, usize), u64)>();
        let mut probe = ProbeHandle::new();

        stream
            .aggregate(
                |_key, val, agg| {
                    *agg += val;
                },
                |key, agg: u64| (key, agg),
                |_key| 0,
            )
            // .inspect(|(tag, value)| println!("{:?}, {:?}", tag, value))
            .probe_with(&mut probe);

        (input, probe)
    });

    let input = Rc::new(RefCell::new(Some(input)));
    let input_2 = input.clone();

    worker
        .log_register()
        .insert::<(CountEvent, CountValue), _>("counts", move |time, data| {
            for (_time_bound, worker_id, (key, value)) in data.drain(..) {
                input
                    .borrow_mut()
                    .as_mut()
                    .map(|input| input.send(((key, worker_id), value.0)));
            }
        });

    (probe, input_2)
}
