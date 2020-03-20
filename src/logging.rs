use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::Input;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::Count;
use std::cell::RefCell;
use std::ops::AddAssign;
use std::rc::Rc;
use std::time::Duration;
use timely::dataflow::operators::*;
use timely::dataflow::ProbeHandle;
use timely::logging::Logger;
use timely::worker::{AsWorker, Worker};

#[derive(Abomonation, Eq, Ord, PartialEq, PartialOrd, Clone, Hash, Debug)]
pub enum CountEvent {
    LightEdges,
    HeavyEdges,
    LightIterInput(u64, u64),
    LightIterActive(u64, u64),
    LightUpdates(u64),
    HeavyUpdates(u64),
    IterationOutput(u64),
    IterationInput(u64),
}

#[derive(Abomonation, Eq, Ord, PartialEq, PartialOrd, Clone, Hash, Debug)]
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
    ProbeHandle<Duration>,
    Rc<RefCell<Option<InputSession<Duration, CountEvent, CountValue>>>>,
)
where
    A: timely::communication::Allocate,
{
    let (input, probe) = worker.dataflow::<Duration, _, _>(move |scope| {
        let (input, stream) = scope.new_collection::<CountEvent, CountValue>();
        let mut probe = ProbeHandle::new();

        stream
            .count()
            .consolidate()
            .inner
            .exchange(|_| 0)
            .inspect(|((_tag, _value), _time, _diff)| {
                // println!("[{:?}] {:?} {:?} ({:?})", time, tag, value.0, diff)
            })
            .probe_with(&mut probe);

        (input, probe)
    });

    let input = Rc::new(RefCell::new(Some(input)));
    let input_2 = input.clone();

    worker
        .log_register()
        .insert::<(CountEvent, CountValue), _>("counts", move |time, data| {
            for (time_bound, _worker_id, (key, value)) in data.drain(..) {
                // println!("{:?} <? {:?}", time_bound, time);
                // Round the duration one second up
                let time = Duration::from_secs(time.as_secs() + 1);
                input
                    .borrow_mut()
                    .as_mut()
                    .map(|input| input.update_at(key, time, value));

                input
                    .borrow_mut()
                    .as_mut()
                    .map(|input| input.advance_to(time_bound));
            }
            input.borrow_mut().as_mut().map(|input| input.flush());
        });

    (probe, input_2)
}
