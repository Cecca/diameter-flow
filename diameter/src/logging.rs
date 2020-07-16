use crate::reporter::Reporter;
use env_logger::Builder;
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::time::*;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::Input as TimelyInput;
use timely::dataflow::operators::*;
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;
use timely::logging::Logger;
use timely::order::Product;
use timely::worker::{AsWorker, Worker};

pub fn get_hostname() -> String {
    let output = std::process::Command::new("hostname")
        .output()
        .expect("Failed to run the hostname command");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

// This is the place to hook into if we want to use syslog
pub fn init_logging(verbose: bool) {
    use std::io::Write;

    let hostname = get_hostname();
    let start = Instant::now();
    let mut builder = Builder::from_default_env();
    builder.format(move |buf, record| {
        writeln!(
            buf,
            "[{}, {:?}] {:.2?} - {}: {}",
            hostname,
            std::thread::current().id(),
            Instant::now() - start,
            record.level(),
            record.args()
        )
    });
    if verbose {
        builder.filter_level(log::LevelFilter::Debug);
    } else {
        builder.filter_level(log::LevelFilter::Info);
    }
    builder.init();
    // log_panics::init();
}

// #[derive(Abomonation, Eq, Ord, PartialEq, PartialOrd, Clone, Hash, Debug)]
// pub enum CountEvent {
//     Active(u32),
//     Centers(u32),
//     Uncovered(u32),
//     LoadStateExchange(u32, u32),
//     LoadMessageExchange(u32, u32),
//     UpdatedNodes(u32, u32),
//     RadiusHist(u32, u32),
// }

// impl CountEvent {
//     pub fn iterations(&self) -> (u32, u32) {
//         match self {
//             Self::Active(iter) => (*iter, 0),
//             Self::Centers(iter) => (*iter, 0),
//             Self::Uncovered(iter) => (*iter, 0),
//             Self::LoadMessageExchange(outer, inner) => (*outer, *inner),
//             Self::LoadStateExchange(outer, inner) => (*outer, *inner),
//             Self::UpdatedNodes(outer, inner) => (*outer, *inner),
//             Self::RadiusHist(iter, _radius) => (*iter, 0),
//         }
//     }

//     pub fn as_string(&self) -> String {
//         match self {
//             Self::Active(_) => "Active".to_owned(),
//             Self::Uncovered(_) => "Uncovered".to_owned(),
//             Self::Centers(_) => "Centers".to_owned(),
//             Self::LoadMessageExchange(_, _) => "LoadMessageExchange".to_owned(),
//             Self::LoadStateExchange(_, _) => "LoadStateExchange".to_owned(),
//             Self::UpdatedNodes(_, _) => "UpdatedNodes".to_owned(),
//             Self::RadiusHist(_, radius) => format!("RadiusHist_{}", radius),
//         }
//     }

//     pub fn load_state_exchange<T: ToPair>(t: T) -> Self {
//         let (outer, inner) = t.to_pair();
//         Self::LoadStateExchange(outer, inner)
//     }

//     pub fn load_message_exchange<T: ToPair>(t: T) -> Self {
//         let (outer, inner) = t.to_pair();
//         Self::LoadMessageExchange(outer, inner)
//     }

//     pub fn updated_nodes<T: ToPair>(t: T) -> Self {
//         let (outer, inner) = t.to_pair();
//         Self::UpdatedNodes(outer, inner)
//     }
// }

// pub trait ToPair {
//     fn to_pair(&self) -> (u32, u32);
// }

// impl ToPair for Product<usize, u32> {
//     fn to_pair(&self) -> (u32, u32) {
//         (self.inner, 0)
//     }
// }

// impl ToPair for Product<Product<usize, u32>, u32> {
//     fn to_pair(&self) -> (u32, u32) {
//         (self.outer.inner, self.inner)
//     }
// }

// pub trait AsCountLogger {
//     fn count_logger(&self) -> Option<Logger<(CountEvent, u64)>>;
// }

// impl<T> AsCountLogger for T
// where
//     T: AsWorker,
// {
//     fn count_logger(&self) -> Option<Logger<(CountEvent, u64)>> {
//         self.log_register().get("counts")
//     }
// }

// pub fn init_count_logging<A>(
//     worker: &mut Worker<A>,
//     reporter: Rc<RefCell<Reporter>>,
// ) -> (
//     ProbeHandle<()>,
//     Rc<RefCell<Option<InputHandle<(), (CountEvent, u64)>>>>,
// )
// where
//     A: timely::communication::Allocate,
// {
//     use timely::dataflow::channels::pact::Pipeline;

//     let (input, probe) = worker.dataflow::<(), _, _>(move |scope| {
//         let (input, stream) = scope.new_input::<(CountEvent, u64)>();
//         let mut probe = ProbeHandle::new();
//         let _reporting_worker = scope.index();

//         stream
//             .aggregate(
//                 |_key, val, agg| {
//                     *agg += val;
//                 },
//                 |key, agg: u64| (key, agg),
//                 |_key| 0,
//             )
//             // .inspect(|(tag, value)| info!("{:?}, {:?}", tag, value))
//             .unary_notify(
//                 Pipeline,
//                 "report",
//                 None,
//                 move |input, output, notificator| {
//                     input.for_each(|t, data| {
//                         let data = data.replace(Vec::new());
//                         for (counter, count) in data.into_iter() {
//                             reporter.borrow_mut().append_counter(counter, count);
//                         }
//                         notificator.notify_at(t.retain());
//                     });
//                     notificator.for_each(|t, _, _| {
//                         output.session(&t).give(());
//                     });
//                 },
//             )
//             .probe_with(&mut probe);

//         (input, probe)
//     });

//     let input = Rc::new(RefCell::new(Some(input)));
//     let input_2 = input.clone();

//     worker
//         .log_register()
//         .insert::<(CountEvent, u64), _>("counts", move |_time, data| {
//             for (_time_bound, worker_id, (key, value)) in data.drain(..) {
//                 input
//                     .borrow_mut()
//                     .as_mut()
//                     .map(|input| input.send((key, value)));
//             }
//         });

//     (probe, input_2)
// }
