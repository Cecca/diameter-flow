use differential_dataflow::difference::Semigroup;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use std::ops::{AddAssign, Mul};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Timestamp;

#[derive(Abomonation, Copy, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Hash)]
pub struct MinSum {
    pub value: u32,
}

impl<'a> AddAssign<&'a Self> for MinSum {
    fn add_assign(&mut self, rhs: &'a Self) {
        self.value = std::cmp::min(self.value, rhs.value);
    }
}

impl Mul<Self> for MinSum {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        MinSum {
            value: self.value + rhs.value,
        }
    }
}

impl Semigroup for MinSum {
    fn is_zero(&self) -> bool {
        false
    }
}

pub trait AsMinSumCollection<G: Scope> {
    /// Turns a stream of edges into a collection of
    /// edges with deltas in the MinSum semigroup
    fn as_min_sum_collection(&self) -> Collection<G, (u32, u32), MinSum>;
}

impl<G: Scope> AsMinSumCollection<G> for Stream<G, (u32, u32)> {
    fn as_min_sum_collection(&self) -> Collection<G, (u32, u32), MinSum> {
        self.unary(Pipeline, "collection_builder", |_, _| {
            |input, output| {
                input.for_each(|t, data| {
                    let mut session = output.session(&t);
                    let mut data = data.replace(Vec::new());
                    for d in data.drain(..) {
                        session.give((d, t.time().clone(), MinSum { value: 1 }))
                    }
                });
            }
        })
        .as_collection()
    }
}

impl<G: Scope> AsMinSumCollection<G> for Stream<G, (u32, u32, u32)> {
    fn as_min_sum_collection(&self) -> Collection<G, (u32, u32), MinSum> {
        self.unary(Pipeline, "collection_builder", |_, _| {
            |input, output| {
                input.for_each(|t, data| {
                    let mut session = output.session(&t);
                    let mut data = data.replace(Vec::new());
                    for (src, dst, weight) in data.drain(..) {
                        session.give(((src, dst), t.time().clone(), MinSum { value: weight }))
                    }
                });
            }
        })
        .as_collection()
    }
}
