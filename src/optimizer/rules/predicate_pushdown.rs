use std::rc::Rc;

use crate::{logical_plan::LogicalPlan, optimizer::OptimizerRule};

pub struct PredicatePushdown;

impl PredicatePushdown {
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for PredicatePushdown {
    fn apply(&self, plan: Rc<LogicalPlan>) -> Rc<LogicalPlan> {
        // TODO:
        plan
    }
}
