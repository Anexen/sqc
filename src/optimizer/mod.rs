use std::rc::Rc;

use crate::logical_plan::LogicalPlan;

mod rules;

pub trait OptimizerRule {
    fn apply(&self, plan: Rc<LogicalPlan>) -> Rc<LogicalPlan>;
}

pub struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self {
            rules: vec![Box::new(rules::PredicatePushdown::new())],
        }
    }
}

impl Optimizer {
    pub fn optimize(&self, plan: Rc<LogicalPlan>) -> Rc<LogicalPlan> {
        let mut plan = plan;
        for rule in &self.rules {
            plan = rule.apply(plan);
        }
        plan
    }
}
