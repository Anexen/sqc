use std::collections::HashMap;

use datafusion_expr::{table_scan, LogicalPlan};

use pyo3::{types::PyDict, Bound, Python};

mod aggregate;
mod common;
mod filter;
mod join;
mod projection;
mod table_scan;

pub struct ExecutionContext<'p> {
    pub tables: HashMap<String, Vec<Bound<'p, PyDict>>>,
    pub py: Python<'p>,
}

impl<'p> ExecutionContext<'p> {
    pub fn new(py: Python<'p>) -> Self {
        Self {
            tables: HashMap::new(),
            py,
        }
    }
}

pub fn execute_plan<'p>(plan: &LogicalPlan, ctx: &ExecutionContext<'p>) -> Vec<Bound<'p, PyDict>> {
    match plan {
        LogicalPlan::TableScan(table_scan) => table_scan::execute(table_scan, ctx),
        LogicalPlan::Projection(projection) => projection::execute(projection, ctx),
        LogicalPlan::Filter(filter) => filter::execute(filter, ctx),
        LogicalPlan::Aggregate(aggregate) => aggregate::execute(aggregate, ctx),
        LogicalPlan::Join(join) => join::execute(join, ctx),
        LogicalPlan::EmptyRelation(empty_relation) => {
            if empty_relation.produce_one_row {
                vec![PyDict::new_bound(ctx.py)]
            } else {
                vec![]
            }
        },
        _ => unimplemented!("{:?}", plan),
    }
}
