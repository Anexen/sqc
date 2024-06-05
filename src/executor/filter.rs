use datafusion_common::ScalarValue;
use datafusion_expr::Filter;
use pyo3::{types::PyDict, Bound, Python};

use super::{common::evaluate_expr, ExecutionContext};

pub fn execute<'p>(filter: &Filter, ctx: &ExecutionContext<'p>) -> Vec<Bound<'p, PyDict>> {
    let input = super::execute_plan(&filter.input, ctx);

    input
        .into_iter()
        .filter(|x| match evaluate_expr(&filter.predicate, x) {
            ScalarValue::Boolean(Some(true)) => true,
            _ => false,
        })
        .collect()
}
