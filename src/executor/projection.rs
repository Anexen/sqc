use datafusion_expr::{LogicalPlan, Projection};
use pyo3::types::IntoPyDict;
use pyo3::{types::PyDict, Bound, Python};

use super::common::{evaluate_expr, scalar_to_py_any};
use super::ExecutionContext;

pub fn execute<'p>(projection: &Projection, ctx: &ExecutionContext<'p>) -> Vec<Bound<'p, PyDict>> {
    let input = super::execute_plan(&projection.input, ctx);
    input
        .into_iter()
        .map(|x| {
            projection
                .schema
                .iter()
                .map(|(_, field)| field.name())
                .zip(projection.expr.iter())
                .map(|(key, expr)| {
                    let value = scalar_to_py_any(ctx.py, &evaluate_expr(&expr, &x));
                    (key, value)
                })
                .collect::<Vec<_>>()
                .into_py_dict_bound(ctx.py)
        })
        .collect()
}
