use datafusion_common::ScalarValue;
use datafusion_expr::TableScan;
use pyo3::{types::PyDict, Bound};

use super::{common::evaluate_expr, ExecutionContext};

pub fn execute<'p>(table_scan: &TableScan, ctx: &ExecutionContext<'p>) -> Vec<Bound<'p, PyDict>> {
    let values = ctx.tables.get(table_scan.table_name.table()).unwrap();
    values
        .into_iter()
        .filter(|x| {
            table_scan
                .filters
                .iter()
                .all(|f| match evaluate_expr(f, x) {
                    ScalarValue::Boolean(Some(true)) => true,
                    _ => false,
                })
        })
        .cloned()
        .collect()
}
