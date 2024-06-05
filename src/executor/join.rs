use std::collections::BTreeMap;

use datafusion_expr::{Aggregate, Expr, Join, JoinType};
use pyo3::{
    types::{IntoPyDict, PyDict, PyDictMethods},
    Bound,
};

use super::{
    common::{evaluate_agg_expr, evaluate_expr, make_hash, scalar_to_py_any},
    ExecutionContext,
};

pub fn execute<'p>(join: &Join, ctx: &ExecutionContext<'p>) -> Vec<Bound<'p, PyDict>> {
    if !matches!(join.join_type, JoinType::Inner) {
        unimplemented!("Join type {} is not implemented", join.join_type);
    }
    let join_filter = match &join.filter {
        Some(Expr::BinaryExpr(binary_expr)) => binary_expr,
        _ => unimplemented!("join_filter: {:?}", join.filter),
    };

    let left = super::execute_plan(&join.left, ctx);
    let right = super::execute_plan(&join.right, ctx);

    let hash_table: BTreeMap<_, _> = right
        .into_iter()
        .map(|x| {
            let value = evaluate_expr(&join_filter.right, &x);
            (make_hash(value), x)
        })
        .collect();

    left.into_iter()
        .filter_map(|x| {
            let value = evaluate_expr(&join_filter.left, &x);
            let key = make_hash(value);
            let result = hash_table.get(&key)?.copy().unwrap();
            result.update(x.as_mapping()).unwrap();
            Some(result)
        })
        .collect()
}
