use std::collections::BTreeMap;

use datafusion_expr::Aggregate;
use pyo3::{
    types::{IntoPyDict, PyDict},
    Bound,
};

use super::{
    common::{evaluate_agg_expr, evaluate_expr, make_hash, scalar_to_py_any},
    ExecutionContext,
};

pub fn execute<'p>(aggregate: &Aggregate, ctx: &ExecutionContext<'p>) -> Vec<Bound<'p, PyDict>> {
    let input = super::execute_plan(&aggregate.input, ctx);
    let (keys, groups): (BTreeMap<u64, Vec<_>>, BTreeMap<u64, Vec<_>>) =
        input
            .into_iter()
            .fold((BTreeMap::new(), BTreeMap::new()), |mut acc, row| {
                let key: Vec<_> = aggregate
                    .group_expr
                    .iter()
                    .map(|e| evaluate_expr(e, &row))
                    .collect();

                let key_hash = make_hash(&key);
                acc.0.entry(key_hash).or_insert(key);
                acc.1.entry(key_hash).or_default().push(row);
                acc
            });

    keys.into_iter()
        .map(|(key_hash, key_values)| {
            let group = groups.get(&key_hash).unwrap();
            let values = aggregate
                .aggr_expr
                .iter()
                .map(|e| evaluate_agg_expr(e, group));

            aggregate
                .schema
                .iter()
                .map(|(_, field)| field.name())
                .zip(key_values.into_iter().chain(values))
                .map(|(name, value)| (name, scalar_to_py_any(ctx.py, &value)))
                .into_py_dict_bound(ctx.py)
        })
        .collect()
}
