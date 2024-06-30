use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use derive_more::Display;
use indexmap::IndexMap;
use itertools::Itertools;
use pyo3::{prelude::*, types::*};

use crate::logical_plan::*;
use crate::stream::*;

type ExecResult<T> = Result<T, ExecError>;
type Stream<'p> = crate::stream::Stream<'p, ExecError>;

#[derive(Debug, Display)]
pub enum ExecError {
    TableNotFound(TableReference),
    ColumnNotFound(Arc<String>),
    AmbiguousColumn(Arc<String>),
    RuntimeError(Arc<String>),
}

impl From<PyErr> for ExecError {
    fn from(value: PyErr) -> Self {
        Self::RuntimeError(value.to_string().into())
    }
}

pub struct ExecutionContext {
    tables: HashMap<String, PyObject>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, name: &str, data: PyObject) {
        self.tables.insert(name.to_string(), data);
    }

    pub fn scan_table<'p>(
        &self,
        py: Python<'p>,
        table_name: &'p TableReference,
    ) -> ExecResult<Stream<'p>> {
        let inner = self
            .tables
            .get(table_name.0.as_ref())
            .cloned()
            .ok_or_else(|| ExecError::TableNotFound(table_name.clone()))?;

        let stream = inner.bind(py).iter()?.map(|obj| match obj {
            Ok(obj) => obj
                .downcast::<PyDict>()
                .map_err(|e| ExecError::RuntimeError(e.to_string().into()))
                .map(|row| {
                    let data = row
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.unbind()))
                        .collect();
                    [(table_name.clone(), data)].into()
                }),
            Err(e) => Err(ExecError::RuntimeError(e.to_string().into())),
        });

        Ok(Stream::new(stream))
    }
}

pub fn execute_plan<'p>(
    py: Python<'p>,
    plan: &'p LogicalPlan,
    ctx: &'p mut ExecutionContext,
) -> ExecResult<Stream<'p>> {
    plan.execute(py, ctx)
}

trait Exec<'p> {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>>;
}

impl<'p> Exec<'p> for LogicalPlan {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>> {
        match self {
            LogicalPlan::Projection(v) => v.execute(py, ctx),
            LogicalPlan::TableScan(v) => v.execute(py, ctx),
            LogicalPlan::Filter(v) => v.execute(py, ctx),
            LogicalPlan::EmptyRelation(v) => v.execute(py, ctx),
            LogicalPlan::SubqueryAlias(v) => v.execute(py, ctx),
            LogicalPlan::Join(v) => v.execute(py, ctx),
            LogicalPlan::Sort(v) => v.execute(py, ctx),
            LogicalPlan::Limit(v) => v.execute(py, ctx),
        }
    }
}

impl<'p> Exec<'p> for Projection {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>> {
        let data = self.input.execute(py, ctx)?.map_ok(move |row| {
            let data = self.expr.iter().flat_map(|(name, expr)| match expr {
                Expr::Wildcard(wildcard) => match wildcard.table.as_ref() {
                    Some(table_ref) => {
                        let part = row
                            .get(table_ref)
                            .ok_or_else(|| ExecError::TableNotFound(table_ref.clone()))
                            .unwrap();

                        part.clone().into_iter().collect_vec()
                    }
                    None => row
                        .values()
                        .flat_map(|x| x.clone().into_iter())
                        .collect_vec(),
                },
                _ => vec![(
                    name.clone(),
                    evaluate_scalar_expr(py, expr, &row).unwrap().into_py(py),
                )],
            });

            let part = data.collect();
            [(TableReference::default(), part)].into()
        });

        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for TableScan {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>> {
        let stream = ctx
            .scan_table(py, &self.table_name)?
            .filter_then(move |row| {
                self.filters
                    .iter()
                    .map(Ok)
                    .and_all(|expr| evaluate_predicate(py, expr, row))
            });

        Ok(Stream::new(stream))
    }
}

impl<'p> Exec<'p> for Sort {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>> {
        let mut input: Vec<_> = self.input.execute(py, ctx)?.try_collect()?;

        let mut indices: Vec<_> = input
            .iter()
            .map(|row| -> ExecResult<Vec<PyObject>> {
                self.expr
                    .iter()
                    .map(|e| evaluate_scalar_expr(py, &e.expr, row))
                    .map_ok(|e| e.into_py(py))
                    .try_collect()
            })
            .enumerate()
            .map(|(i, v)| v.map(move |k| (k, i)))
            .try_collect()?;

        indices.sort_by(|(a, _), (b, _)| {
            a.iter().zip(b.iter()).enumerate().fold(
                std::cmp::Ordering::Equal,
                |acc, (i, (a, b))| {
                    let ordering = acc.then(a.bind(py).compare(b.bind(py)).unwrap());
                    if self.expr[i].asc {
                        ordering
                    } else {
                        ordering.reverse()
                    }
                },
            )
        });

        for i in 0..input.len() {
            let mut index = indices[i].1;
            while index < i {
                index = indices[index].1;
            }
            indices[i].1 = index;
            input.swap(i, index);
        }

        Ok(Stream::new(input.into_iter().map(Ok)))
    }
}

impl<'p> Exec<'p> for Join {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>> {
        let left: Vec<_> = self.left.execute(py, ctx)?.collect();
        let right: Vec<_> = self.right.execute(py, ctx)?.collect();

        let hash_table: BTreeMap<_, _> = right
            .into_iter()
            .map_then(|x| {
                let join_keys = self
                    .on
                    .iter()
                    .filter_map(|(_, right_expr)| {
                        evaluate_scalar_expr(py, right_expr, &x).unwrap().0
                    })
                    .collect_vec();

                let value = PyTuple::new_bound(py, join_keys);
                let key = value.hash()?;
                Ok((key, x))
            })
            .try_collect()?;

        let stream = left.into_iter().filter_map_ok(move |x| {
            let join_keys = self
                .on
                .iter()
                .filter_map(|(left_expr, _)| evaluate_scalar_expr(py, left_expr, &x).unwrap().0)
                .collect_vec();

            let value = PyTuple::new_bound(py, join_keys);
            let key = value.hash().unwrap();

            let mut result = hash_table.get(&key)?.clone();
            for (table_ref, data) in x.into_iter() {
                result
                    .entry(table_ref)
                    .or_insert(IndexMap::new())
                    .extend(data);
            }
            Some(result)
        });

        Ok(Stream::new(stream))
    }
}

impl<'p> Exec<'p> for SubqueryAlias {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>> {
        let data = self
            .input
            .execute(py, ctx)?
            .map_ok(|row| row.into_values().map(|v| (self.alias.clone(), v)).collect());
        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for Filter {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>> {
        let data = self
            .input
            .execute(py, ctx)?
            .filter_then(move |row| evaluate_predicate(py, &self.predicate, row));

        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for EmptyRelation {
    fn execute(
        &'p self,
        _py: Python<'p>,
        _ctx: &'p mut ExecutionContext,
    ) -> ExecResult<Stream<'p>> {
        let table_ref = TableReference::default();
        let row = [(table_ref, IndexMap::new())].into();
        let data = vec![Ok(row)];
        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for Limit {
    fn execute(&'p self, py: Python<'p>, ctx: &'p mut ExecutionContext) -> ExecResult<Stream<'p>> {
        let limit = evaluate_scalar_expr(py, &self.limit, &Default::default())?
            .into_bound(py)
            .extract::<usize>()?;

        let offset = self
            .offset
            .as_ref()
            .map(|offset| {
                evaluate_scalar_expr(py, offset, &Default::default())?
                    .into_bound(py)
                    .extract::<usize>()
            })
            .transpose()?
            .unwrap_or(0);

        let stream = self.input.execute(py, ctx)?.skip(offset).take(limit);

        Ok(Stream::new(stream))
    }
}

fn evaluate_predicate(py: Python, predicate: &Expr, row: &RowInner) -> ExecResult<bool> {
    evaluate_scalar_expr(py, predicate, row)
        .and_then(|x| x.into_bound(py).is_truthy().map_err(|e| e.into()))
}

fn evaluate_scalar_expr(py: Python, expr: &Expr, row: &RowInner) -> ExecResult<ScalarValue> {
    match expr {
        Expr::Column(column) => {
            let table_ref = match column.relation.as_ref() {
                Some(t) => t,
                None => {
                    let candidates = row
                        .iter()
                        .filter_map(|(k, v)| v.contains_key(column.name.as_ref()).then_some(k))
                        .take(2)
                        .collect_vec();

                    match candidates.len() {
                        0 => return Err(ExecError::ColumnNotFound(column.name.clone())),
                        1 => candidates.into_iter().next().unwrap(),
                        _ => return Err(ExecError::AmbiguousColumn(column.name.clone())),
                    }
                }
            };
            let part = row
                .get(table_ref)
                .ok_or_else(|| ExecError::TableNotFound(table_ref.clone()))?;

            let result = match part.get(column.name.as_ref()) {
                Some(x) => x.into_py(py),
                None => return Ok(ScalarValue::NULL),
            };

            Ok(result.into())
        }
        Expr::Literal(scalar) => Ok(scalar.clone()),
        Expr::UnaryExpr(unary_expr) => {
            let expr = evaluate_scalar_expr(py, &unary_expr.expr, row)?;

            if expr.is_null() {
                return Ok(ScalarValue::NULL);
            }

            let value = expr.into_bound(py);

            let result = match unary_expr.op {
                Operator::Plus => value.call_method0("__pos__")?.into_py(py),
                Operator::Minus => value.call_method0("__neg__")?.into_py(py),
                Operator::Not => (!value.is_truthy()?).into_py(py),
                _ => unreachable!(),
            };

            Ok(result.into())
        }
        Expr::BinaryExpr(binary_expr) => {
            let left = evaluate_scalar_expr(py, &binary_expr.left, row)?;
            let right = evaluate_scalar_expr(py, &binary_expr.right, row)?;

            if left.is_null() | right.is_null() {
                return Ok(ScalarValue::NULL);
            };
            let left = left.into_bound(py);
            let right = right.into_bound(py);

            let result = match binary_expr.op {
                Operator::Plus => left.add(right)?.into_py(py),
                Operator::Minus => left.sub(right)?.into_py(py),
                Operator::Multiply => left.mul(right)?.into_py(py),
                Operator::Divide => left.div(right)?.into_py(py),
                Operator::IntegerDivide => left.call_method1("__floordiv__", (right,))?.into_py(py),
                Operator::Modulo => left.call_method1("__mod__", (right,))?.into_py(py),
                Operator::Eq => left.eq(right)?.into_py(py),
                Operator::Gt => left.gt(right)?.into_py(py),
                Operator::GtEq => left.ge(right)?.into_py(py),
                Operator::Lt => left.lt(right)?.into_py(py),
                Operator::LtEq => left.le(right)?.into_py(py),
                Operator::And => (left.is_truthy()? && right.is_truthy()?).into_py(py),
                Operator::Or => (left.is_truthy()? || right.is_truthy()?).into_py(py),
                Operator::Not => unreachable!(),
                Operator::Arrow => match left.get_item(right) {
                    Ok(x) => x.into_py(py),
                    Err(_) => return Ok(ScalarValue::NULL),
                },
            };

            Ok(result.into())
        }
        Expr::ScalarFunction(f) => {
            let args: Vec<_> = f
                .args
                .iter()
                .map(|a| evaluate_scalar_expr(py, a, row))
                .try_collect()?;

            f.func.invoke(&args)
        }
        _ => todo!(),
    }
}
