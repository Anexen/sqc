use std::{
    collections::{BTreeMap, HashMap},
    rc::Rc,
};

use indexmap::IndexMap;
use itertools::Itertools;
use pyo3::{exceptions::PyNameError, prelude::*, types::*};

use crate::{functions::scalar::ScalarFunctionImpl, logical_plan::*};
use crate::{functions::scalar::ScalarUDF, stream::*};

pub struct ExecutionContext {
    tables: HashMap<String, PyObject>,
    scalar_functions: HashMap<String, Rc<dyn ScalarFunctionImpl>>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            scalar_functions: crate::functions::scalar::registry()
                .into_iter()
                .flat_map(|f| {
                    f.names()
                        .into_iter()
                        .map(move |s| (s.to_string(), Rc::clone(&f)))
                })
                .collect(),
        }
    }
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_scalar_udf(&mut self, name: String, function: PyObject) {
        self.scalar_functions
            .insert(name, Rc::new(ScalarUDF::new(function)));
    }

    pub fn add_table(&mut self, name: &str, data: PyObject) {
        self.tables.insert(name.to_string(), data);
    }

    pub fn scan_table<'p>(
        &self,
        py: Python<'p>,
        table_name: &'p TableReference,
    ) -> PyResult<Stream<'p>> {
        let inner = self.tables.get(table_name.0.as_ref()).ok_or_else(|| {
            PyNameError::new_err(format!("table `{}` is not defined", table_name))
        })?;

        let stream = inner.bind(py).iter()?.map(|obj| match obj {
            Ok(obj) => obj.downcast::<PyDict>().map_err(|e| e.into()).map(|row| {
                let data = row
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.unbind()))
                    .collect();
                [(table_name.clone(), data)].into()
            }),
            Err(e) => Err(e),
        });

        Ok(Stream::new(stream))
    }
}

pub fn execute_plan<'p>(
    py: Python<'p>,
    plan: &'p LogicalPlan,
    ctx: &'p ExecutionContext,
) -> PyResult<Stream<'p>> {
    plan.execute(py, ctx)
}

trait Exec<'p> {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>>;
}

trait ExecScalar<'p> {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<PyObject>;
}

impl<'p> Exec<'p> for LogicalPlan {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
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
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let data = self.input.execute(py, ctx)?.map_then(move |row| {
            let data = self
                .expr
                .iter()
                .map(|(name, expr)| -> PyResult<Vec<(String, PyObject)>> {
                    match expr {
                        Expr::Wildcard(wildcard) => match wildcard.table.as_ref() {
                            Some(table_ref) => {
                                let part = row.get(table_ref).ok_or_else(|| {
                                    PyNameError::new_err(format!(
                                        "table `{}` is not defined",
                                        table_ref
                                    ))
                                })?;

                                Ok(part
                                    .into_iter()
                                    .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                                    .collect_vec())
                            }
                            None => Ok(row
                                .values()
                                .flat_map(|x| {
                                    x.into_iter().map(|(k, v)| (k.clone(), v.clone_ref(py)))
                                })
                                .collect_vec()),
                        },
                        _ => Ok(vec![(name.clone(), expr.execute(py, ctx, &row)?)]),
                    }
                })
                .flatten_ok();

            let part: IndexMap<_, _> = data.try_collect()?;
            Ok([(TableReference::default(), part)].into())
        });

        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for TableScan {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let stream = ctx
            .scan_table(py, &self.table_name)?
            .filter_then(move |row| {
                self.filters
                    .iter()
                    .map(Ok)
                    .and_all(|expr| evaluate_predicate(py, ctx, expr, row))
            });

        Ok(Stream::new(stream))
    }
}

impl<'p> Exec<'p> for Sort {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let mut input: Vec<_> = self.input.execute(py, ctx)?.try_collect()?;

        let mut indices: Vec<_> = input
            .iter()
            .map(|row| -> PyResult<Vec<PyObject>> {
                self.expr
                    .iter()
                    .map(|e| e.expr.execute(py, ctx, row))
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
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let left: Vec<_> = self.left.execute(py, ctx)?.collect();
        let right: Vec<_> = self.right.execute(py, ctx)?.collect();

        let hash_table: BTreeMap<_, _> = right
            .into_iter()
            .map_then(|x| {
                let join_keys = self
                    .on
                    .iter()
                    .filter_map(|(_, right_expr)| right_expr.execute(py, ctx, &x).ok())
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
                .filter_map(|(left_expr, _)| left_expr.execute(py, ctx, &x).ok())
                .collect_vec();

            let value = PyTuple::new_bound(py, join_keys);
            let key = value.hash().unwrap();

            let mut result: Row = IndexMap::new();

            for (table_ref, data) in hash_table.get(&key)? {
                result
                    .entry(table_ref.clone())
                    .or_insert(IndexMap::new())
                    .extend(data.iter().map(|(k, v)| (k.clone(), v.clone_ref(py))));
            }

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
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let data = self
            .input
            .execute(py, ctx)?
            .map_ok(|row| row.into_values().map(|v| (self.alias.clone(), v)).collect());
        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for Filter {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let data = self
            .input
            .execute(py, ctx)?
            .filter_then(move |row| evaluate_predicate(py, ctx, &self.predicate, row));

        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for EmptyRelation {
    fn execute(&'p self, _py: Python<'p>, _ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let table_ref = TableReference::default();
        let row = [(table_ref, IndexMap::new())].into();
        let data = vec![Ok(row)];
        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for Limit {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let limit = self
            .limit
            .execute(py, ctx, &Default::default())?
            .extract::<usize>(py)?;

        let offset = self
            .offset
            .as_ref()
            .map(|offset| {
                offset
                    .execute(py, ctx, &Default::default())?
                    .extract::<usize>(py)
            })
            .transpose()?
            .unwrap_or(0);

        let stream = self.input.execute(py, ctx)?.skip(offset).take(limit);

        Ok(Stream::new(stream))
    }
}

fn evaluate_predicate(
    py: Python,
    ctx: &ExecutionContext,
    predicate: &Expr,
    row: &Row,
) -> PyResult<bool> {
    predicate
        .execute(py, ctx, row)
        .and_then(|x| x.is_truthy(py))
}

impl<'p> ExecScalar<'p> for Expr {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<PyObject> {
        match self {
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
                            0 => {
                                return Err(PyNameError::new_err(format!(
                                    "column `{}` is not defined",
                                    column.name
                                )))
                            }
                            1 => candidates[0],
                            _ => {
                                return Err(PyNameError::new_err(format!(
                                    "column `{}` is ambiguous",
                                    column.name
                                )))
                            }
                        }
                    }
                };
                let part = row.get(table_ref).ok_or_else(|| {
                    PyNameError::new_err(format!("table `{}` is not defined", table_ref))
                })?;

                let result = match part.get(column.name.as_ref()) {
                    Some(x) => x.into_py(py),
                    None => return Ok(py.None()),
                };

                Ok(result.into())
            }
            Expr::Literal(scalar) => Ok(scalar.clone_ref(py)),
            Expr::UnaryExpr(unary_expr) => {
                let value = unary_expr.expr.execute(py, ctx, row)?;

                if value.is_none(py) {
                    return Ok(value);
                }

                let result = match unary_expr.op {
                    Operator::Plus => value.call_method0(py, "__pos__")?,
                    Operator::Minus => value.call_method0(py, "__neg__")?,
                    Operator::Not => (!value.is_truthy(py)?).into_py(py),
                    _ => unreachable!(),
                };

                Ok(result.into())
            }
            Expr::BinaryExpr(binary_expr) => {
                let left = binary_expr.left.execute(py, ctx, row)?;
                let right = binary_expr.right.execute(py, ctx, row)?;

                if left.is_none(py) | right.is_none(py) {
                    return Ok(py.None());
                };
                let left = left.bind(py);
                let right = right.bind(py);

                let result = match binary_expr.op {
                    Operator::Plus => left.add(right)?.into_py(py),
                    Operator::Minus => left.sub(right)?.into_py(py),
                    Operator::Multiply => left.mul(right)?.into_py(py),
                    Operator::Divide => left.div(right)?.into_py(py),
                    Operator::IntegerDivide => {
                        left.call_method1("__floordiv__", (right,))?.into_py(py)
                    }
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
                        Err(_) => return Ok(py.None()),
                    },
                };

                Ok(result.into())
            }
            Expr::ScalarFunction(f) => {
                let args: Vec<_> = f
                    .args
                    .iter()
                    .map(|expr| expr.execute(py, ctx, row))
                    .try_collect()?;

                let f_impl = ctx.scalar_functions.get(&f.name).ok_or_else(|| {
                    PyNameError::new_err(format!("function `{}` is not defined", f.name))
                })?;

                f_impl.invoke(py, &args)
            }
            _ => todo!(),
        }
    }
}
