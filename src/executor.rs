use std::{
    cell::RefCell,
    collections::{hash_map::Entry, BTreeMap, HashMap},
    rc::Rc,
};

use itertools::Itertools;
use pyo3::{
    intern,
    prelude::*,
    types::{PyAny, *},
};

use crate::{
    functions::scalar::{ScalarUDF, Volatility},
    logical_plan::*,
    stream::{IndexMap, Row, Stream, *},
};

pub struct ExecutionContext {
    tables: HashMap<String, PyObject>,
    scalar_functions: HashMap<Identifier, ScalarUDF>,
    result_cache: RefCell<HashMap<Identifier, PyObject>>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            scalar_functions: crate::functions::scalar::registry()
                .unwrap()
                .into_iter()
                .map(|f| (f.name.clone(), f))
                .collect(),
            result_cache: RefCell::new(HashMap::new()),
        }
    }
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_scalar_udf(&mut self, name: &str, function: PyObject) {
        self.scalar_functions.insert(
            Identifier::new(name.to_string()),
            ScalarUDF::volatile(name, function),
        );
    }

    pub fn add_table(&mut self, name: &str, data: PyObject) {
        self.tables.insert(name.to_string(), data);
    }

    pub fn scan_table<'p>(
        &self,
        py: Python<'p>,
        table_name: &'p TableReference,
    ) -> PyResult<Stream<'p>> {
        let inner = self
            .tables
            .get(table_name.0.as_ref())
            .ok_or_else(|| NameError!("table `{}` is not defined", table_name))?;

        let stream = inner.bind(py).iter()?.map_then(|obj| {
            obj.downcast_into::<PyDict>()
                .map_err(PyErr::from)
                .map(|row| IndexMap::from_iter([(table_name.clone(), row)]))
        });

        Ok(Stream::new(stream))
    }
}

pub fn execute_plan<'p>(
    py: Python<'p>,
    plan: &'p LogicalPlan,
    ctx: &'p mut ExecutionContext,
) -> PyResult<Stream<'p>> {
    plan.execute(py, ctx)
}

pub trait Exec<'p> {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext)
        -> ::pyo3::PyResult<Stream<'p>>;
}

impl<'p> Exec<'p> for LogicalPlan {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> Result<Stream<'p>, PyErr> {
        match self {
            LogicalPlan::Projection(v) => v.execute(py, ctx),
            LogicalPlan::TableScan(v) => v.execute(py, ctx),
            LogicalPlan::SubqueryAlias(v) => v.execute(py, ctx),
            LogicalPlan::Filter(v) => v.execute(py, ctx),
            LogicalPlan::EmptyRelation(v) => v.execute(py, ctx),
            LogicalPlan::Join(v) => v.execute(py, ctx),
            LogicalPlan::Sort(v) => v.execute(py, ctx),
            LogicalPlan::Limit(v) => v.execute(py, ctx),
        }
    }
}

pub trait ExecExpr<'p> {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> Result<Bound<'p, PyAny>, PyErr>;
}

impl<'p> ExecExpr<'p> for Expr {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> Result<Bound<'p, PyAny>, PyErr> {
        match self {
            Expr::Column(v) => v.execute(py, ctx, row),
            Expr::Alias(v) => v.execute(py, ctx, row),
            Expr::Literal(v) => v.execute(py, ctx, row),
            Expr::Unary(v) => v.execute(py, ctx, row),
            Expr::Binary(v) => v.execute(py, ctx, row),
            Expr::ScalarFunction(v) => v.execute(py, ctx, row),
            Expr::Wildcard(v) => v.execute(py, ctx, row),
            Expr::Tuple(v) => v.execute(py, ctx, row),
            Expr::List(v) => v.execute(py, ctx, row),
            Expr::Dict(v) => v.execute(py, ctx, row),
            Expr::GetItem(v) => v.execute(py, ctx, row),
            Expr::GetAttr(v) => v.execute(py, ctx, row),
            Expr::MethodCall(v) => v.execute(py, ctx, row),
        }
    }
}

impl<'p> Exec<'p> for Projection {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let table_ref = TableReference::default();

        let data = self.input.execute(py, ctx)?.map_then(move |row| {
            let data = self
                .expr
                .iter()
                .map(|(name, expr)| -> PyResult<Vec<(_, _)>> {
                    match expr {
                        Expr::Wildcard(wildcard) => match wildcard.table.as_ref() {
                            Some(table_ref) => {
                                let part = row.get(table_ref).ok_or_else(|| {
                                    NameError!("table `{}` is not defined", table_ref)
                                })?;

                                Ok(part
                                    .into_iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect_vec())
                            }
                            None => Ok(row
                                .values()
                                .flat_map(|x| x.into_iter().map(|(k, v)| (k.clone(), v.clone())))
                                .collect_vec()),
                        },
                        _ => Ok(vec![(
                            PyString::new_bound(py, &name.0).into_any(),
                            expr.execute(py, ctx, &row)?,
                        )]),
                    }
                })
                .flatten_ok();

            let part: Vec<_> = data.try_collect()?;
            Ok(IndexMap::from_iter([(
                table_ref.clone(),
                part.into_py_dict_bound(py),
            )]))
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
            .map(|row| -> PyResult<Vec<_>> {
                self.expr
                    .iter()
                    .map(|e| e.expr.execute(py, ctx, row).map(|v| v.unbind()))
                    .try_collect()
            })
            .enumerate()
            .map(|(i, v)| v.map(|k| (k, i)))
            .try_collect()?;

        indices.sort_by(|(a, _), (b, _)| {
            a.iter()
                .map(|a| a.bind(py))
                .zip(b.iter().map(|b| b.bind(py)))
                .enumerate()
                .fold(std::cmp::Ordering::Equal, |acc, (i, (a, b))| {
                    let ordering = acc.then(a.compare(b).unwrap());
                    if self.expr[i].asc {
                        ordering
                    } else {
                        ordering.reverse()
                    }
                })
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

        let stream = left.into_iter().filter_map_then(move |x| {
            let join_keys = self
                .on
                .iter()
                .filter_map(|(left_expr, _)| left_expr.execute(py, ctx, x).ok())
                .collect_vec();

            let value = PyTuple::new_bound(py, join_keys);
            let key = value.hash()?;

            let mut result: Row = IndexMap::default();

            let row = match hash_table.get(&key) {
                Some(row) => row,
                None => return Ok(None),
            };

            for (table_ref, data) in row {
                let t = result
                    .entry(table_ref.clone())
                    .or_insert(PyDict::new_bound(py));

                for (k, v) in data.iter() {
                    t.set_item(k, v)?;
                }
            }

            for (table_ref, data) in x.into_iter() {
                let t = result
                    .entry(table_ref.clone())
                    .or_insert(PyDict::new_bound(py));

                for (k, v) in data.iter() {
                    t.set_item(k, v)?;
                }
            }

            Ok(Some(result))
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
    fn execute(&'p self, py: Python<'p>, _ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let table_ref = TableReference::default();
        let row = IndexMap::from_iter([(table_ref, PyDict::new_bound(py))]);
        let data = vec![Ok(row)];
        Ok(Stream::new(data))
    }
}

impl<'p> Exec<'p> for Limit {
    fn execute(&'p self, py: Python<'p>, ctx: &'p ExecutionContext) -> PyResult<Stream<'p>> {
        let limit = self
            .limit
            .execute(py, ctx, &Default::default())?
            .extract::<usize>()?;

        let offset = self
            .offset
            .as_ref()
            .map(|offset| {
                offset
                    .execute(py, ctx, &Default::default())?
                    .extract::<usize>()
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
    predicate.execute(py, ctx, row).and_then(|x| x.is_truthy())
}

impl<'p> ExecExpr<'p> for Wildcard {
    fn execute(
        &'p self,
        _py: Python<'p>,
        _ctx: &'p ExecutionContext,
        _row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        unreachable!();
    }
}

impl<'p> ExecExpr<'p> for Alias {
    fn execute(
        &'p self,
        _py: Python<'p>,
        _ctx: &'p ExecutionContext,
        _row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        unreachable!();
    }
}
impl<'p> ExecExpr<'p> for Column {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        if self.name.0.starts_with('@') {
            return ctx
                .tables
                .get(self.name.0.as_ref())
                .map(|v| v.clone_ref(py).into_bound(py))
                .ok_or_else(|| NameError!("variable `{}` is not defined", self.name));
        }

        let table_ref = match self.relation.as_ref() {
            Some(t) => t,
            None => {
                if row.len() == 1 {
                    row.keys().next().unwrap()
                } else {
                    let name = PyString::new_bound(py, &self.name.0);
                    let candidates = row
                        .iter()
                        .filter_map(|(k, v)| v.contains(&name).unwrap().then_some(k))
                        .take(2)
                        .collect_vec();

                    match candidates.len() {
                        0 => return Err(NameError!("column `{}` is not defined", self.name)),
                        1 => candidates[0],
                        _ => return Err(NameError!("column `{}` is ambiguous", self.name)),
                    }
                }
            }
        };
        let part = row
            .get(table_ref)
            .ok_or_else(|| NameError!("table `{}` is not defined", table_ref))?;

        match part.get_item(&self.name)? {
            Some(x) => Ok(x),
            None => Ok(py.None().into_bound(py)),
        }
    }
}

impl<'p> ExecExpr<'p> for Rc<PyObject> {
    fn execute(
        &'p self,
        py: Python<'p>,
        _ctx: &'p ExecutionContext,
        _row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        Ok(self.bind(py).clone())
    }
}

impl<'p> ExecExpr<'p> for UnaryExpr {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let value = self.expr.execute(py, ctx, row)?;

        if value.is_none() {
            return Ok(value);
        }

        let result = match self.op {
            Operator::Plus => value.call_method0(intern!(py, "__pos__"))?,
            Operator::Minus => value.call_method0(intern!(py, "__neg__"))?,
            Operator::Not => PyBool::new_bound(py, !value.is_truthy()?)
                .to_owned()
                .into_any(),
            _ => unreachable!(),
        };

        Ok(result)
    }
}
impl<'p> ExecExpr<'p> for BinaryExpr {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let left = self.left.execute(py, ctx, row)?;
        let right = self.right.execute(py, ctx, row)?;

        if !matches!(self.op, Operator::Is | Operator::IsNot) && (left.is_none() | right.is_none())
        {
            return Ok(PyNone::get_bound(py).to_owned().into_any());
        };

        let result = match self.op {
            Operator::Plus => left.add(right)?,
            Operator::Minus => left.sub(right)?,
            Operator::Multiply => left.mul(right)?,
            Operator::Divide => left.div(right)?,
            Operator::IntegerDivide => left.call_method1(intern!(py, "__floordiv__"), (right,))?,
            Operator::Modulo => left.call_method1(intern!(py, "__mod__"), (right,))?,
            Operator::Eq => left.eq(right)?.into_py(py).into_bound(py),
            Operator::Gt => left.gt(right)?.into_py(py).into_bound(py),
            Operator::GtEq => left.ge(right)?.into_py(py).into_bound(py),
            Operator::Lt => left.lt(right)?.into_py(py).into_bound(py),
            Operator::LtEq => left.le(right)?.into_py(py).into_bound(py),
            Operator::And => (left.is_truthy()? && right.is_truthy()?)
                .into_py(py)
                .into_bound(py),
            Operator::Or => (left.is_truthy()? || right.is_truthy()?)
                .into_py(py)
                .into_bound(py),
            Operator::Not => unreachable!(),
            // Operator::Arrow => match left.get_item(right) {
            //     Ok(x) => x.into_py(py),
            //     Err(_) => return Ok(py.None()),
            // },
            Operator::Arrow => todo!(),
            Operator::Is => left.is(&right).into_py(py).into_bound(py),
            Operator::IsNot => (!left.is(&right)).into_py(py).into_bound(py),
            Operator::BitAnd => left.bitand(right)?,
            Operator::BitOr => left.bitor(right)?,
        };

        Ok(result)
    }
}

impl<'p> ExecExpr<'p> for ScalarFunction {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let f_impl = ctx
            .scalar_functions
            .get(&self.name)
            .ok_or_else(|| NameError!("function `{}` is not defined", self.name))?;

        if f_impl.volatility == Volatility::Stable {
            return match ctx.result_cache.borrow_mut().entry(f_impl.name.clone()) {
                Entry::Occupied(e) => Ok(e.get().clone_ref(py).into_bound(py)),
                Entry::Vacant(e) => {
                    let v = e.insert(f_impl.inner.call0(py)?);
                    Ok(v.clone_ref(py).into_bound(py))
                }
            };
        };

        let args: Vec<_> = self
            .args
            .iter()
            .map(|expr| expr.execute(py, ctx, row))
            .try_collect()?;

        let kwargs: Vec<_> = self
            .kwargs
            .iter()
            .map(|(k, v)| -> PyResult<_> { Ok((k.clone(), v.execute(py, ctx, row)?)) })
            .try_collect()?;

        f_impl.inner.bind(py).call(
            PyTuple::new_bound(py, args),
            Some(&kwargs.into_py_dict_bound(py)),
        )
    }
}

impl<'p> ExecExpr<'p> for MethodCall {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let args: Vec<_> = self
            .args
            .iter()
            .map(|expr| expr.execute(py, ctx, row))
            .try_collect()?;

        let input = self.input.execute(py, ctx, row)?;

        input.call_method1(
            PyString::new_bound(py, self.name.0.as_ref()),
            PyTuple::new_bound(py, args),
        )
    }
}

impl<'p> ExecExpr<'p> for Tuple {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let args: Vec<_> = self
            .elements
            .iter()
            .map(|expr| expr.execute(py, ctx, row))
            .try_collect()?;

        Ok(PyTuple::new_bound(py, args).into_any())
    }
}

impl<'p> ExecExpr<'p> for List {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let args: Vec<_> = self
            .elements
            .iter()
            .map(|expr| expr.execute(py, ctx, row))
            .try_collect()?;

        Ok(PyList::new_bound(py, args).into_any())
    }
}

impl<'p> ExecExpr<'p> for Dict {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let items: Vec<_> = self
            .items
            .iter()
            .map(|(k, v)| -> PyResult<_> {
                Ok((k.execute(py, ctx, row)?, v.execute(py, ctx, row)?))
            })
            .try_collect()?;

        Ok(items.into_py_dict_bound(py).into_any())
    }
}

impl<'p> ExecExpr<'p> for GetItem {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let mut input = self.input.execute(py, ctx, row)?;
        for key in self.keys.iter() {
            if input.is_none() {
                break;
            }
            if let Ok(item) = input.get_item(key.execute(py, ctx, row)?) {
                input = item
            } else {
                input = py.None().into_bound(py);
            }
        }

        Ok(input)
    }
}

impl<'p> ExecExpr<'p> for GetAttr {
    fn execute(
        &'p self,
        py: Python<'p>,
        ctx: &'p ExecutionContext,
        row: &'p Row,
    ) -> PyResult<Bound<'p, PyAny>> {
        let mut input = self.input.execute(py, ctx, row)?;
        if input.is_none() {
            return Ok(py.None().into_bound(py));
        }

        for key in self.keys.iter() {
            let key = key.execute(py, ctx, row)?;
            let key = key.downcast::<PyString>().map_err(PyErr::from)?;

            if let Ok(item) = input.getattr(key) {
                input = item
            } else {
                return Ok(py.None().into_bound(py));
            }
        }

        Ok(input)
    }
}
