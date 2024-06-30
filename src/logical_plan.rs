use derive_builder::{Builder, UninitializedFieldError};
use derive_more::{Display, Error, From};
use derive_visitor::Drive;
use indexmap::IndexMap;
use itertools::Itertools;
use pyo3::prelude::*;
use pyo3::types::PyNone;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Error, Display)]
#[display(fmt = "{_0}")]
pub struct PlanError(#[error(not(source))] String);

impl From<UninitializedFieldError> for PlanError {
    fn from(e: UninitializedFieldError) -> Self {
        PlanError(e.to_string())
    }
}

#[derive(Clone, From, Drive)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum LogicalPlan {
    Projection(Projection),
    TableScan(TableScan),
    SubqueryAlias(SubqueryAlias),
    Filter(Filter),
    EmptyRelation(EmptyRelation),
    Join(Join),
    Sort(Sort),
    Limit(Limit),
}

#[derive(Clone, Drive, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
pub struct Limit {
    #[drive(skip)]
    pub limit: Expr,
    #[drive(skip)]
    pub offset: Option<Expr>,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, From, Drive, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
pub struct Sort {
    #[drive(skip)]
    pub expr: Vec<OrderByExpr>,
    #[builder(setter(into))]
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        write!(f, "{}", if self.asc { " ASC" } else { " DESC" })?;
        write!(
            f,
            "{}",
            if self.nulls_first {
                " NULLS FIRST"
            } else {
                " NULLS LAST"
            }
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Display, Drive, Hash, PartialEq, Eq)]
#[display(fmt = "{_0}")]
pub struct TableReference(#[drive(skip)] pub Arc<String>);

impl Default for TableReference {
    fn default() -> Self {
        Self("data".to_string().into())
    }
}

#[derive(Clone, Display, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
#[display(
    fmt = "{name}",
    // r#"match relation { Some(v) => format!("{v}.{name}"), None => name.to_string() }"#
)]
pub struct Column {
    #[builder(setter(into))]
    pub name: Arc<String>,
    #[builder(default)]
    pub relation: Option<TableReference>,
}

#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ScalarValue(pub Option<pyo3::PyObject>);

impl ScalarValue {
    pub const NULL: Self = Self(None);

    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }

    pub fn into_bound(self, py: Python<'_>) -> Bound<PyAny> {
        if let Some(value) = self.0 {
            value.into_bound(py).into_any()
        } else {
            PyNone::get_bound(py).into_py(py).into_bound(py)
        }
    }
}
//
// impl PartialEq for ScalarValue {
//     fn eq(&self, other: &Self) -> bool {
//         Python::with_gil(|py| {
//             if let (Some(a), Some(b)) = (self.0.as_ref(), other.0.as_ref()) {
//                 a.bind(py).eq(b.bind(py)).unwrap_or(false)
//             } else {
//                 false
//             }
//         })
//     }
// }
//
// impl Eq for ScalarValue {}
//
// impl PartialOrd for ScalarValue {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Python::with_gil(|py| match (self.0.as_ref(), other.0.as_ref()) {
//             (Some(a), Some(b)) => a.bind(py).compare(b.bind(py)).ok(),
//             _ => todo!(),
//         })
//     }
// }
//
// impl Ord for ScalarValue {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         Python::with_gil(|py| match (self.0.as_ref(), other.0.as_ref()) {
//             (Some(a), Some(b)) => a.bind(py).compare(b.bind(py)).unwrap(),
//             _ => todo!(),
//         })
//     }
// }

impl From<PyObject> for ScalarValue {
    fn from(value: PyObject) -> Self {
        let value = Python::with_gil(|py| if value.is_none(py) { None } else { Some(value) });
        Self(value)
    }
}

impl IntoPy<PyObject> for ScalarValue {
    fn into_py(self, py: Python<'_>) -> PyObject {
        if let Some(value) = self.0 {
            value.into_py(py)
        } else {
            PyNone::get_bound(py).into_py(py)
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(value) = &self.0 {
            write!(f, "{value}")
        } else {
            write!(f, "None")
        }
    }
}

#[derive(Clone, Display, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[display(fmt = "{expr} AS {name}")]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct Alias {
    pub expr: Box<Expr>,
    // pub relation: Option<TableReference>,
    pub name: String,
}

#[derive(Clone, Builder, Drive)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct SubqueryAlias {
    pub alias: TableReference,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, Builder, Drive)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct Projection {
    #[drive(skip)]
    pub expr: IndexMap<String, Expr>,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, Builder, Drive)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
pub struct EmptyRelation {
    #[builder(default = "true")]
    #[drive(skip)]
    pub produce_one_row: bool,
}

#[derive(Clone, Builder, Drive)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
pub struct TableScan {
    pub table_name: TableReference,
    #[drive(skip)]
    #[builder(default)]
    pub projection: Option<Vec<usize>>,
    #[drive(skip)]
    #[builder(default)]
    pub filters: Vec<Expr>,
    #[drive(skip)]
    #[builder(default)]
    pub fetch: Option<usize>,
}

#[derive(Clone, Builder, Drive)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct Filter {
    #[drive(skip)]
    pub predicate: Expr,
    pub input: Arc<LogicalPlan>,
}

#[derive(Clone, Builder, Drive)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    /// equi conditions
    #[drive(skip)]
    pub on: Vec<(Expr, Expr)>,
    /// non-equi conditions
    #[drive(skip)]
    #[builder(default)]
    pub filter: Option<Expr>,
    #[drive(skip)]
    pub join_type: JoinType,
}

#[derive(Clone, From, Display)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum Expr {
    Column(Column),
    Alias(Alias),
    Literal(ScalarValue),
    UnaryExpr(UnaryExpr),
    BinaryExpr(BinaryExpr),
    ScalarFunction(ScalarFunction),
    Wildcard(Wildcard),
}

#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ScalarFunction {
    pub func: Arc<dyn ScalarFunctionImpl>,
    pub args: Vec<Expr>,
}

impl fmt::Display for ScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.func.name(),
            self.args.iter().map(|e| e.to_string()).join(", ")
        )
    }
}

pub trait ScalarFunctionImpl: std::fmt::Debug {
    fn name(&self) -> &str;
    fn invoke(&self, args: &[ScalarValue]) -> Result<ScalarValue, crate::executor::ExecError>;
}

#[derive(Clone, Display, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
#[display(fmt = "({left} {op} {right})")]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: Operator,
    pub right: Box<Expr>,
}

impl BinaryExpr {
    pub fn is_strict_equality(&self) -> bool {
        match self.op {
            Operator::Eq => true,
            Operator::And => {
                let left = match self.left.as_ref() {
                    Expr::BinaryExpr(e) => e.is_strict_equality(),
                    _ => false,
                };
                let right = match self.left.as_ref() {
                    Expr::BinaryExpr(e) => e.is_strict_equality(),
                    _ => false,
                };
                left && right
            }
            _ => false,
        }
    }

    pub fn try_decompose_into(&self, out: &mut Vec<(Expr, Expr)>) -> Result<(), PlanError> {
        match self.op {
            Operator::Eq => {
                out.push((*self.left.clone(), *self.right.clone()));
            }
            Operator::And => {
                match self.left.as_ref() {
                    Expr::BinaryExpr(e) => e.try_decompose_into(out),
                    _ => Err(PlanError("".to_string())),
                }?;

                match self.right.as_ref() {
                    Expr::BinaryExpr(e) => e.try_decompose_into(out),
                    _ => Err(PlanError("".to_string())),
                }?;
            }
            _ => return Err(PlanError("".to_string())),
        };
        Ok(())
    }
}

#[derive(Clone, Display, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
#[display(fmt = "{op} {expr}")]
pub struct UnaryExpr {
    pub op: Operator,
    pub expr: Box<Expr>,
}

#[derive(Clone, Display, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
#[display(
    fmt = "{}",
    r#"match table { Some(v) => format!("{v}.*"), None => "*".to_string() }"#
)]
pub struct Wildcard {
    #[builder(default)]
    pub table: Option<TableReference>,
    // TODO: * EXCEPT (name)
}

#[derive(Clone, PartialEq, Eq, Display)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum Operator {
    #[display(fmt = "+")]
    Plus,
    #[display(fmt = "-")]
    Minus,
    #[display(fmt = "*")]
    Multiply,
    #[display(fmt = "/")]
    Divide,
    #[display(fmt = "//")]
    IntegerDivide,
    #[display(fmt = "%")]
    Modulo,
    #[display(fmt = "=")]
    Eq,
    #[display(fmt = ">")]
    Gt,
    #[display(fmt = ">=")]
    GtEq,
    #[display(fmt = "<")]
    Lt,
    #[display(fmt = "<=")]
    LtEq,
    #[display(fmt = "AND")]
    And,
    #[display(fmt = "OR")]
    Or,
    #[display(fmt = "NOT")]
    Not,
    #[display(fmt = "->")]
    Arrow,
}

#[derive(Clone, Display)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl Expr {
    pub fn and(self, other: Expr) -> Expr {
        self.binary_expr(other, Operator::And)
    }

    pub fn binary_expr(self, right: Expr, op: Operator) -> Expr {
        BinaryExprBuilder::default()
            .left(self)
            .op(op)
            .right(right)
            .build()
            .unwrap()
            .into()
    }

    pub fn extract_columns(&self) -> Vec<&Column> {
        let mut result = Vec::new();
        self.extract_columns_impl(&mut result);
        result
    }

    fn extract_columns_impl<'e>(&'e self, columns: &mut Vec<&'e Column>) {
        match self {
            Expr::Column(column) => columns.push(column),
            Expr::Alias(alias) => alias.expr.extract_columns_impl(columns),
            Expr::Literal(_) => {}
            Expr::UnaryExpr(unary_expr) => unary_expr.expr.extract_columns_impl(columns),
            Expr::BinaryExpr(binary_expr) => {
                binary_expr.right.extract_columns_impl(columns);
                binary_expr.left.extract_columns_impl(columns);
            }
            Expr::ScalarFunction(f) => f.args.iter().for_each(|a| a.extract_columns_impl(columns)),
            Expr::Wildcard(_) => todo!(),
        };
    }

    pub fn split_binary_expression(&self, op: Operator) -> Vec<Expr> {
        let mut result = Vec::new();
        self.split_binary_expression_impl(&op, &mut result);
        result
    }

    fn split_binary_expression_impl(&self, op: &Operator, exprs: &mut Vec<Expr>) {
        match self {
            Expr::BinaryExpr(binary_expr) if &binary_expr.op == op => {
                binary_expr.left.split_binary_expression_impl(op, exprs);
                binary_expr.right.split_binary_expression_impl(op, exprs);
            }
            Expr::Alias(alias) => alias.expr.split_binary_expression_impl(op, exprs),
            other => {
                exprs.push(other.clone());
            }
        };
    }
}
