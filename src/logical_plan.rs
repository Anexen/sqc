use derive_builder::{Builder, UninitializedFieldError};
use derive_more::{Display, Error, From};
use indexmap::IndexMap;
use itertools::Itertools;
use pyo3::PyObject;
use std::fmt;
use std::rc::Rc;

#[derive(Debug, Error, Display)]
#[display(fmt = "{_0}")]
pub struct PlanError(#[error(not(source))] String);

impl From<UninitializedFieldError> for PlanError {
    fn from(e: UninitializedFieldError) -> Self {
        PlanError(e.to_string())
    }
}

#[derive(Clone, From)]
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

#[derive(Clone, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
pub struct Limit {
    pub limit: Expr,
    pub offset: Option<Expr>,
    pub input: Rc<LogicalPlan>,
}

#[derive(Clone, From, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
pub struct Sort {
    pub expr: Vec<OrderByExpr>,
    #[builder(setter(into))]
    pub input: Rc<LogicalPlan>,
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

#[derive(Debug, Clone, Display, Hash, PartialEq, Eq)]
#[display(fmt = "{_0}")]
pub struct TableReference(pub Rc<String>);

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
    pub name: Rc<String>,
    #[builder(default)]
    pub relation: Option<TableReference>,
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

#[derive(Clone, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct SubqueryAlias {
    pub alias: TableReference,
    pub input: Rc<LogicalPlan>,
}

#[derive(Clone, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct Projection {
    pub expr: IndexMap<String, Expr>,
    pub input: Rc<LogicalPlan>,
}

#[derive(Clone, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
pub struct EmptyRelation {
    #[builder(default = "true")]
    pub produce_one_row: bool,
}

#[derive(Clone, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"))]
pub struct TableScan {
    pub table_name: TableReference,
    #[builder(default)]
    pub projection: Option<Vec<usize>>,
    #[builder(default)]
    pub filters: Vec<Expr>,
    #[builder(default)]
    pub fetch: Option<usize>,
}

#[derive(Clone, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct Filter {
    pub predicate: Expr,
    pub input: Rc<LogicalPlan>,
}

#[derive(Clone, Builder)]
#[cfg_attr(debug_assertions, derive(Debug))]
#[builder(build_fn(error = "PlanError"), setter(into))]
pub struct Join {
    pub left: Rc<LogicalPlan>,
    pub right: Rc<LogicalPlan>,
    pub join_type: JoinType,
    /// equi conditions
    #[builder(default)]
    pub on: Vec<(Expr, Expr)>,
    /// non-equi conditions
    #[builder(default)]
    pub filter: Option<Expr>,
}

#[derive(Clone, From, Display)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum Expr {
    Column(Column),
    Alias(Alias),
    Literal(Rc<PyObject>),
    UnaryExpr(UnaryExpr),
    BinaryExpr(BinaryExpr),
    ScalarFunction(ScalarFunction),
    Wildcard(Wildcard),
}

#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ScalarFunction {
    pub name: String,
    pub args: Vec<Expr>,
}

impl fmt::Display for ScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.args.iter().map(|e| e.to_string()).join(", ")
        )
    }
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
