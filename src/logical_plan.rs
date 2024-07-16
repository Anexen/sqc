use derive_more::{Display, From};
use itertools::Itertools;
use pyo3::{FromPyObject, PyObject};
use std::fmt;
use std::rc::Rc;

use crate::stream::IndexMap;

#[derive(Debug, Clone, Display, Eq, Hash, PartialEq)]
#[display(fmt = "{_0}")]
pub struct Identifier(pub Rc<String>);

impl Identifier {
    pub fn new(value: String) -> Self {
        Self(Rc::new(value))
    }
}

impl<'p> FromPyObject<'p> for Identifier {
    fn extract_bound(ob: &pyo3::Bound<'p, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let value = pyo3::types::PyAnyMethods::extract::<String>(ob)?;
        Ok(Self::new(value))
    }
}

impl pyo3::IntoPy<PyObject> for Identifier {
    fn into_py(self, py: pyo3::Python<'_>) -> PyObject {
        pyo3::types::PyString::new_bound(py, self.0.as_ref()).into()
    }
}

impl pyo3::ToPyObject for Identifier {
    fn to_object(&self, py: pyo3::Python<'_>) -> PyObject {
        pyo3::types::PyString::new_bound(py, self.0.as_ref()).into()
    }
}

impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

// impl<'p> FromPyObjectBound<'_, 'p> for Identifier {
//     fn from_py_object_bound(ob: Borrowed<'_, 'p, PyAny>) -> PyResult<Self> {
//         Self::extract_bound(&ob)
//     }
// }

#[derive(Debug, Clone, From)]
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

#[derive(Debug, Clone)]
pub struct Limit {
    pub limit: Expr,
    pub offset: Option<Expr>,
    pub input: Rc<LogicalPlan>,
}

#[derive(Debug, Clone, From)]
pub struct Sort {
    pub expr: Vec<OrderByExpr>,
    pub input: Rc<LogicalPlan>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Display)]
#[display(
    fmt = "{name}",
    // r#"match relation { Some(v) => format!("{v}.{name}"), None => name.to_string() }"#
)]
pub struct Column {
    pub name: Identifier,
    pub relation: Option<TableReference>,
}

#[derive(Debug, Clone, Display)]
#[display(fmt = "{expr} AS {name}")]
pub struct Alias {
    pub expr: Box<Expr>,
    // pub relation: Option<TableReference>,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct SubqueryAlias {
    pub alias: TableReference,
    pub input: Rc<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub expr: IndexMap<Identifier, Expr>,
    pub input: Rc<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct EmptyRelation {
    pub produce_one_row: bool,
}

#[derive(Debug, Clone)]
pub struct TableScan {
    pub table_name: TableReference,
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<Expr>,
    pub fetch: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub predicate: Expr,
    pub input: Rc<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct Join {
    pub left: Rc<LogicalPlan>,
    pub right: Rc<LogicalPlan>,
    pub join_type: JoinType,
    /// equi conditions
    pub on: Vec<(Expr, Expr)>,
    /// non-equi conditions
    pub filter: Option<Expr>,
}

#[derive(Debug, Clone, From, Display)]
pub enum Expr {
    Column(Column),
    Alias(Alias),
    Literal(Rc<PyObject>),
    Unary(UnaryExpr),
    Binary(BinaryExpr),
    ScalarFunction(ScalarFunction),
    Wildcard(Wildcard),
    Tuple(Tuple),
    List(List),
    Dict(Dict),
    GetItem(GetItem),
    GetAttr(GetAttr),
    MethodCall(MethodCall),
    Try(Try),
}

fn display_comma_separated_expr(exprs: &[Expr]) -> String {
    exprs.iter().map(ToString::to_string).join(", ")
}

#[derive(Debug, Clone, Display)]
#[display(fmt = "({})", "display_comma_separated_expr(&self.elements)")]
pub struct Tuple {
    pub elements: Vec<Expr>,
}

#[derive(Debug, Clone, Display)]
#[display(fmt = "[{}]", "display_comma_separated_expr(&self.elements)")]
pub struct List {
    pub elements: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct Dict {
    pub items: Vec<(Expr, Expr)>,
}

impl fmt::Display for Dict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let items = self
            .items
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .join(", ");

        write!(f, "{{{items}}}")
    }
}

#[derive(Debug, Clone)]
pub struct GetItem {
    pub input: Box<Expr>,
    pub keys: Vec<Expr>,
}

impl fmt::Display for GetItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys = self.keys.iter().map(|e| format!("[{e}]")).join("");
        write!(f, "{}{}", self.input, keys)
    }
}

#[derive(Debug, Clone)]
pub struct GetAttr {
    pub input: Box<Expr>,
    pub keys: Vec<Expr>,
}

impl fmt::Display for GetAttr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys = self.keys.iter().map(|e| e.to_string()).join(".");
        write!(f, "{}.{}", self.input, keys)
    }
}
#[derive(Debug, Clone)]
pub struct ScalarFunction {
    pub name: Identifier,
    pub args: Vec<Expr>,
    pub kwargs: IndexMap<Identifier, Expr>,
}

impl fmt::Display for ScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args = self.args.iter().map(|e| e.to_string()).join(", ");
        write!(f, "{}({})", self.name, args)
    }
}

#[derive(Debug, Clone)]
pub struct MethodCall {
    pub input: Box<Expr>,
    pub name: Identifier,
    pub args: Vec<Expr>,
}

impl fmt::Display for MethodCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args = self.args.iter().map(|e| e.to_string()).join(", ");
        write!(f, "{}.{}({})", self.input, self.name, args)
    }
}

#[derive(Debug, Clone, Display)]
#[display(fmt = "({left} {op} {right})")]
pub struct BinaryExpr {
    pub left: Box<Expr>,
    pub op: Operator,
    pub right: Box<Expr>,
}

#[derive(Debug, Clone, Display)]
#[display(fmt = "{op} {expr}")]
pub struct UnaryExpr {
    pub op: Operator,
    pub expr: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct Wildcard {
    pub table: Option<TableReference>,
    // TODO: * EXCEPT (name)
}

impl fmt::Display for Wildcard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.table.as_ref() {
            Some(v) => write!(f, "{v}.*"),
            None => write!(f, "*"),
        }
    }
}

#[derive(Debug, Clone, Display)]
#[display(fmt = "try({})", "display_comma_separated_expr(&self.args)")]
pub struct Try {
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Display)]
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
    #[display(fmt = "and")]
    And,
    #[display(fmt = "or")]
    Or,
    #[display(fmt = "not")]
    Not,
    #[display(fmt = "->")]
    Arrow,
    #[display(fmt = "is")]
    Is,
    #[display(fmt = "is not")]
    IsNot,
    #[display(fmt = "&")]
    BitAnd,
    #[display(fmt = "|")]
    BitOr,
}

#[derive(Debug, Clone, Display)]
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
        Expr::Binary(BinaryExpr {
            left: Box::new(self),
            op,
            right: Box::new(right),
        })
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
            Expr::Unary(unary_expr) => unary_expr.expr.extract_columns_impl(columns),
            Expr::Binary(binary_expr) => {
                binary_expr.right.extract_columns_impl(columns);
                binary_expr.left.extract_columns_impl(columns);
            }
            Expr::ScalarFunction(f) => f.args.iter().for_each(|a| a.extract_columns_impl(columns)),
            Expr::Wildcard(_) => todo!(),
            Expr::Tuple(Tuple { elements }) | Expr::List(List { elements }) => elements
                .iter()
                .for_each(|a| a.extract_columns_impl(columns)),
            Expr::Dict(v) => {
                v.items.iter().for_each(|(k, v)| {
                    k.extract_columns_impl(columns);
                    v.extract_columns_impl(columns);
                });
            }
            Expr::GetItem(GetItem { input, keys }) | Expr::GetAttr(GetAttr { input, keys }) => {
                input.extract_columns_impl(columns);
                keys.iter().for_each(|k| k.extract_columns_impl(columns));
            }
            Expr::MethodCall(v) => {
                v.input.extract_columns_impl(columns);
                v.args.iter().for_each(|a| a.extract_columns_impl(columns));
            }
            Expr::Try(v) => {
                v.args.iter().for_each(|a| a.extract_columns_impl(columns));
            }
        };
    }

    pub fn split_binary_expression(&self, op: Operator) -> Vec<Expr> {
        let mut result = Vec::new();
        self.split_binary_expression_impl(&op, &mut result);
        result
    }

    fn split_binary_expression_impl(&self, op: &Operator, exprs: &mut Vec<Expr>) {
        match self {
            Expr::Binary(binary_expr) if &binary_expr.op == op => {
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
