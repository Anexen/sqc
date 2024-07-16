use std::{any::Any, rc::Rc};

use derive_more::{Display, Error};
use itertools::Itertools;
use pyo3::{
    prelude::*,
    types::{PyFloat, PyLong},
    PyTypeInfo,
};
use sqlparser::ast;

use crate::logical_plan::*;
use crate::stream::IndexMap;

type PlanResult<T = LogicalPlan> = Result<T, PlannerError>;
type EquijoinPredicate = (Expr, Expr);

#[derive(Debug, Error, Display)]
pub enum PlannerError {
    NotImplemented(#[error(not(source))] String),
    SyntaxError(#[error(not(source))] String),
}

pub fn prepare_plan(query: &ast::Query) -> PlanResult<PreparedPlan> {
    let mut visitor = Visitor::default();
    let result = visitor.visit_query(query)?;

    Ok(PreparedPlan {
        logical_plan: Rc::new(result),
        external_names: visitor.external_names,
    })
}

pub struct PreparedPlan {
    pub logical_plan: Rc<LogicalPlan>,
    pub external_names: Vec<String>,
}

#[derive(PartialEq, Eq)]
enum FunctionArgumentTypeOrdering {
    Positional = 1,
    Keyword = 2,
}

#[derive(Default)]
struct Visitor {
    external_names: Vec<String>,
}

impl Visitor {
    pub fn visit_query(&mut self, query: &ast::Query) -> PlanResult {
        let mut result = self.visit_set_expr(query.body.as_ref(), query.order_by.as_ref())?;

        if let Some(limit) = query.limit.as_ref() {
            let offset = query
                .offset
                .as_ref()
                .map(|offset| self.visit_expr(&offset.value))
                .transpose()?;

            result = LogicalPlan::Limit(Limit {
                input: Rc::new(result),
                limit: self.visit_expr(limit)?,
                offset,
            })
        }

        Ok(result)
    }

    fn visit_set_expr(&mut self, expr: &ast::SetExpr, order_by: &[ast::OrderByExpr]) -> PlanResult {
        match expr {
            ast::SetExpr::Select(select) => self.visit_select(select, order_by),
            ast::SetExpr::Query(query) => self.visit_query(query),
            _ => unimplemented!("{:?}", expr.type_id()),
        }
    }

    fn visit_select(&mut self, select: &ast::Select, order_by: &[ast::OrderByExpr]) -> PlanResult {
        let mut result = self.visit_from(&select.from)?;
        if let Some(predicate) = &select.selection {
            result = LogicalPlan::Filter(Filter {
                predicate: self.visit_expr(predicate)?,
                input: Rc::new(result),
            });
        }

        if !order_by.is_empty() {
            let sort_expr = order_by
                .iter()
                .map(|o| self.visit_order_by_expr(o))
                .try_collect()?;

            result = LogicalPlan::Sort(Sort {
                expr: sort_expr,
                input: Rc::new(result),
            });
        }

        let projection = self.visit_projection(&select.projection)?;
        result = LogicalPlan::Projection(Projection {
            expr: projection,
            input: Rc::new(result),
        });

        Ok(result)
    }

    fn visit_from(&mut self, from: &[ast::TableWithJoins]) -> PlanResult {
        if from.is_empty() {
            return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
            }));
        };

        let scans = from
            .iter()
            .map(|t| self.visit_table_with_joins(t))
            .collect::<PlanResult<Vec<_>>>()?;

        if scans.len() > 1 {
            return Err(PlannerError::NotImplemented("cross join".to_string()));
        }

        Ok(scans.into_iter().next().unwrap())
    }

    fn visit_table_with_joins(&mut self, table: &ast::TableWithJoins) -> PlanResult {
        let mut result = self.visit_table_factor(&table.relation)?;

        if table.joins.is_empty() {
            return Ok(result);
        }

        let mut table_refs = vec![get_table_ref(&result)?];
        let joins = table.joins.iter();

        for join in joins {
            let join_node = self.visit_join(&result, join, &table_refs)?;
            table_refs.push(get_table_ref(&join_node.right)?);
            result = join_node.into();
        }

        Ok(result)
    }

    fn visit_order_by_expr(&mut self, order_by: &ast::OrderByExpr) -> PlanResult<OrderByExpr> {
        let expr = self.visit_expr(&order_by.expr)?;

        // null values sort as if larger than any non-null value, so
        // NULLS FIRST is the default for DESC order, and NULLS LAST otherwise.
        let asc = order_by.asc.unwrap_or(true);
        let nulls_first = order_by.nulls_first.unwrap_or(asc);

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
        })
    }

    fn visit_join(
        &mut self,
        left: &LogicalPlan,
        join: &ast::Join,
        left_refs: &[TableReference],
    ) -> PlanResult<Join> {
        let right = self.visit_table_factor(&join.relation)?;
        let right_refs = &[get_table_ref(&right)?];

        let on_expr;
        let filter_expr;

        let join_type = match &join.join_operator {
            ast::JoinOperator::Inner(constraint) => {
                (on_expr, filter_expr) =
                    self.visit_join_constraint(constraint, left_refs, right_refs)?;

                JoinType::Inner
            }
            // ast::JoinOperator::LeftOuter(constraint) => {
            //     (on_expr, filter_expr) = self.visit_join_constraint(constraint)?;
            //     JoinType::Left
            // }
            _ => todo!("{join}"),
        };

        Ok(Join {
            left: Rc::new(left.clone()),
            right: Rc::new(right),
            join_type,
            on: on_expr,
            filter: filter_expr,
        })
    }

    fn visit_join_constraint(
        &mut self,
        join_constraint: &ast::JoinConstraint,
        left_refs: &[TableReference],
        right_refs: &[TableReference],
    ) -> PlanResult<(Vec<EquijoinPredicate>, Option<Expr>)> {
        match join_constraint {
            ast::JoinConstraint::On(expr) => {
                let expr = self.visit_expr(expr)?;
                split_eq_and_noneq_join_predicate(&expr, left_refs, right_refs)
            }
            _ => unimplemented!("{join_constraint:?}"),
        }
    }

    fn visit_table_factor(&mut self, table_factor: &ast::TableFactor) -> PlanResult {
        match table_factor {
            ast::TableFactor::Table { name, alias, .. } => {
                let name = name.to_string();
                if name.starts_with('@') {
                    self.external_names.push(name.clone());
                }

                let mut result = LogicalPlan::TableScan(TableScan {
                    table_name: TableReference(name.into()),
                    projection: None,
                    filters: Vec::new(),
                    fetch: None,
                });

                if let Some(alias) = alias {
                    result = LogicalPlan::SubqueryAlias(SubqueryAlias {
                        alias: TableReference(alias.to_string().into()),
                        input: Rc::new(result),
                    });
                }

                Ok(result)
            }
            _ => unimplemented!("{table_factor}"),
        }
    }

    fn visit_projection(
        &mut self,
        projection: &[ast::SelectItem],
    ) -> PlanResult<IndexMap<Identifier, Expr>> {
        projection
            .iter()
            .map(|x| -> PlanResult<(Identifier, Expr)> {
                use ast::SelectItem;
                match x {
                    SelectItem::UnnamedExpr(expr) => {
                        let result = self.visit_expr(expr)?;
                        Ok((format!("{result}").into(), result))
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        Ok((alias.value.clone().into(), self.visit_expr(expr)?))
                    }
                    SelectItem::Wildcard(_) => {
                        let result = Expr::Wildcard(Wildcard { table: None });
                        Ok(("*".to_string().into(), result))
                    }
                    SelectItem::QualifiedWildcard(object_name, _) => {
                        let table = TableReference(object_name.to_string().into());
                        let result = Expr::Wildcard(Wildcard { table: Some(table) });
                        Ok((object_name.to_string().into(), result))
                    }
                }
            })
            .collect()
    }

    fn visit_expr(&mut self, expr: &ast::Expr) -> PlanResult<Expr> {
        let result = match expr {
            ast::Expr::Identifier(ident) => {
                if ident.value.starts_with('@') {
                    self.external_names.push(ident.value.clone())
                }

                Expr::Column(Column {
                    name: Identifier::new(ident.value.clone()),
                    relation: None,
                })
            }
            ast::Expr::CompoundIdentifier(ident) => Expr::Column(Column {
                name: Identifier::new(ident[ident.len() - 1].value.clone()),
                relation: if !ident.is_empty() {
                    Some(TableReference(ident[0].value.clone().into()))
                } else {
                    None
                },
            }),
            ast::Expr::Nested(nested) => self.visit_expr(nested)?,
            ast::Expr::UnaryOp { op, expr } => Expr::Unary(UnaryExpr {
                op: self.visit_unary_op(op)?,
                expr: Box::new(self.visit_expr(expr)?),
            }),
            ast::Expr::BinaryOp { left, op, right } => {
                let left = self.visit_expr(left)?;
                let op = self.visit_binary_op(op)?;
                let right = self.visit_expr(right)?;

                match (left, op, right) {
                    (left, Operator::Arrow, Expr::Column(c)) => Expr::GetAttr(GetAttr {
                        input: Box::new(left),
                        keys: vec![make_literal(c.name.to_string())],
                    }),
                    (left, Operator::Arrow, Expr::ScalarFunction(f)) => {
                        Expr::MethodCall(MethodCall {
                            input: Box::new(left),
                            name: f.name,
                            args: f.args,
                        })
                    }
                    (left, op, right) => left.binary_expr(right, op),
                }
            }
            ast::Expr::Value(value) => self.visit_value(value)?,
            ast::Expr::Named { expr, name } => Expr::Alias(Alias {
                name: name.value.clone(),
                expr: Box::new(self.visit_expr(expr)?),
            }),
            ast::Expr::Subscript { expr, subscript } => {
                let right = match subscript.as_ref() {
                    ast::Subscript::Index { index } => index,
                    _ => unimplemented!("{expr:?}"),
                };

                Expr::GetItem(GetItem {
                    input: Box::new(self.visit_expr(expr)?),
                    keys: vec![self.visit_expr(right)?],
                })
            }
            ast::Expr::Function(function) => {
                let name = function.name.to_string();
                if name.starts_with('@') {
                    self.external_names.push(name.clone())
                }
                let (args, kwargs) = self.visit_function_arguments(&function.args)?;
                if name == "try" {
                    if args.is_empty() {
                        return Err(PlannerError::SyntaxError(
                            "try() missing 1 required positional argument".to_string(),
                        ));
                    }

                    if !kwargs.is_empty() {
                        return Err(PlannerError::SyntaxError(
                            "try() got an unexpected keyword argument".to_string(),
                        ));
                    }

                    Expr::Try(Try { args })
                } else {
                    Expr::ScalarFunction(ScalarFunction {
                        name: Identifier::new(name),
                        args,
                        kwargs,
                    })
                }
            }
            ast::Expr::IsDistinctFrom(left, right) => Expr::Binary(BinaryExpr {
                left: Box::new(self.visit_expr(left)?),
                op: Operator::IsNot,
                right: Box::new(self.visit_expr(right)?),
            }),
            ast::Expr::IsNotDistinctFrom(left, right) => Expr::Binary(BinaryExpr {
                left: Box::new(self.visit_expr(left)?),
                op: Operator::Is,
                right: Box::new(self.visit_expr(right)?),
            }),
            ast::Expr::Tuple(elements) => Expr::Tuple(Tuple {
                elements: elements.iter().map(|x| self.visit_expr(x)).try_collect()?,
            }),
            ast::Expr::Array(array) => Expr::List(List {
                elements: array
                    .elem
                    .iter()
                    .map(|x| self.visit_expr(x))
                    .try_collect()?,
            }),
            ast::Expr::Dictionary(items) => Expr::Dict(Dict {
                items: items
                    .iter()
                    .map(|x| -> PlanResult<_> {
                        let key = make_literal(x.key.value.clone());
                        let value = self.visit_expr(&x.value)?;
                        Ok((key, value))
                    })
                    .try_collect()?,
            }),
            ast::Expr::MapAccess { column, keys } => Expr::GetItem(GetItem {
                input: Box::new(self.visit_expr(column)?),
                keys: keys.iter().map(|k| self.visit_expr(&k.key)).try_collect()?,
            }),
            _ => return Err(PlannerError::NotImplemented(format!("{expr:?}"))),
        };
        Ok(result)
    }

    fn visit_function_arguments(
        &mut self,
        args: &ast::FunctionArguments,
    ) -> PlanResult<(Vec<Expr>, IndexMap<Identifier, Expr>)> {
        let mut ordering = FunctionArgumentTypeOrdering::Positional;
        let mut positional = Vec::new();
        let mut keyword = IndexMap::default();

        match args {
            ast::FunctionArguments::None => {}
            ast::FunctionArguments::Subquery(_) => todo!(),
            ast::FunctionArguments::List(arg_list) => {
                for arg in arg_list.args.iter() {
                    match arg {
                        ast::FunctionArg::Named { name, arg, .. } => {
                            if ordering == FunctionArgumentTypeOrdering::Positional {
                                ordering = FunctionArgumentTypeOrdering::Keyword;
                            };
                            keyword.insert(
                                Identifier::new(name.to_string()),
                                self.visit_function_arg_expr(arg)?,
                            );
                        }
                        ast::FunctionArg::Unnamed(arg) => {
                            if ordering == FunctionArgumentTypeOrdering::Keyword {
                                return Err(PlannerError::SyntaxError(
                                    "positional argument follows keyword argument".to_string(),
                                ));
                            };
                            positional.push(self.visit_function_arg_expr(arg)?);
                        }
                    }
                }
            }
        };

        Ok((positional, keyword))
    }

    fn visit_function_arg_expr(&mut self, arg: &ast::FunctionArgExpr) -> PlanResult<Expr> {
        match arg {
            ast::FunctionArgExpr::Expr(e) => self.visit_expr(e),
            ast::FunctionArgExpr::QualifiedWildcard(_) => todo!(),
            ast::FunctionArgExpr::Wildcard => todo!(),
        }
    }

    fn visit_unary_op(&mut self, op: &ast::UnaryOperator) -> PlanResult<Operator> {
        use ast::UnaryOperator::*;
        Ok(match op {
            Plus => Operator::Plus,
            Minus => Operator::Minus,
            Not => Operator::Not,
            _ => unimplemented!("{op}"),
        })
    }

    fn visit_binary_op(&mut self, op: &ast::BinaryOperator) -> PlanResult<Operator> {
        use ast::BinaryOperator::*;
        Ok(match op {
            Plus => Operator::Plus,
            Minus => Operator::Minus,
            Multiply => Operator::Multiply,
            Divide => Operator::Divide,
            DuckIntegerDivide => Operator::IntegerDivide,
            Modulo => Operator::Modulo,
            Eq => Operator::Eq,
            Gt => Operator::Gt,
            GtEq => Operator::GtEq,
            Lt => Operator::Lt,
            LtEq => Operator::LtEq,
            And => Operator::And,
            Or => Operator::Or,
            Arrow => Operator::Arrow,
            BitwiseAnd => Operator::BitAnd,
            BitwiseOr => Operator::BitOr,
            _ => unimplemented!("{op}"),
        })
    }

    fn visit_value(&mut self, value: &ast::Value) -> PlanResult<Expr> {
        let value: PyObject = Python::with_gil(|py| match value {
            ast::Value::Number(v, _) => {
                if v.contains('.') | v.contains('e') | v.contains('E') {
                    PyFloat::type_object_bound(py).call1((v,)).unwrap().unbind()
                } else {
                    PyLong::type_object_bound(py).call1((v,)).unwrap().unbind()
                }
            }
            ast::Value::SingleQuotedString(v) => v.into_py(py),
            ast::Value::DoubleQuotedString(v) => v.into_py(py),
            ast::Value::TripleSingleQuotedString(v) => v.into_py(py),
            ast::Value::TripleDoubleQuotedString(v) => v.into_py(py),
            ast::Value::Boolean(v) => v.into_py(py),
            ast::Value::Null => py.None(),
            // ast::Value::Placeholder(_) => unimplemented!(),
            _ => unimplemented!("{value}"),
        });

        Ok(make_literal(value))
    }
}

fn get_table_ref(plan: &LogicalPlan) -> PlanResult<TableReference> {
    let table_ref = match plan {
        LogicalPlan::TableScan(v) => v.table_name.clone(),
        LogicalPlan::SubqueryAlias(v) => v.alias.clone(),
        LogicalPlan::Projection(v) => get_table_ref(&v.input)?,
        LogicalPlan::Filter(v) => get_table_ref(&v.input)?,
        LogicalPlan::EmptyRelation(_) => unreachable!(),
        LogicalPlan::Join(_) => unreachable!(),
        LogicalPlan::Sort(v) => get_table_ref(&v.input)?,
        LogicalPlan::Limit(v) => get_table_ref(&v.input)?,
    };
    Ok(table_ref)
}

fn make_literal<T: IntoPy<PyObject>>(value: T) -> Expr {
    let value = Python::with_gil(|py| value.into_py(py));
    Expr::Literal(Rc::new(value))
}

fn split_eq_and_noneq_join_predicate(
    filter: &Expr,
    left_refs: &[TableReference],
    right_refs: &[TableReference],
) -> PlanResult<(Vec<EquijoinPredicate>, Option<Expr>)> {
    let exprs = filter.split_binary_expression(Operator::And);

    let mut join_keys: Vec<(Expr, Expr)> = vec![];
    // Conditions like a = 10, will be added to non-equijoin.
    let mut filters: Vec<Expr> = vec![];

    for expr in exprs {
        match expr {
            Expr::Binary(ref binary_expr) if binary_expr.op == Operator::Eq => {
                let join_key_pair = find_valid_equijoin_key_pair(
                    &binary_expr.left,
                    &binary_expr.right,
                    left_refs,
                    right_refs,
                );

                if let Some((left_expr, right_expr)) = join_key_pair {
                    join_keys.push((left_expr, right_expr));
                } else {
                    filters.push(expr);
                }
            }
            _ => filters.push(expr),
        }
    }

    let filter_expr = filters.into_iter().reduce(Expr::and);
    Ok((join_keys, filter_expr))
}

fn find_valid_equijoin_key_pair(
    left: &Expr,
    right: &Expr,
    left_refs: &[TableReference],
    right_refs: &[TableReference],
) -> Option<EquijoinPredicate> {
    if check_all_columns_from_relation(left, left_refs)
        && check_all_columns_from_relation(right, right_refs)
    {
        Some((left.clone(), right.clone()))
    } else if check_all_columns_from_relation(left, right_refs)
        && check_all_columns_from_relation(right, left_refs)
    {
        Some((right.clone(), left.clone()))
    } else {
        None
    }
}

fn check_all_columns_from_relation(expr: &Expr, tables: &[TableReference]) -> bool {
    let columns = expr.extract_columns();
    columns
        .iter()
        .filter_map(|c| c.relation.as_ref())
        .all(|r| tables.contains(r))
}

// pub fn try_unzip<I, C, T, E>(iter: I) -> Result<C, E>
// where
//     I: IntoIterator<Item = Result<T, E>>,
//     C: Extend<T> + Default,
// {
//     iter.into_iter().try_fold(C::default(), |mut c, r| {
//         c.extend([r?]);
//         Ok(c)
//     })
// }
