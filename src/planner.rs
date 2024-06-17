use itertools::Itertools;
use std::any::Any;

use indexmap::IndexMap;
use pyo3::prelude::*;
use sqlparser::ast;

use crate::logical_plan::*;

type PlanResult<T = LogicalPlan> = Result<T, PlanError>;
type EquijoinPredicate = (Expr, Expr);

pub fn prepare_plan(query: &ast::Query) -> PlanResult {
    Visitor.visit_query(query)
}

struct Visitor;

impl Visitor {
    fn visit_query(&self, query: &ast::Query) -> PlanResult {
        let mut result = self.visit_set_expr(query.body.as_ref(), query.order_by.as_ref())?;

        if let Some(limit) = query.limit.as_ref() {
            let offset = query
                .offset
                .as_ref()
                .map(|offset| self.visit_expr(&offset.value))
                .transpose()?;

            result = LimitBuilder::default()
                .input(result.into())
                .limit(self.visit_expr(limit)?)
                .offset(offset)
                .build()?
                .into();
        }

        Ok(result)
    }

    fn visit_set_expr(&self, expr: &ast::SetExpr, order_by: &[ast::OrderByExpr]) -> PlanResult {
        match expr {
            ast::SetExpr::Select(select) => self.visit_select(select, order_by),
            ast::SetExpr::Query(query) => self.visit_query(query),
            _ => unimplemented!("{:?}", expr.type_id()),
        }
    }

    fn visit_select(&self, select: &ast::Select, order_by: &[ast::OrderByExpr]) -> PlanResult {
        let mut result = self.visit_from(&select.from)?;
        if let Some(predicate) = &select.selection {
            result = FilterBuilder::default()
                .predicate(self.visit_expr(predicate)?)
                .input(result)
                .build()?
                .into();
        }

        if !order_by.is_empty() {
            let sort_expr = order_by
                .iter()
                .map(|o| self.visit_order_by_expr(o))
                .try_collect()?;

            result = SortBuilder::default()
                .expr(sort_expr)
                .input(result)
                .build()?
                .into();
        }

        let projection = self.visit_projection(&select.projection)?;
        result = ProjectionBuilder::default()
            .expr(projection)
            .input(result)
            .build()?
            .into();

        Ok(result)
    }

    fn visit_from(&self, from: &[ast::TableWithJoins]) -> PlanResult {
        if from.is_empty() {
            return Ok(EmptyRelationBuilder::default().build()?.into());
        };

        let scans = from
            .iter()
            .map(|t| self.visit_table_with_joins(t))
            .collect::<PlanResult<Vec<_>>>()?;

        if scans.len() > 1 {
            todo!("cross_join")
        }

        Ok(scans.into_iter().next().unwrap())
    }

    fn visit_table_with_joins(&self, table: &ast::TableWithJoins) -> PlanResult {
        let mut result = self.visit_table_factor(&table.relation)?;

        if table.joins.is_empty() {
            return Ok(result);
        }

        let mut table_refs = vec![self.get_table_ref(&result)?];
        let joins = table.joins.iter();

        for join in joins {
            let join_node = self.visit_join(&result, join, &mut table_refs)?;
            table_refs.push(self.get_table_ref(&join_node.right)?);
            result = join_node.into();
        }

        Ok(result)
    }

    fn visit_order_by_expr(&self, order_by: &ast::OrderByExpr) -> PlanResult<OrderByExpr> {
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
        &self,
        left: &LogicalPlan,
        join: &ast::Join,
        left_refs: &[TableReference],
    ) -> PlanResult<Join> {
        let right = self.visit_table_factor(&join.relation)?;
        let right_refs = &[self.get_table_ref(&right)?];

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

        JoinBuilder::default()
            .left(left.clone())
            .right(right)
            .join_type(join_type)
            .on(on_expr)
            .filter(filter_expr)
            .build()
    }

    fn visit_join_constraint(
        &self,
        join_constraint: &ast::JoinConstraint,
        left_refs: &[TableReference],
        right_refs: &[TableReference],
    ) -> PlanResult<(Vec<EquijoinPredicate>, Option<Expr>)> {
        match join_constraint {
            ast::JoinConstraint::On(expr) => {
                let expr = self.visit_expr(expr)?;
                self.split_eq_and_noneq_join_predicate(&expr, left_refs, right_refs)
            }
            _ => unimplemented!("{join_constraint:?}"),
        }
    }

    fn visit_table_factor(&self, table_factor: &ast::TableFactor) -> PlanResult {
        match table_factor {
            ast::TableFactor::Table { name, alias, .. } => {
                let mut result = TableScanBuilder::default()
                    .table_name(TableReference(name.to_string().into()))
                    .build()?
                    .into();

                if let Some(alias) = alias {
                    result = SubqueryAliasBuilder::default()
                        .alias(TableReference(alias.to_string().into()))
                        .input(result)
                        .build()?
                        .into();
                }

                Ok(result)
            }
            _ => unimplemented!("{table_factor}"),
        }
    }

    fn visit_projection(
        &self,
        projection: &[ast::SelectItem],
    ) -> PlanResult<IndexMap<String, Expr>> {
        projection
            .iter()
            .map(|x| -> PlanResult<(String, Expr)> {
                use ast::SelectItem;
                match x {
                    SelectItem::UnnamedExpr(expr) => {
                        let result = self.visit_expr(expr)?;
                        Ok((format!("{result}"), result))
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        Ok((alias.value.clone(), self.visit_expr(expr)?))
                    }
                    SelectItem::Wildcard(_) => {
                        let result = WildcardBuilder::default().build()?.into();
                        Ok(("*".to_string(), result))
                    }
                    SelectItem::QualifiedWildcard(object_name, _) => {
                        let table_ref = TableReference(object_name.to_string().into());
                        let result = WildcardBuilder::default().table(table_ref).build()?.into();
                        Ok((object_name.to_string(), result))
                    }
                }
            })
            .collect()
    }

    fn visit_expr(&self, expr: &ast::Expr) -> PlanResult<Expr> {
        let result = match expr {
            ast::Expr::Identifier(ident) => ColumnBuilder::default()
                .name(ident.value.clone())
                .build()?
                .into(),
            ast::Expr::CompoundIdentifier(ident) => ColumnBuilder::default()
                .name(ident[ident.len() - 1].value.clone())
                .relation(if !ident.is_empty() {
                    Some(TableReference(ident[0].value.clone().into()))
                } else {
                    None
                })
                .build()?
                .into(),
            ast::Expr::Nested(nested) => self.visit_expr(nested)?,
            ast::Expr::UnaryOp { op, expr } => UnaryExprBuilder::default()
                .op(self.visit_unary_op(op)?)
                .expr(self.visit_expr(expr)?)
                .build()?
                .into(),
            ast::Expr::BinaryOp { left, op, right } => BinaryExprBuilder::default()
                .left(self.visit_expr(left)?)
                .op(self.visit_binary_op(op)?)
                .right(self.visit_expr(right)?)
                .build()?
                .into(),
            ast::Expr::Value(value) => self.visit_value(value)?,
            ast::Expr::Named { expr, name } => AliasBuilder::default()
                .expr(self.visit_expr(expr)?)
                .name(name.value.clone())
                .build()?
                .into(),
            ast::Expr::Subscript { expr, subscript } => {
                let right = match subscript.as_ref() {
                    ast::Subscript::Index { index } => index,
                    _ => unimplemented!("{expr:?}"),
                };

                BinaryExprBuilder::default()
                    .left(self.visit_expr(expr)?)
                    .op(Operator::Arrow)
                    .right(self.visit_expr(right)?)
                    .build()?
                    .into()
            }
            _ => unimplemented!("{expr:?}"),
        };
        Ok(result)
    }

    fn visit_unary_op(&self, op: &ast::UnaryOperator) -> PlanResult<Operator> {
        use ast::UnaryOperator::*;
        Ok(match op {
            Plus => Operator::Plus,
            Minus => Operator::Minus,
            Not => Operator::Not,
            _ => unimplemented!("{op}"),
        })
    }

    fn visit_binary_op(&self, op: &ast::BinaryOperator) -> PlanResult<Operator> {
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
            _ => unimplemented!("{op}"),
        })
    }

    fn visit_value(&self, value: &ast::Value) -> PlanResult<Expr> {
        let py_value = Python::with_gil(|py| match value {
            ast::Value::Number(v, _) => Some(v.parse::<i64>().unwrap().into_py(py)),
            ast::Value::SingleQuotedString(v) => Some(v.into_py(py)),
            ast::Value::Boolean(v) => Some(v.into_py(py)),
            ast::Value::Null => None,
            // ast::Value::Placeholder(_) => unimplemented!(),
            _ => unimplemented!("{value}"),
        });
        Ok(ScalarValue(py_value).into())
    }

    fn get_table_ref(&self, plan: &LogicalPlan) -> PlanResult<TableReference> {
        let table_ref = match plan {
            LogicalPlan::TableScan(v) => v.table_name.clone(),
            LogicalPlan::SubqueryAlias(v) => v.alias.clone(),
            LogicalPlan::Projection(v) => self.get_table_ref(&v.input)?,
            LogicalPlan::Filter(v) => self.get_table_ref(&v.input)?,
            LogicalPlan::EmptyRelation(_) => unreachable!(),
            LogicalPlan::Join(_) => unreachable!(),
            LogicalPlan::Sort(v) => self.get_table_ref(&v.input)?,
            LogicalPlan::Limit(v) => self.get_table_ref(&v.input)?,
        };
        Ok(table_ref)
    }

    fn split_eq_and_noneq_join_predicate(
        &self,
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
                Expr::BinaryExpr(ref binary_expr) if binary_expr.op == Operator::Eq => {
                    let join_key_pair = self.find_valid_equijoin_key_pair(
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
        &self,
        left: &Expr,
        right: &Expr,
        left_refs: &[TableReference],
        right_refs: &[TableReference],
    ) -> Option<EquijoinPredicate> {
        if self.check_all_columns_from_relation(left, left_refs)
            && self.check_all_columns_from_relation(right, right_refs)
        {
            Some((left.clone(), right.clone()))
        } else if self.check_all_columns_from_relation(left, right_refs)
            && self.check_all_columns_from_relation(right, left_refs)
        {
            Some((right.clone(), left.clone()))
        } else {
            None
        }
    }

    fn check_all_columns_from_relation(&self, expr: &Expr, tables: &[TableReference]) -> bool {
        let columns = expr.extract_columns();
        columns
            .iter()
            .filter_map(|c| c.relation.as_ref())
            .all(|r| tables.contains(r))
    }
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
