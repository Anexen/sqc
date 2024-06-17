use derive_visitor::{Drive, Visitor};
use itertools::Itertools;

use crate::logical_plan::*;

pub fn explain(plan: &LogicalPlan) -> String {
    let mut visitor = Explain::default();
    plan.drive(&mut visitor);
    visitor.content.join("")
}

#[derive(Default, Visitor)]
#[visitor(Filter, SubqueryAlias, TableScan, Projection, Join, Sort, Limit)]
pub struct Explain {
    content: Vec<String>,
    depth: usize,
}

impl Explain {
    fn enter_table_scan(&mut self, table_scan: &TableScan) {
        self.push_indent();
        self.content
            .push(format!("TableScan: {}\n", table_scan.table_name));

        self.depth += 1;
    }

    fn exit_table_scan(&mut self, _table_scan: &TableScan) {
        self.depth -= 1;
    }

    fn enter_join(&mut self, join: &Join) {
        self.push_indent();
        self.content.push(format!(
            "{} Join: [{}] filter: {}\n",
            join.join_type,
            join.on
                .iter()
                .map(|x| format!("{} = {}", x.0, x.1))
                .join(", "),
            join.filter
                .as_ref()
                .map(|f| format!("{f}"))
                .unwrap_or_default(),
        ));

        self.depth += 1;
    }

    fn exit_join(&mut self, _join: &Join) {
        self.depth -= 1;
    }

    fn enter_filter(&mut self, filter: &Filter) {
        self.push_indent();
        self.content
            .push(format!("Filter: [{}]\n", filter.predicate));
        self.depth += 1;
    }

    fn exit_filter(&mut self, _filter: &Filter) {
        self.depth -= 1;
    }

    fn enter_subquery_alias(&mut self, alias: &SubqueryAlias) {
        self.push_indent();
        self.content
            .push(format!("SubqueryAlias: {}\n", alias.alias));

        self.depth += 1;
    }

    fn exit_subquery_alias(&mut self, _alias: &SubqueryAlias) {
        self.depth -= 1;
    }

    fn enter_projection(&mut self, projection: &Projection) {
        self.push_indent();
        self.content.push(format!(
            "Projection: [{}]\n",
            projection
                .expr
                .iter()
                .map(|(name, expr)| format!("{expr} AS {name}"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        self.depth += 1;
    }

    fn exit_projection(&mut self, _projection: &Projection) {
        self.depth -= 1;
    }

    fn enter_sort(&mut self, sort: &Sort) {
        self.push_indent();
        self.content.push(format!(
            "Sort: [{}]\n",
            sort.expr.iter().map(|e| e.to_string()).join(", ")
        ));
        self.depth += 1;
    }

    fn exit_sort(&mut self, _sort: &Sort) {
        self.depth -= 1;
    }

    fn enter_limit(&mut self, limit: &Limit) {
        self.push_indent();
        self.content.push(format!(
            "Limit: {}, Offset: {:?}\n",
            limit.limit, limit.offset
        ));
        self.depth += 1;
    }

    fn exit_limit(&mut self, _limit: &Limit) {
        self.depth -= 1;
    }

    fn push_indent(&mut self) {
        self.content.push("  ".repeat(self.depth))
    }
}
