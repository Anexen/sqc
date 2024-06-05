use std::hash::{DefaultHasher, Hash, Hasher};

use datafusion_common::{arrow::datatypes::DataType, ScalarValue};
use datafusion_expr::{
    expr::AggregateFunctionDefinition, AggregateFunction, ColumnarValue, Expr, Operator,
};
use pyo3::{
    types::{PyAnyMethods, PyDict, PyDictMethods, PyNone},
    Bound, Py, PyAny, Python, ToPyObject,
};

pub fn make_hash<T: Hash>(value: T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub fn scalar_to_py_any(py: Python, value: &ScalarValue) -> Py<PyAny> {
    match value {
        ScalarValue::Int64(Some(v)) => v.to_object(py).into(),
        ScalarValue::Float64(Some(v)) => v.to_object(py).into(),
        ScalarValue::Utf8(Some(v)) => v.to_object(py).into(),
        ScalarValue::Boolean(Some(v)) => v.to_object(py).into(),
        v if v.is_null() => PyNone::get_bound(py).to_object(py).into(),
        _ => unimplemented!("Value {}", value),
    }
}

pub fn evaluate_agg_expr<'p>(expr: &Expr, rows: &Vec<Bound<'p, PyDict>>) -> ScalarValue {
    match expr {
        Expr::AggregateFunction(aggfunc) => match aggfunc.func_def {
            AggregateFunctionDefinition::BuiltIn(AggregateFunction::Sum) => {
                let expr = aggfunc.args.first().unwrap();
                let mut result = ScalarValue::new_zero(&DataType::Int64).unwrap();
                for row in rows {
                    result = result.add(evaluate_expr(expr, row)).unwrap();
                }
                result
            }
            _ => unimplemented!(),
        },
        _ => panic!("Unsupported expression: {:?}", expr),
    }
}

pub fn evaluate_expr<'p>(expr: &Expr, row: &Bound<'p, PyDict>) -> ScalarValue {
    match expr {
        Expr::Literal(lit) => lit.clone(),
        Expr::Alias(alias) => evaluate_expr(&alias.expr, row),
        Expr::Column(column) => match row.get_item(&column.name).unwrap() {
            None => ScalarValue::Null,
            Some(value) => {
                let data_type = crate::infer_field_schema(&value);
                match data_type {
                    DataType::Null => ScalarValue::Null,
                    DataType::Boolean => ScalarValue::Boolean(value.extract::<bool>().ok()),
                    DataType::Int64 => ScalarValue::Int64(value.extract::<i64>().ok()),
                    DataType::Float64 => ScalarValue::Float64(value.extract::<f64>().ok()),
                    DataType::Utf8 => ScalarValue::Utf8(value.extract::<String>().ok()),
                    _ => unimplemented!(),
                }
            }
        },
        Expr::BinaryExpr(binary_expr) => {
            let left_val = evaluate_expr(&binary_expr.left, row);
            let right_val = evaluate_expr(&binary_expr.right, row);
            match binary_expr.op {
                Operator::Plus => left_val.add(right_val).unwrap(),
                Operator::Minus => left_val.sub(right_val).unwrap(),
                Operator::Multiply => left_val.mul(right_val).unwrap(),
                Operator::Divide => left_val.div(right_val).unwrap(),
                Operator::Eq => left_val.eq(&right_val).into(),
                Operator::Gt => left_val.gt(&right_val).into(),
                Operator::GtEq => left_val.ge(&right_val).into(),
                Operator::Lt => left_val.lt(&right_val).into(),
                Operator::LtEq => left_val.le(&right_val).into(),
                Operator::And => matches!(
                    (left_val, right_val),
                    (
                        ScalarValue::Boolean(Some(true)),
                        ScalarValue::Boolean(Some(true))
                    )
                )
                .into(),
                Operator::Or => matches!(
                    (left_val, right_val),
                    (ScalarValue::Boolean(Some(true)), _) | (_, ScalarValue::Boolean(Some(true))),
                )
                .into(),
                _ => panic!("Unsupported binary expression: {}", binary_expr),
            }
        }
        Expr::ScalarFunction(scalar_function) => match &scalar_function.func_def {
            datafusion_expr::ScalarFunctionDefinition::UDF(udf) => {
                let args: Vec<ColumnarValue> = scalar_function
                    .args
                    .iter()
                    .map(|arg| evaluate_expr(arg, row).into())
                    .collect();

                let udf_result = udf.invoke(&args).unwrap();

                match udf_result {
                    ColumnarValue::Array(_) => unimplemented!(),
                    ColumnarValue::Scalar(scalar) => scalar,
                }
            }
        },
        _ => panic!("Unsupported expression: {:?}", expr),
    }
}
