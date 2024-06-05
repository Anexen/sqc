#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

#[macro_use]
extern crate log;

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::{collections::HashMap, sync::Arc};

use datafusion_common::{
    arrow::datatypes::{DataType, Field, Schema},
    config::ConfigOptions,
    DataFusionError, ScalarValue,
};
use datafusion_expr::{
    builder::LogicalTableSource, AggregateUDF, ScalarUDF,
    TableSource, WindowUDF,
};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_sql::{
    planner::ContextProvider,
    sqlparser::{dialect::GenericDialect, parser::Parser},
    TableReference,
};
use errors::SqcError;
use pyo3::{
    exceptions,
    prelude::*,
    types::{self, IntoPyDict, PyDict, PyList, PyNone},
};
// use sqlparser::{dialect::GenericDialect, parser::Parser};

mod errors;
mod executor;
// mod logical_plan;
// mod parser;
// mod planner;
// mod expr;
// mod scalar;
// mod schema;

#[pymodule]
pub fn sqc(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    // fn sqc(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(query, m)?)?;
    // m.add_function(wrap_pyfunction!(demo, m)?)?;
    Ok(())
}

#[pyfunction]
pub fn query<'p>(
    py: Python<'p>,
    query: &str,
    // values: Vec<Bound<'p, PyDict>>,
    tables: &Bound<'p, PyAny>,
) -> PyResult<Vec<Bound<'p, PyDict>>> {
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, query).unwrap();
    let statement = &ast[0];

    let mut schema_provider = MyContextProvider::new();
    let mut execution_context = executor::ExecutionContext::new(py);

    if !tables.is_none() {
        match tables.downcast::<PyList>() {
            Ok(values) => {
                let dataset = values.extract::<Vec<_>>()?;
                schema_provider.add_dataset("dataset", &dataset);
                execution_context
                    .tables
                    .insert("dataset".to_string(), dataset);
            }
            Err(_) => {
                match tables.downcast::<PyDict>() {
                    Ok(tables) => {
                        for item in tables.items() {
                            let name = item.get_item(0).unwrap().extract::<String>()?;
                            let dataset = item.get_item(1).unwrap().extract::<Vec<_>>()?;
                            schema_provider.add_dataset(&name, &dataset);
                            execution_context.tables.insert(name, dataset);
                        }
                    }
                    Err(e) => return Err(exceptions::PyTypeError::new_err(e.to_string())),
                };
            }
        }
    }

    // create a logical query plan
    let sql_to_rel = datafusion_sql::planner::SqlToRel::new(&schema_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    // show the plan
    debug!("{:?}", &plan);

    Ok(executor::execute_plan(&plan, &execution_context))
}

pub fn infer_schema(value: &Bound<'_, PyDict>) -> Schema {
    match infer_field_schema(value) {
        DataType::Struct(fields) => Schema::new(fields),
        _ => unreachable!(),
    }
}

/// Infer the data type of a value
pub fn infer_field_schema(value: &Bound<'_, PyAny>) -> DataType {
    if value.is_instance_of::<types::PyInt>() {
        DataType::Int64
    } else if value.is_instance_of::<types::PyString>() {
        DataType::Utf8
    } else if value.is_instance_of::<types::PyBytes>() {
        DataType::Binary
    } else if value.is_instance_of::<types::PyBool>() {
        DataType::Boolean
    } else if value.is_instance_of::<types::PyDate>() {
        DataType::Date64
    } else if value.is_instance_of::<types::PyList>() {
        DataType::new_list(infer_field_schema(&value.get_item(0).unwrap()), false)
    } else if value.is_instance_of::<types::PyDict>() {
        DataType::Struct(
            value
                .downcast::<PyDict>()
                .unwrap()
                .items()
                .into_iter()
                .map(|kv| {
                    Field::new(
                        kv.get_item(0).unwrap().to_string(),
                        infer_field_schema(&kv.get_item(1).unwrap()),
                        false,
                    )
                })
                .collect(),
        )
    } else {
        unimplemented!("{:?} is not supported", value.getattr("__class__").unwrap())
    }
}

struct MyContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl MyContextProvider {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            options: Default::default(),
        }
    }

    pub fn add_dataset(&mut self, name: &str, dataset: &Vec<Bound<'_, PyDict>>) {
        let row = dataset.first().unwrap();

        self.tables
            .insert(name.to_string(), create_table_source(infer_schema(row)));
    }
}

fn create_table_source(schema: Schema) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(schema)))
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> Result<Arc<dyn TableSource>, DataFusionError> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(table.clone()),
            _ => datafusion_common::plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        let meta = match name {
            "round" => ScalarUDF::from(RoundUDF::new()),
            _ => return None,
        };
        Some(Arc::new(meta))
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udfs_names(&self) -> Vec<String> {
        vec!["round".to_string()]
    }

    fn udafs_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwfs_names(&self) -> Vec<String> {
        Vec::new()
    }
}

#[derive(Debug)]
pub struct RoundUDF {
    signature: Signature,
}

impl RoundUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Float64, DataType::Int64, DataType::UInt64],
                Volatility::Immutable,
            ),
        }
    }
}

pub fn round(number: f64, rounding: i32) -> f64 {
    let scale: f64 = 10_f64.powi(rounding);
    (number * scale).round() / scale
}

impl ScalarUDFImpl for RoundUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        match (&args[0], &args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Float64(Some(v))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(precision))),
            ) => {
                let result = round(*v, *precision as i32);
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(result))))
            }
            _ => Err(DataFusionError::NotImplemented("".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pyo3::{
        types::{IntoPyDict, PyDict, PyList},
        IntoPy, Py, Python,
    };
    use rstest::{fixture, rstest};
}
