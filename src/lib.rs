use derive_more::{Display, Error, From};
use pyo3::{
    create_exception,
    prelude::*,
    types::{IntoPyDict as _, PyDict},
};

mod executor;
mod explain;
mod logical_plan;
mod parser;
mod planner;

pub use executor::{execute_plan, ExecutionContext};
pub use parser::parse_query;
pub use planner::prepare_plan;

create_exception!("sqc", PySqcError, pyo3::exceptions::PyException);
create_exception!("sqc", PyParserError, PySqcError);

#[pymodule]
pub fn sqc(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SqcError", py.get_type_bound::<PySqcError>())?;
    m.add("ParserError", py.get_type_bound::<PyParserError>())?;

    m.add_function(wrap_pyfunction!(query, m)?)?;
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    m.add_function(wrap_pyfunction!(explain_, m)?)?;
    Ok(())
}

#[pyfunction]
pub fn query(query: &str, data: Option<PyObject>) -> PyResult<PyObject> {
    let ast = parser::parse_query(query)?;
    let plan = planner::prepare_plan(&ast)?;
    // let plan = optimize_plan(plan);

    Python::with_gil(|py| {
        let mut ctx = ExecutionContext::new();
        if let Some(data) = data {
            if let Ok(tables) = data.bind(py).downcast::<PyDict>() {
                for (k, v) in tables {
                    ctx.add_table(&k.to_string(), v.into());
                }
            } else {
                ctx.add_table("data", data);
            };
        }

        let stream = execute_plan(py, &plan, &ctx).map_err(PyErr::from)?;

        stream
            .map(|row| {
                row.map(|p| {
                    p.into_values()
                        .flat_map(|v| v.into_iter())
                        .collect::<Vec<_>>()
                        .into_py_dict_bound(py)
                        .unbind()
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|x| x.into_py(py))
            .map_err(PyErr::from)

        // .map(|s| s.collect::<Result<Vec<_>, executor::ExecError>>())
        // .map(|s| s.into_iter().map().into_py(py))
        // Ok(pyo3::types::PyList::empty_bound(py).into_py(py))
    })
}

#[pyfunction]
pub fn parse(query: &str) -> PyResult<String> {
    let ast = parser::parse_query(query)?;
    Ok(format!("{ast:#?}"))
}

#[pyfunction(name = "explain")]
pub fn explain_(query: &str) -> PyResult<String> {
    let ast = parser::parse_query(query)?;
    let plan = planner::prepare_plan(&ast)?;
    let result = explain::explain(&plan);
    Ok(result.to_string())
}

#[derive(Debug, Display, Error, From)]
pub enum SqcError {
    #[display(fmt = "query parsing error")]
    ParserError(parser::ParserError),
    #[display(fmt = "query planning error")]
    PlannerError(logical_plan::PlanError),
    #[display(fmt = "table not found: {_0}")]
    TableNotFound(#[error(not(source))] String),
    #[display(fmt = "Runtime Error")]
    RuntimeError(PyErr),
}

impl From<parser::ParserError> for PyErr {
    fn from(value: parser::ParserError) -> Self {
        PyParserError::new_err(value.to_string())
    }
}

impl From<logical_plan::PlanError> for PyErr {
    fn from(value: logical_plan::PlanError) -> Self {
        PySqcError::new_err(value.to_string())
    }
}

impl From<executor::ExecError> for PyErr {
    fn from(value: executor::ExecError) -> Self {
        PySqcError::new_err(value.to_string())
    }
}

impl From<SqcError> for PyErr {
    fn from(value: SqcError) -> Self {
        PySqcError::new_err(value.to_string())
    }
}
