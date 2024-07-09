use derive_more::{Display, Error, From};
// use optimizer::Optimizer;
use pyo3::{
    create_exception,
    exceptions::PyNameError,
    prelude::*,
    types::{IntoPyDict as _, PyDict},
};

mod executor;
mod functions;
mod logical_plan;
// mod optimizer;
mod parser;
mod planner;
mod stream;

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
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (query, data=None))]
pub fn query(py: Python<'_>, query: &str, data: Option<PyObject>) -> PyResult<PyObject> {
    let ast = parser::parse_query(query)?;
    let plan = planner::prepare_plan(&ast)?;

    // let plan = Optimizer::default().optimize(plan.logical_plan);

    let mut ctx = ExecutionContext::new();

    if !plan.external_names.is_empty() {
        try_extract_variables_from_scope(py, &plan.external_names, &mut ctx).ok();
    }

    if let Some(data) = data {
        if let Ok(tables) = data.bind(py).downcast::<PyDict>() {
            for (k, v) in tables {
                if v.is_callable() {
                    ctx.add_scalar_udf(&k.to_string(), v.unbind())
                } else {
                    ctx.add_table(&k.to_string(), v.into());
                }
            }
        } else {
            ctx.add_table("data", data);
        };
    }

    let stream = execute_plan(py, &plan.logical_plan, &mut ctx)?;

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
}

#[pyfunction]
pub fn parse(query: &str) -> PyResult<String> {
    let ast = parser::parse_query(query)?;
    Ok(format!("{ast:#?}"))
}

// #[pyfunction(name = "explain")]
// pub fn explain_(query: &str) -> PyResult<String> {
//     let ast = parser::parse_query(query)?;
//     let plan = planner::prepare_plan(&ast)?;
//     let result = explain::explain(&plan);
//     Ok(result.to_string())
// }

#[derive(Debug, Display, Error, From)]
pub enum SqcError {
    #[display(fmt = "query parsing error")]
    ParserError(parser::QueryParserError),
    #[display(fmt = "query planning error")]
    PlannerError(logical_plan::PlanError),
    #[display(fmt = "table not found: {_0}")]
    TableNotFound(#[error(not(source))] String),
    #[display(fmt = "Runtime Error")]
    RuntimeError(PyErr),
}

impl From<parser::QueryParserError> for PyErr {
    fn from(value: parser::QueryParserError) -> Self {
        PyParserError::new_err(value.to_string())
    }
}

impl From<logical_plan::PlanError> for PyErr {
    fn from(value: logical_plan::PlanError) -> Self {
        PySqcError::new_err(value.to_string())
    }
}

impl From<SqcError> for PyErr {
    fn from(value: SqcError) -> Self {
        PySqcError::new_err(value.to_string())
    }
}

fn try_extract_variables_from_scope(
    py: Python<'_>,
    variables: &[String],
    ctx: &mut ExecutionContext,
) -> PyResult<()> {
    let locals =
        unsafe { Py::<PyDict>::from_borrowed_ptr_or_err(py, pyo3::ffi::PyEval_GetLocals()) }?;

    let globals =
        unsafe { Py::<PyDict>::from_borrowed_ptr_or_err(py, pyo3::ffi::PyEval_GetGlobals()) }?;

    let locals = locals.bind(py);
    let globals = globals.bind(py);

    for name in variables {
        let _name = &name[1..];
        let value = if let Ok(Some(value)) = locals.get_item(_name) {
            value
        } else if let Ok(Some(value)) = globals.get_item(_name) {
            value
        } else {
            return Err(PyNameError::new_err(format!(
                "variable `{}` is not defined",
                _name
            )));
        };

        if value.is_callable() {
            ctx.add_scalar_udf(name, value.unbind())
        } else {
            ctx.add_table(name, value.unbind());
        }
    }

    Ok(())
}
