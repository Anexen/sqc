use itertools::Itertools;
// use optimizer::Optimizer;
use pyo3::{
    create_exception,
    prelude::*,
    types::{IntoPyDict as _, PyDict},
};

#[macro_use]
mod macros;

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

create_exception!("sqc", SqcError, pyo3::exceptions::PyBaseException);
create_exception!("sqc", ParserError, SqcError);
create_exception!("sqc", PlannerError, SqcError);

impl From<parser::ParserError> for PyErr {
    fn from(value: parser::ParserError) -> Self {
        ParserError::new_err(value.to_string())
    }
}

impl From<planner::PlannerError> for PyErr {
    fn from(value: planner::PlannerError) -> Self {
        PlannerError::new_err(value.to_string())
    }
}

#[pymodule]
pub fn sqc(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SqcError", py.get_type_bound::<SqcError>())?;
    m.add("ParserError", py.get_type_bound::<ParserError>())?;
    m.add("PlanError", py.get_type_bound::<PlannerError>())?;

    m.add_function(wrap_pyfunction!(query, m)?)?;
    m.add_function(wrap_pyfunction!(parse, m)?)?;
    m.add_function(wrap_pyfunction!(prepare, m)?)?;
    m.add_function(wrap_pyfunction!(explain_, m)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (query, data=None))]
pub fn query(py: Python<'_>, query: &str, data: Option<PyObject>) -> PyResult<PyObject> {
    let ast = parser::parse_query(query)?;
    let plan = planner::prepare_plan(&ast)?;

    // let plan = Optimizer::default().optimize(plan.logical_plan);
    execute(py, &plan, data)
}

#[pyclass]
#[pyo3(unsendable)]
pub struct PreparedQuery {
    plan: planner::PreparedPlan,
}

#[pymethods]
impl PreparedQuery {
    #[pyo3(signature = (data=None))]
    fn execute(&self, py: Python<'_>, data: Option<PyObject>) -> PyResult<PyObject> {
        execute(py, &self.plan, data)
    }
}

#[pyfunction]
pub fn prepare(query: &str) -> PyResult<PreparedQuery> {
    let ast = parser::parse_query(query)?;
    Ok(PreparedQuery {
        plan: planner::prepare_plan(&ast)?,
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
    // let result = explain::explain(&plan);
    Ok(format!("{:#?}", plan.logical_plan))
}

fn execute(
    py: Python<'_>,
    plan: &planner::PreparedPlan,
    data: Option<PyObject>,
) -> PyResult<PyObject> {
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

    let result: Vec<_> = execute_plan(py, &plan.logical_plan, &mut ctx)?
        .map_ok(|row| {
            if row.len() == 1 {
                row.into_values().next().unwrap()
            } else {
                row.into_values()
                    .flat_map(|v| v.into_iter())
                    .into_py_dict_bound(py)
            }
        })
        .try_collect()?;

    Ok(result.into_py(py))
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
            return Err(NameError!("variable `{}` is not defined", _name));
        };

        if value.is_callable() {
            ctx.add_scalar_udf(name, value.clone().unbind())
        }

        ctx.add_table(name, value.unbind());
    }

    Ok(())
}
