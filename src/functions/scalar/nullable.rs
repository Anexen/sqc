use pyo3::{prelude::*, types::*};

#[pyfunction]
#[pyo3(signature = (*args))]
pub fn coalesce(args: Bound<'_, PyTuple>) -> Option<Bound<'_, PyAny>> {
    args.iter().find(|x| !x.is_none())
}

#[pyfunction]
pub fn null_if<'p>(a: Bound<'p, PyAny>, b: Bound<'p, PyAny>) -> PyResult<Option<Bound<'p, PyAny>>> {
    a.eq(b).map(|v| if v { None } else { Some(a) })
}

#[pyfunction]
pub fn is_null(value: Bound<'_, PyAny>) -> bool {
    value.is_none()
}

#[pyfunction]
pub fn is_not_null(value: Bound<'_, PyAny>) -> bool {
    !value.is_none()
}
