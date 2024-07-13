use pyo3::{intern, prelude::*, types::*};

#[pyfunction]
pub fn lower<'p>(py: Python<'p>, value: &Bound<'p, PyString>) -> PyResult<Bound<'p, PyAny>> {
    value.call_method0(intern!(py, "lower"))
}

#[pyfunction]
pub fn upper<'p>(py: Python<'p>, value: &Bound<'p, PyString>) -> PyResult<Bound<'p, PyAny>> {
    value.call_method0(intern!(py, "upper"))
}

#[pyfunction]
pub fn repeat<'p>(
    value: &Bound<'p, PyString>,
    n: &Bound<'p, PyLong>,
) -> PyResult<Bound<'p, PyAny>> {
    value.mul(n)
}

#[pyfunction]
#[pyo3(signature = (*args))]
pub fn concat<'p>(py: Python<'p>, args: &Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
    intern!(py, "").call_method1(intern!(py, "join"), (args,))
}
