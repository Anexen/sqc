use pyo3::prelude::*;

#[pyfunction]
#[pyo3(name = "empty", signature = (iterable, /))]
pub fn empty(iterable: &Bound<'_, PyAny>) -> PyResult<bool> {
    iterable.len().map(|l| l == 0)
}

#[pyfunction]
pub fn index_of(iterable: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    Ok(iterable
        .iter()?
        .position(|item| item.and_then(|x| x.eq(value)).unwrap_or(false)))
}
