use pyo3::{intern, prelude::*};

#[pyfunction]
pub fn now<'p>(py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
    let datetime = py.import_bound(intern!(py, "datetime"))?;
    datetime
        .getattr(intern!(py, "datetime"))?
        .call_method0(intern!(py, "now"))
}
