use pyo3::prelude::*;

#[pyfunction]
pub fn sqrt(x: f64) -> f64 {
    x.sqrt()
}
