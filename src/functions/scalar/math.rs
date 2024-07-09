use pyo3::{exceptions::PyTypeError, prelude::*};

use super::{ScalarFunctionImpl, Volatility};

#[derive(Debug)]
pub struct Power;

impl ScalarFunctionImpl for Power {
    fn names(&self) -> Vec<&'static str> {
        vec!["pow"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        match args.len() {
            2 => args[0]
                .bind(py)
                .pow(args[1].bind(py), py.None())
                .map(|v| v.unbind()),
            3 => args[0]
                .bind(py)
                .pow(args[1].bind(py), args[2].bind(py))
                .map(|v| v.unbind()),

            _ => Err(PyTypeError::new_err(format!(
                "expected 2 or 3 arguments, got {}",
                args.len()
            ))),
        }
    }
}
