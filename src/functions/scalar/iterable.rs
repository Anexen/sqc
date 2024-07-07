use pyo3::{exceptions::PyTypeError, prelude::*};

use super::{ScalarFunctionImpl, Volatility};

#[derive(Debug)]
pub struct Length;

impl ScalarFunctionImpl for Length {
    fn names(&self) -> Vec<&'static str> {
        vec!["length", "len"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        if args.len() != 1 {
            return Err(PyTypeError::new_err("expected 0 arguments, got 1"));
        };

        if args[0].is_none(py) {
            return Ok(py.None());
        }

        args[0].call_method0(py, "__len__")
    }
}
