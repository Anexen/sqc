use pyo3::{
    prelude::*,
    types::{PyLong, PyString, PyTuple},
    PyTypeInfo,
};

use super::{ScalarFunctionImpl, Volatility};

#[derive(Debug)]
pub struct ToString;

impl ScalarFunctionImpl for ToString {
    fn names(&self) -> Vec<&'static str> {
        vec!["to_string", "str"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        PyString::type_object_bound(py)
            .call1(PyTuple::new_bound(py, args))
            .map(|v| v.unbind())
    }
}

#[derive(Debug)]
pub struct ToInt;

impl ScalarFunctionImpl for ToInt {
    fn names(&self) -> Vec<&'static str> {
        vec!["to_integer", "int"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        PyLong::type_object_bound(py)
            .call1(PyTuple::new_bound(py, args))
            .map(|v| v.unbind())
    }
}
