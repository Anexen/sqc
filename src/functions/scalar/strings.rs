use pyo3::{
    prelude::*,
    types::{PyList, PyString, PyTuple},
    PyTypeInfo,
};

use super::{ScalarFunctionImpl, Volatility};

#[derive(Debug)]
pub struct Lowercase;

impl ScalarFunctionImpl for Lowercase {
    fn names(&self) -> Vec<&'static str> {
        vec!["lowercase", "lower"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        call_string_method(py, "lower", args)
    }
}

#[derive(Debug)]
pub struct Uppercase;

impl ScalarFunctionImpl for Uppercase {
    fn names(&self) -> Vec<&'static str> {
        vec!["uppercase", "upper"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        call_string_method(py, "upper", args)
    }
}

#[derive(Debug)]
pub struct Repeat;

impl ScalarFunctionImpl for Repeat {
    fn names(&self) -> Vec<&'static str> {
        vec!["repeat"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        call_string_method(py, "__mul__", args)
    }
}

#[derive(Debug)]
pub struct Concat;

impl ScalarFunctionImpl for Concat {
    fn names(&self) -> Vec<&'static str> {
        vec!["concat"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        let args = PyList::new_bound(py, args);
        PyString::new_bound(py, "")
            .call_method1("join", (args,))
            .map(|v| v.unbind())
    }
}

fn call_string_method(py: Python, method: &str, args: &[PyObject]) -> PyResult<PyObject> {
    let args = PyTuple::new_bound(py, args);

    PyString::type_object_bound(py)
        .call_method1(method, args)
        .map(|v| v.unbind())
}
