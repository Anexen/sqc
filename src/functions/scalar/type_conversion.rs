use pyo3::{prelude::*, types::*, PyTypeInfo};

use super::{ScalarFunctionImpl, Volatility};

#[derive(Debug)]
pub struct ToString;

impl ScalarFunctionImpl for ToString {
    fn names(&self) -> Vec<&'static str> {
        vec!["str"]
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
        vec!["int"]
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

#[derive(Debug)]
pub struct ToTuple;

impl ScalarFunctionImpl for ToTuple {
    fn names(&self) -> Vec<&'static str> {
        vec!["tuple"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        PyTuple::type_object_bound(py)
            .call1(PyTuple::new_bound(py, args))
            .map(|v| v.unbind())
    }
}

#[derive(Debug)]
pub struct ToList;

impl ScalarFunctionImpl for ToList {
    fn names(&self) -> Vec<&'static str> {
        vec!["list"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        PyList::type_object_bound(py)
            .call1(PyTuple::new_bound(py, args))
            .map(|v| v.unbind())
    }
}

#[derive(Debug)]
pub struct ToDict;

impl ScalarFunctionImpl for ToDict {
    fn names(&self) -> Vec<&'static str> {
        vec!["dict"]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        PyDict::type_object_bound(py)
            .call1(PyTuple::new_bound(py, args))
            .map(|v| v.unbind())
    }
}
