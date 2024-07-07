use pyo3::{prelude::*, types::PyTuple};

use super::{ScalarFunctionImpl, Volatility};

#[derive(Debug)]
pub struct ScalarUDF {
    inner: PyObject,
}

impl ScalarUDF {
    pub fn new(inner: PyObject) -> Self {
        Self { inner }
    }
}

impl ScalarFunctionImpl for ScalarUDF {
    fn names(&self) -> Vec<&'static str> {
        vec![]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Volatile
    }

    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject> {
        self.inner
            .bind(py)
            .call1(PyTuple::new_bound(py, args))
            .map(|v| v.unbind())
    }
}
