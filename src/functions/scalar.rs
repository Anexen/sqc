use pyo3::{types::PyAnyMethods, Python};

use crate::{
    executor::ExecError,
    logical_plan::{ScalarFunctionImpl, ScalarValue},
};

#[derive(Debug)]
pub struct Length;

impl ScalarFunctionImpl for Length {
    fn name(&self) -> &str {
        "length"
    }

    fn invoke(&self, args: &[ScalarValue]) -> Result<ScalarValue, ExecError> {
        if args.len() != 1 {
            return Err(ExecError::RuntimeError("".to_string().into()));
        };
        if args[0].is_null() {
            return Ok(ScalarValue::NULL);
        }

        Python::with_gil(|py| {
            args[0]
                .clone()
                .into_bound(py)
                .call_method0("__len__")
                .map_err(|e| e.into())
                .map(|v| v.unbind().into())
        })
    }
}
