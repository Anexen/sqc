use pyo3::prelude::*;

mod datetime;
mod iterable;
mod math;
mod nullable;
mod strings;
mod udf;

pub use udf::ScalarUDF;

#[derive(Debug, Clone, PartialEq)]
pub enum Volatility {
    // always return the same result when given the same input
    Immutable,
    // return the same result given the same arguments for all rows within a single query
    Stable,
    // can return different results on successive calls with the same arguments
    Volatile,
}

macro_rules! udf {
    ($py:expr, $function:path) => {
        ::pyo3::wrap_pyfunction_bound!($function, $py)?.into()
    };
}

pub fn registry() -> PyResult<Vec<ScalarUDF>> {
    Python::with_gil(|py| {
        let builtins = py.import_bound("builtins")?;

        let mut result: Vec<_> = [
            "int", "str", "tuple", "dict", "list", "float", "len", "round", "pow", "set",
        ]
        .into_iter()
        .map(|name| -> PyResult<_> {
            let f = builtins.getattr(name)?;
            Ok(ScalarUDF::immutable(name.to_string(), f.unbind()))
        })
        .collect::<PyResult<_>>()?;

        let null_if: PyObject = udf!(py, self::nullable::null_if);

        result.extend([
            // nullable
            ScalarUDF::immutable("coalesce", udf!(py, self::nullable::coalesce)),
            ScalarUDF::immutable("null_if", null_if.clone_ref(py)),
            ScalarUDF::immutable("nullif", null_if),
            ScalarUDF::immutable("is_null", udf!(py, self::nullable::is_null)),
            ScalarUDF::immutable("is_not_null", udf!(py, self::nullable::is_not_null)),
            // iterable
            ScalarUDF::immutable("empty", udf!(py, self::iterable::empty)),
            ScalarUDF::immutable("index_of", udf!(py, self::iterable::index_of)),
            // math
            ScalarUDF::immutable("sqrt", udf!(py, self::math::sqrt)),
            // string
            ScalarUDF::immutable("lower", udf!(py, self::strings::lower)),
            ScalarUDF::immutable("upper", udf!(py, self::strings::upper)),
            ScalarUDF::immutable("repeat", udf!(py, self::strings::repeat)),
            ScalarUDF::immutable("concat", udf!(py, self::strings::concat)),
            // datetime
            ScalarUDF::stable("now", udf!(py, self::datetime::now)),
        ]);

        Ok(result)
    })
}
