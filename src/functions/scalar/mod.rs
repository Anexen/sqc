use pyo3::prelude::*;

mod datetime;
mod iterable;
mod math;
mod strings;
mod type_conversion;
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
        ::pyo3::wrap_pyfunction!($function)($py)?.into()
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

        result.extend([
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
