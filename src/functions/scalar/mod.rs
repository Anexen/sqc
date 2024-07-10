use std::rc::Rc;

use pyo3::{PyObject, PyResult, Python};

mod iterable;
mod strings;
mod type_conversion;
mod udf;
mod math;

pub use udf::ScalarUDF;

pub enum Volatility {
    // always return the same result when given the same input
    Immutable,
    // return the same result given the same arguments for all rows within a single query
    Stable,
    // can return different results on successive calls with the same arguments
    Volatile,
}

pub trait ScalarFunctionImpl: std::fmt::Debug {
    fn names(&self) -> Vec<&'static str>;
    fn volatility(&self) -> Volatility;
    fn invoke(&self, py: Python<'_>, args: &[PyObject]) -> PyResult<PyObject>;
}

pub fn registry() -> Vec<Rc<dyn ScalarFunctionImpl>> {
    vec![
        Rc::new(type_conversion::ToString),
        Rc::new(type_conversion::ToInt),
        Rc::new(type_conversion::ToTuple),
        Rc::new(type_conversion::ToList),
        Rc::new(type_conversion::ToDict),
        Rc::new(iterable::Length),
        Rc::new(math::Power),
        Rc::new(strings::Lowercase),
        Rc::new(strings::Uppercase),
        Rc::new(strings::Repeat),
        Rc::new(strings::Concat),
    ]
}
