use pyo3::prelude::*;

use crate::logical_plan::Identifier;

use super::Volatility;

#[derive(Debug)]
pub struct ScalarUDF {
    pub name: Identifier,
    pub volatility: Volatility,
    pub inner: PyObject,
}

impl ScalarUDF {
    pub fn new(name: Identifier, volatility: Volatility, inner: PyObject) -> Self {
        Self {
            name,
            volatility,
            inner,
        }
    }
    pub fn volatile<T: ToString>(name: T, inner: PyObject) -> Self {
        Self::new(name.to_string().into(), Volatility::Volatile, inner)
    }

    pub fn immutable<T: ToString>(name: T, inner: PyObject) -> Self {
        Self::new(name.to_string().into(), Volatility::Immutable, inner)
    }

    pub fn stable<T: ToString>(name: T, inner: PyObject) -> Self {
        Self::new(name.to_string().into(), Volatility::Stable, inner)
    }
}
