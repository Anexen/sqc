use pyo3::{prelude::*, types::PyDict};

use crate::logical_plan::TableReference;

pub type IndexMap<K, V> = indexmap::IndexMap<K, V, fxhash::FxBuildHasher>;
// pub type IndexMap<K, V> = indexmap::IndexMap<K, V>;

// pub type RowPart = IndexMap<Identifier, PyObject>;
pub type Row<'p> = IndexMap<TableReference, Bound<'p, PyDict>>;

pub struct Stream<'s, E = PyErr> {
    inner: Box<dyn Iterator<Item = Result<Row<'s>, E>> + 's>,
}

impl<'s, E> Stream<'s, E> {
    pub fn new<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Result<Row<'s>, E>> + 's,
    {
        Stream {
            inner: Box::new(iter.into_iter()),
        }
    }
}

impl<'s, E> Iterator for Stream<'s, E> {
    type Item = Result<Row<'s>, E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait ResultIterator<R, E>: Iterator<Item = Result<R, E>> {
    fn map_then<B, F>(self, f: F) -> MapThen<Self, F>
    where
        Self: Sized,
        F: FnMut(R) -> Result<B, E>,
    {
        MapThen { iter: self, f }
    }

    fn filter_then<B, F>(self, f: F) -> FilterThen<Self, F>
    where
        Self: Sized,
        F: FnMut(&R) -> Result<B, E>,
    {
        FilterThen { iter: self, f }
    }

    fn filter_map_then<B, F>(self, f: F) -> FilterMapThen<Self, F>
    where
        Self: Sized,
        F: FnMut(&R) -> Result<Option<B>, E>,
    {
        FilterMapThen { iter: self, f }
    }

    fn and_all<F>(mut self, mut f: F) -> Result<bool, E>
    where
        Self: Sized,
        F: FnMut(&R) -> Result<bool, E>,
    {
        while let Some(item) = self.next() {
            match item {
                Err(e) => return Err(e),
                Ok(x) => match f(&x) {
                    Err(e) => return Err(e),
                    Ok(v) if !v => return Ok(false),
                    _ => {}
                },
            }
        }

        Ok(true)
    }
}

impl<R, E, T> ResultIterator<R, E> for T where T: Iterator<Item = Result<R, E>> {}

pub struct MapThen<I, F> {
    iter: I,
    f: F,
}

impl<I, F, R1, R2, E> Iterator for MapThen<I, F>
where
    I: Iterator<Item = Result<R1, E>>,
    F: FnMut(R1) -> Result<R2, E>,
{
    type Item = Result<R2, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let f = &mut self.f;
        self.iter.next().map(|r| r.and_then(f))
    }
}

pub struct FilterThen<I, F> {
    iter: I,
    f: F,
}

impl<I, F, R1, E> Iterator for FilterThen<I, F>
where
    I: Iterator<Item = Result<R1, E>>,
    F: FnMut(&R1) -> Result<bool, E>,
{
    type Item = Result<R1, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let f = &mut self.f;
        while let Some(item) = self.iter.next() {
            match item {
                Err(e) => return Some(Err(e)),
                Ok(x) => match f(&x) {
                    Err(e) => return Some(Err(e)),
                    Ok(v) if v => return Some(Ok(x)),
                    _ => {}
                },
            }
        }

        None
    }
}

pub struct FilterMapThen<I, F> {
    iter: I,
    f: F,
}

impl<I, F, R1, R2, E> Iterator for FilterMapThen<I, F>
where
    I: Iterator<Item = Result<R1, E>>,
    F: FnMut(&R1) -> Result<Option<R2>, E>,
{
    type Item = Result<R2, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let f = &mut self.f;

        while let Some(item) = self.iter.next() {
            match item {
                Err(e) => return Some(Err(e)),
                Ok(x) => match f(&x) {
                    Ok(Some(v)) => return Some(Ok(v)),
                    Ok(None) => {}
                    Err(e) => return Some(Err(e)),
                },
            }
        }

        None
    }
}
