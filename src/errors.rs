use thiserror::Error;

pub type SqcResult<T> = Result<T, SqcError>;

#[derive(Error, Debug)]
pub enum SqcError {
    #[error("query parsing error")]
    ParserError(#[from] datafusion_sql::sqlparser::parser::ParserError),
    #[error("empty query")]
    EmptyQuery,
    #[error("unsupported query")]
    UnsupportedQuery,
}

impl From<SqcError> for pyo3::PyErr {
    fn from(error: SqcError) -> Self {
        pyo3::exceptions::PyRuntimeError::new_err(format!("{:?}", error))
    }
}
