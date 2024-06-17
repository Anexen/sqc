use derive_more::{Display, Error, From};
use sqlparser::{
    ast::{Query, Statement},
    dialect::GenericDialect,
    parser::{Parser, ParserOptions},
};

#[derive(Debug, Error, Display, From)]
pub enum ParserError {
    EmptyQuery,
    #[display(fmt = "Unsupported query: {_0}")]
    UnsupportedQuery(#[error(not(source))] String),
    MultipleQueries,
    #[display(fmt = "Query parsing error: {_0}")]
    InvalidQuery(sqlparser::parser::ParserError),
}

pub fn parse_query(query: &str) -> Result<Query, ParserError> {
    let dialect = GenericDialect {};

    let options = ParserOptions::new()
        .with_trailing_commas(true)
        .with_unescape(false);

    let statements = Parser::new(&dialect)
        .with_options(options)
        .try_with_sql(query)?
        .parse_statements()?;

    match statements.len() {
        0 => Err(ParserError::EmptyQuery),
        1 => match statements.into_iter().next().unwrap() {
            Statement::Query(query) => Ok(*query),
            stmt => Err(ParserError::UnsupportedQuery(stmt.to_string())),
        },
        _ => Err(ParserError::MultipleQueries),
    }
}
