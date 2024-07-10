use derive_more::{Display, Error, From};
use sqlparser::{
    ast::{self, Query, Statement},
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError, ParserOptions},
    tokenizer::Token,
};

#[derive(Debug, Error, Display, From)]
pub enum QueryParserError {
    EmptyQuery,
    #[display(fmt = "Unsupported query: {_0}")]
    UnsupportedQuery(#[error(not(source))] String),
    MultipleQueries,
    #[display(fmt = "Query parsing error: {_0}")]
    InvalidQuery(ParserError),
}

pub fn parse_query(query: &str) -> Result<Query, QueryParserError> {
    let dialect = PythonDialect::default();

    let options = ParserOptions::new()
        .with_trailing_commas(true)
        .with_unescape(true);

    let statements = Parser::new(&dialect)
        .with_options(options)
        .try_with_sql(query)?
        .parse_statements()?;

    match statements.len() {
        0 => Err(QueryParserError::EmptyQuery),
        1 => match statements.into_iter().next().unwrap() {
            Statement::Query(query) => Ok(*query),
            stmt => Err(QueryParserError::UnsupportedQuery(stmt.to_string())),
        },
        _ => Err(QueryParserError::MultipleQueries),
    }
}

#[derive(Debug, Default)]
struct PythonDialect;

impl Dialect for PythonDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '_'
    }

    fn supports_string_literal_backslash_escape(&self) -> bool {
        true
    }

    fn supports_triple_quoted_string(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        false // TODO (kwargs)
    }

    fn supports_dictionary_syntax(&self) -> bool {
        true
    }

    fn supports_lambda_functions(&self) -> bool {
        false // TODO
    }

    fn supports_select_wildcard_except(&self) -> bool {
        false // TODO
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &ast::Expr,
        precedence: u8,
    ) -> Option<Result<ast::Expr, ParserError>> {
        let steps_back = match parser.next_token().token {
            Token::Mul => match parser.next_token().token {
                Token::Mul => return Some(parse_power_operator(parser, expr, precedence)),
                _ => 2,
            },
            // handle integer division
            Token::Div => match parser.next_token().token {
                Token::Div => return Some(parse_integer_divide(parser, expr, precedence)),
                _ => 2,
            },
            Token::Word(w) => match w.keyword {
                // replace None with sql NULL
                Keyword::NONE => return Some(Ok(ast::Expr::Value(ast::Value::Null))),
                // make IS operator work as Python's is operator (identity check)
                Keyword::IS => return Some(parse_identity_operator(parser, expr, precedence)),
                _ => 1,
            },
            _ => 1,
        };

        // back to the initial state
        for _ in 0..steps_back {
            parser.prev_token();
        }

        None
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<ast::Expr, ParserError>> {
        let steps_back = match parser.next_token().token {
            Token::Word(w) if w.keyword == Keyword::NONE => {
                return Some(Ok(ast::Expr::Value(ast::Value::Null)));
            }
            Token::LBrace if self.supports_dictionary_syntax() => {
                parser.prev_token();
                return Some(parse_dictionary_literal(parser));
            }
            Token::LParen => {
                parser.prev_token();
                return Some(parse_maybe_tuple_literal(parser));
            }
            _ => 1,
        };

        for _ in 0..steps_back {
            parser.prev_token();
        }

        None
    }
}

fn parse_power_operator(
    parser: &mut Parser,
    expr: &ast::Expr,
    precedence: u8,
) -> Result<ast::Expr, ParserError> {
    // replace ** operator with pow function
    let exponent = parser.parse_subexpr(precedence)?;
    Ok(function_call("pow", [expr.clone(), exponent]))
}

fn parse_integer_divide(
    parser: &mut Parser,
    expr: &ast::Expr,
    precedence: u8,
) -> Result<ast::Expr, ParserError> {
    let right = parser.parse_subexpr(precedence)?;
    Ok(ast::Expr::BinaryOp {
        left: Box::new(expr.clone()),
        op: ast::BinaryOperator::DuckIntegerDivide,
        right: Box::new(right),
    })
}

fn parse_identity_operator(
    parser: &mut Parser,
    expr: &ast::Expr,
    precedence: u8,
) -> Result<ast::Expr, ParserError> {
    if parser.parse_keyword(Keyword::NOT) {
        let right = parser.parse_subexpr(precedence)?;
        Ok(ast::Expr::IsDistinctFrom(
            Box::new(expr.clone()),
            Box::new(right),
        ))
    } else {
        let right = parser.parse_subexpr(precedence)?;
        Ok(ast::Expr::IsNotDistinctFrom(
            Box::new(expr.clone()),
            Box::new(right),
        ))
    }
}

fn parse_dictionary_literal(parser: &mut Parser) -> Result<ast::Expr, ParserError> {
    parser.expect_token(&Token::LBrace)?;

    let elements = if parser.peek_token().token == Token::RBrace {
        // empty dict
        vec![]
    } else {
        parser.parse_comma_separated(parse_dictionary_field)?
    };

    parser.expect_token(&Token::RBrace)?;

    let fields = ast::Expr::Array(ast::Array {
        elem: elements,
        named: false,
    });

    Ok(function_call("dict", [fields]))
}

fn parse_dictionary_field(parser: &mut Parser) -> Result<ast::Expr, ParserError> {
    let key = parser.parse_expr()?;
    parser.expect_token(&Token::Colon)?;
    let value = parser.parse_expr()?;
    Ok(ast::Expr::Tuple(vec![key, value]))
}

fn parse_maybe_tuple_literal(parser: &mut Parser) -> Result<ast::Expr, ParserError> {
    // (1) -> is not tuple
    // (1,) -> is tuple

    parser.expect_token(&Token::LParen)?;
    let elements = parser.parse_comma_separated(|p| p.parse_expr())?;

    if elements.len() == 1 {
        // look back for trailing comma
        parser.prev_token();
        if parser.next_token().token != Token::Comma {
            parser.expect_token(&Token::RParen)?;
            return Ok(elements.into_iter().next().unwrap());
        }
    }

    parser.expect_token(&Token::RParen)?;

    Ok(ast::Expr::Tuple(elements))
}

fn function_call(name: &str, args: impl IntoIterator<Item = ast::Expr>) -> ast::Expr {
    ast::Expr::Function(ast::Function {
        name: ast::ObjectName(vec![ast::Ident::new(name)]),
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}
