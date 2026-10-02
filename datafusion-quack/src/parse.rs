//! Parsing client SQL in the session's dialect.
//!
//! DataFusion's parser (sqlparser) reads almost everything DuckDB clients send. One
//! gap is patched here, on tokens, and only for statements that failed to parse.
//! Remove it when sqlparser parses the statement itself.

use std::collections::VecDeque;

use datafusion::error::DataFusionError;
use datafusion::sql::parser::{DFParserBuilder, Statement};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

/// Parses `sql` with `dialect`.
pub(crate) fn parse(
    dialect: &dyn Dialect,
    sql: &str,
) -> Result<VecDeque<Statement>, DataFusionError> {
    let error = match parse_input(dialect, sql) {
        Ok(statements) => return Ok(statements),
        Err(error) => error,
    };
    match unparenthesized_insert_source(dialect, sql) {
        // the original error is the one that describes the client's SQL
        Some(tokens) => parse_input(dialect, tokens).map_err(|_| error),
        None => Err(error),
    }
}

fn parse_input<'a>(
    dialect: &dyn Dialect,
    input: impl Into<datafusion::sql::parser::ParserInput<'a>>,
) -> Result<VecDeque<Statement>, DataFusionError> {
    DFParserBuilder::new(input)
        .with_dialect(dialect)
        .build()?
        .parse_statements()
}

/// DuckDB's `ATTACH` sends `INSERT INTO t (VALUES (1, 'a'))`: the source is a
/// parenthesized `VALUES`, with no column list. sqlparser 0.62 takes the `(` for a
/// column list and fails (it only recognizes a parenthesized `SELECT` there). The
/// statement means `INSERT INTO t VALUES (1, 'a')`, so this returns its tokens without
/// that pair of parentheses, or `None` if `sql` isn't such an insert.
fn unparenthesized_insert_source(dialect: &dyn Dialect, sql: &str) -> Option<Vec<TokenWithSpan>> {
    let mut tokens = Tokenizer::new(dialect, sql).tokenize_with_location().ok()?;
    // positions of the tokens that aren't whitespace or a trailing `;`
    let significant: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t.token, Token::Whitespace(_) | Token::EOF))
        .map(|(i, _)| i)
        .collect();
    let at = |position: usize| significant.get(position).map(|&index| &tokens[index].token);
    let is_keyword = |position: usize, keyword: Keyword| matches!(at(position), Some(Token::Word(w)) if w.keyword == keyword && w.quote_style.is_none());
    if !is_keyword(0, Keyword::INSERT) || !is_keyword(1, Keyword::INTO) {
        return None;
    }
    // the table name: words separated by dots
    let mut position = 2;
    loop {
        if !matches!(at(position), Some(Token::Word(_))) {
            return None;
        }
        position += 1;
        if at(position) != Some(&Token::Period) {
            break;
        }
        position += 1;
    }
    let open = position;
    if at(open) != Some(&Token::LParen) || !is_keyword(open + 1, Keyword::VALUES) {
        return None;
    }
    // the matching `)` must end the statement
    let mut depth = 0usize;
    let mut close = None;
    for position in open..significant.len() {
        match at(position) {
            Some(Token::LParen) => depth += 1,
            Some(Token::RParen) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    close = Some(position);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close?;
    if !(close + 1..significant.len()).all(|position| at(position) == Some(&Token::SemiColon)) {
        return None;
    }
    let (open, close) = (significant[open], significant[close]);
    // remove the later one first, so the earlier index stays valid
    tokens.remove(close);
    tokens.remove(open);
    Some(tokens)
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::DuckDbDialect;

    use super::*;

    fn parsed(sql: &str) -> Result<String, String> {
        parse(&DuckDbDialect {}, sql)
            .map(|statements| {
                statements
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .map_err(|e| e.to_string())
    }

    #[test]
    fn duckdbs_parenthesized_values_insert_parses() {
        assert_eq!(
            parsed("INSERT INTO t (VALUES (1, 'a'))"),
            parsed("INSERT INTO t VALUES (1, 'a')")
        );
        assert_eq!(
            parsed("INSERT INTO main.t (VALUES (1, 'a'), (2, NULL));"),
            parsed("INSERT INTO main.t VALUES (1, 'a'), (2, NULL)")
        );
        assert!(parsed(r#"INSERT INTO "My Table" (VALUES ((1 + 2), 'x'))"#).is_ok());
    }

    #[test]
    fn other_statements_are_left_alone() {
        // parsed as written (sqlparser handles these)
        assert!(parsed(r#"INSERT INTO t (id, "name") (VALUES (2, 'b'))"#).is_ok());
        assert!(parsed("INSERT INTO t (SELECT 1)").is_ok());
        // still errors, with the parser's own message
        for sql in [
            "INSERT INTO t (VALUES (1)) junk",
            "INSERT INTO t (VALUES (1)",
            "INSERT INTO t (values_column) VALUES (1) (",
            "SELEC 1",
        ] {
            let error = parsed(sql).unwrap_err();
            assert!(
                error.contains("Expected") || error.contains("found"),
                "{sql}: {error}"
            );
        }
    }
}
