//! Canonical forms of SELECT statements for the semantic tier (L3), and the
//! relation analysis per-table TTLs rely on, on the `crate::sql_text` tokens.
//!
//! The tier accepts only statements that parse under one small grammar (an
//! allow-list): everything else is ineligible and falls through to L1/L2,
//! which match the text exactly. Unquoted words are compared ASCII-lower-
//! cased; `ident` is an unquoted word of at most 63 bytes that is neither a
//! word of this grammar nor a PostgreSQL reserved (or type/function-name)
//! keyword, nor one HeliosDB-Nano cannot read as an identifier.
//!
//! ```text
//! statement   := SELECT [ALL | DISTINCT] select_list FROM relation [WHERE bool_expr]
//!                [GROUP BY colref {, colref}] [ORDER BY order_item {, order_item}]
//!                [LIMIT int] [OFFSET int] [;]
//! select_list := '*' | select_item {, select_item}
//! select_item := (colref | agg) [[AS] ident]
//! agg         := (count | sum | min | max | avg) '(' [DISTINCT] colref ')' | count '(' '*' ')'
//! colref      := ident | qual '.' ident
//! relation    := ident ['.' ident] [[AS] ident]
//! bool_expr   := bool_term {(AND | OR) bool_term}
//! bool_term   := [NOT] (pred | '(' bool_expr ')')
//! pred        := colref cmp (literal | colref)
//!              | colref IS [NOT] (NULL | TRUE | FALSE)
//!              | colref [NOT] IN '(' literal {, literal} ')'
//!              | colref [NOT] BETWEEN literal AND literal
//!              | colref [NOT] (LIKE | ILIKE) string
//! cmp         := = | <> | != | < | <= | > | >=
//! literal     := ['-'] number | string | TRUE | FALSE | NULL
//! order_item  := colref [ASC | DESC] [NULLS (FIRST | LAST)]
//! int         := digits only
//! string      := a plain '...' literal: no E/B/X/U&/N prefix and no backslash
//! number      := digits [. [digits]] | . digits, then an optional exponent;
//!                no `_` separators, nothing glued to it
//! ```
//!
//! A `qual` must be the relation's alias, or its bare name when it has no
//! alias; a column named like the relation or its alias (a whole-row
//! reference in PostgreSQL) is refused. So quoted identifiers, `::`,
//! functions other than the five aggregates, `CASE`, arithmetic and string
//! operators, `$n`, dollar quotes, prefixed strings, subqueries, joins, comma
//! lists in FROM, `ONLY`, `TABLESAMPLE`, `LATERAL`, set operations, `HAVING`,
//! `WINDOW`, `FETCH`, `FOR UPDATE`, `GROUP BY ALL`/`DISTINCT`, ordinals in
//! `ORDER BY`/`GROUP BY`, `COLLATE`, `OPERATOR(…)` and three-part names are
//! all refused by construction. A statement longer than the configured
//! bound (`[cache] semantic_max_statement_bytes`) is not read at all.
//!
//! HeliosDB-Nano (sqlparser 0.53 behind text rewrites) refuses or rewrites
//! some statements PostgreSQL accepts, in ways that depend on what the form
//! drops (the alias, the qualifier's spelling, whitespace, comments), so
//! those are refused too: a relation alias without `AS` that sqlparser
//! reserves (`FROM kv sort`), an `ident` it cannot read where a column
//! reference starts (`trim.val`), and a statement one of its spacing-exact
//! text rewrites may fire on: `AS OF` / `VERSIONS BETWEEN` (`SELECT v AS of
//! FROM t`), the `SECURITY DEFINER` / `SECURITY INVOKER` removal (`FROM
//! xsecurity definer` reads the table `x` there) and the `AS IDENTITY (…)`
//! removal. Its other rewrites a statement of the grammar can meet read
//! every spelling of a form alike ([`nano_rewrites`]). The scanner already
//! ends a statement's tokens where HeliosDB-Nano reads its comments
//! differently. A statement naming one of the catalogs it emulates (any
//! `pg_…` or `information_schema` text, [`sql_text::names_nano_catalog`]) is
//! refused: it answers those from a router that matches the raw text,
//! spacing included (`name='x'` keeps every `pg_settings` row, `name = 'x'`
//! one). And comments are not inert there, nor on HeliosDB-Lite: Nano's
//! planner searches the raw statement for `AS OF`, Lite cuts an `AS OF`
//! clause out of it across the end of a comment (`-- as of now` newline
//! `WHERE id = 1` loses its `WHERE`), and Lite's `SIMILAR TO` and `COLLATE`
//! rewrites track quotes without knowing comments (a quote in one flips
//! what they take for a literal). So the only comment a statement of the
//! grammar may hold is a `helios:` hint of words and `key=value` pieces
//! ([`inert_comment`]); any other comment makes it ineligible.
//!
//! The canonical form serialises the parse: the qualifier's spelling dropped
//! (it is always the one relation), the relation's alias and `AS` dropped
//! from FROM, the relation (with its schema) kept, output aliases and a
//! select-list `AS` kept as written, `!=` read as `<>`, literals verbatim in
//! place, `ASC`/`DESC`/`NULLS` kept as written. Keywords fold to lower case;
//! every identifier (relation, schema, column, output alias) keeps its
//! spelling, and a qualifier must be spelled exactly like the alias (or,
//! without one, the relation). PostgreSQL folds unquoted names, but
//! HeliosDB-Lite never does (`Orders` and `orders` are two tables there,
//! and `T.id` does not resolve against the alias `t`), HeliosDB-Nano's
//! point-lookup fast path looks a table up as written, and Nano reports an
//! output column's label as written in the RowDescription (`SELECT VAL …` is
//! labelled `VAL`): case variants only cost hit rate. Nothing in the form
//! depends on how the backend resolves a name; where an unqualified name
//! could mean something else than the qualified one, the form records which
//! was written, or the statement is refused:
//!
//! - every column reference records whether it was qualified: PostgreSQL
//!   reads `q.name` as the column `name` when the relation has one and
//!   otherwise as the call `name(q)` (attribute notation, the "computed
//!   field" pattern), while a bare `name` is then an error. So `t.val` and
//!   `x.val` share an entry, `t.val` and `val` never do (and
//!   `t.double precision` is column `double`, while a bare `double
//!   precision` reads as a type name);
//! - a lone `ORDER BY` name is an output column first in PostgreSQL but an
//!   input column first in HeliosDB-Nano whenever the relation has a column
//!   of that name, and a lone `GROUP BY` name is an input column first and an
//!   output alias only when there is no such column, which the proxy cannot
//!   know: a lone `ORDER BY` or `GROUP BY` name equal to an output alias is
//!   refused. Every `ORDER BY` and `GROUP BY` column is serialised as written
//!   (an unaliased output column is named after its own column, so both
//!   readings of any other name sort by the same column): two statements
//!   share an entry only when they sort and group by the same words.
//!
//! The only approximate part is optional and lives in the semantic tier: in
//! an *approximable* statement (no `DISTINCT`, in the select list or an
//! aggregate, and no `GROUP BY`), an unqualified column without an output
//! alias is a [`Canonical::columns`] slot that the tier may fill from an
//! entry listing a similar column name at the same position. A qualified
//! column is never a slot: `q.name` may be the call `name(q)`, which can
//! return another number of rows (a set-returning function) or fail where
//! the stored column did not. Aggregates are exact.
//!
//! [`relations_complete`] says whether a read's extracted tables name every
//! relation its text names, which a per-table TTL longer than `ttl_secs`
//! requires: only a statement of this grammar whose one relation, spelled as
//! written (lower case: a backend that does not fold names may read another
//! table for `Orders`), is among them and that has no qualified column
//! reference (which may call a function reading other tables). The check is
//! syntactic only: a view, or a
//! table whose row-level security policy, rules, partitions, cascades or
//! triggers involve other tables, reads (or is changed through) relations
//! its text never names, and a table a function called from a `SELECT`
//! writes is changed by a statement the proxy treats as a read; the
//! configuration must not give such a relation a longer TTL (documented).
//! It is a function of the parse alone, so a read gets the same answer
//! whether or not the semantic tier looked at it. Parsing and serialising
//! are linear in the tokens, and the parse never recurses.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use crate::config::POSTGRES_RESERVED_WORDS;
use crate::sql_text::{self, Token, TokenKind, MAX_IDENTIFIER_BYTES};

/// Words this grammar gives a meaning: never an identifier.
const GRAMMAR_WORDS: &[&str] = &[
    "all", "and", "as", "asc", "avg", "between", "by", "count", "desc", "distinct", "false",
    "first", "from", "group", "ilike", "in", "is", "last", "like", "limit", "max", "min", "not",
    "null", "nulls", "offset", "or", "order", "select", "sum", "true", "where",
];

/// The aggregate functions of the grammar.
const AGGREGATES: &[&str] = &["avg", "count", "max", "min", "sum"];

/// The comparison operators of the grammar (the lexer reads `!=` as `<>`).
const COMPARISONS: &[&str] = &["=", "<>", "<", "<=", ">", ">="];

/// Words HeliosDB-Nano's parser (sqlparser 0.53, PostgreSQL dialect) cannot
/// read as an identifier where a column reference starts, so `trim.val` (or
/// `top.val` first in the select list) fails there while `x.val` does not:
/// `RESERVED_FOR_IDENTIFIER` (`exists`, `interval`, `struct`, `trim`) and
/// `top`, which `SELECT` reads as `TOP n`. The canonical form drops the
/// qualifier's spelling, so such a word is never an identifier here.
const NANO_NON_IDENTIFIERS: &[&str] = &["exists", "interval", "struct", "top", "trim"];

/// sqlparser 0.53's `RESERVED_FOR_TABLE_ALIAS`: words HeliosDB-Nano never
/// reads as a relation alias written without `AS` (`FROM kv sort` fails
/// there; `FROM kv AS sort` does not). The canonical form drops the alias,
/// so a relation alias without `AS` is refused when it is one of them.
const NANO_RESERVED_FOR_TABLE_ALIAS: &[&str] = &[
    "analyze",
    "anti",
    "cluster",
    "connect",
    "cross",
    "distribute",
    "end",
    "except",
    "explain",
    "fetch",
    "for",
    "format",
    "full",
    "global",
    "group",
    "having",
    "inner",
    "intersect",
    "join",
    "lateral",
    "left",
    "limit",
    "match_recognize",
    "natural",
    "offset",
    "on",
    "order",
    "outer",
    "partition",
    "pivot",
    "prewhere",
    "qualify",
    "right",
    "select",
    "semi",
    "set",
    "settings",
    "sort",
    "start",
    "top",
    "union",
    "unpivot",
    "using",
    "view",
    "where",
    "window",
    "with",
];

/// A column reference: `name` or `qual.name`. The qualifier as written (it
/// must be spelled like the alias or the relation), the name lower-cased (as
/// PostgreSQL resolves it, for the checks that refuse a name PostgreSQL may
/// read otherwise) and as written when that differs.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColRef {
    qual: Option<String>,
    name: String,
    written: Option<String>,
}

impl ColRef {
    /// The column's name as written: what the form keeps (a backend that
    /// does not fold names reads another column, or labels the output
    /// column with it).
    fn spelling(&self) -> &str {
        self.written.as_deref().unwrap_or(&self.name)
    }
}

/// An output alias: whether `AS` preceded it, its name lower-cased (as
/// PostgreSQL resolves it) and as written (the RowDescription label on a
/// backend that does not fold it).
#[derive(Debug, Clone)]
struct Alias {
    with_as: bool,
    name: String,
    spelling: String,
}

/// A select-list expression.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Expr {
    Col(ColRef),
    /// `func([DISTINCT] arg)`, or `count(*)` with `arg` `None`.
    Agg {
        func: String,
        distinct: bool,
        arg: Option<ColRef>,
    },
}

/// A select item: an expression and its output alias.
#[derive(Debug, Clone)]
struct Item {
    expr: Expr,
    alias: Option<Alias>,
}

/// One piece of a WHERE clause: a token kept as is, or a column.
#[derive(Debug, Clone)]
enum Part {
    Tok(char, String),
    Col(ColRef),
}

#[derive(Debug, Clone)]
struct OrderItem {
    col: ColRef,
    direction: Option<String>,
    nulls: Option<String>,
}

/// A statement of the grammar.
#[derive(Debug)]
struct Statement {
    /// `all` or `distinct`, as written.
    quantifier: Option<String>,
    /// The select items; `None` for `*`.
    items: Option<Vec<Item>>,
    /// The schema, as written.
    schema: Option<String>,
    /// The relation, lower-cased (the table extraction's form) and as
    /// written.
    relation: String,
    relation_spelling: String,
    /// The relation's alias.
    alias: Option<Alias>,
    filter: Vec<Part>,
    group_by: Vec<ColRef>,
    order_by: Vec<OrderItem>,
    limit: Option<String>,
    offset: Option<String>,
}

/// Whether `t` is an identifier of the grammar: not a word of the grammar,
/// nor a PostgreSQL reserved or type/function-name keyword
/// ([`POSTGRES_RESERVED_WORDS`]: some are values, `null`, `user`,
/// `current_date`, the rest syntax). A word with a non-ASCII byte
/// never is: the proxy does not know the session's `client_encoding`, and in
/// one with ASCII-range trail bytes (SJIS, BIG5, GBK, UHC, JOHAB, GB18030) a
/// letter byte can belong to a multi-byte character, which case folding
/// would turn into another character (two distinct identifiers would share
/// a form). Nor does a word longer than [`MAX_IDENTIFIER_BYTES`]: PostgreSQL
/// truncates it, so two different longer words can name the same column or
/// table.
fn is_ident(t: &Token) -> bool {
    t.kind == TokenKind::Word
        && t.text.is_ascii()
        && t.text.len() <= MAX_IDENTIFIER_BYTES
        && !GRAMMAR_WORDS.contains(&t.text.as_str())
        && POSTGRES_RESERVED_WORDS
            .binary_search(&t.text.as_str())
            .is_err()
        && !NANO_NON_IDENTIFIERS.contains(&t.text.as_str())
}

/// Whether `text` is a number as PostgreSQL's lexer reads one, without `_`
/// separators (older servers read `1_000` as `1` and a label): digits with an
/// optional `.` and fraction, or `.` and a fraction, then an optional
/// exponent. A number running into an identifier (`1x`, `0x1F`) is not one.
fn plain_number(text: &str) -> bool {
    let b = text.as_bytes();
    let digits = |mut i: usize| {
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        i
    };
    let mut i = digits(0);
    let mut mantissa = i;
    if b.get(i) == Some(&b'.') {
        let end = digits(i + 1);
        mantissa += end - (i + 1);
        i = end;
    }
    if mantissa == 0 {
        return false;
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        let start = i + 1 + usize::from(matches!(b.get(i + 1), Some(b'+' | b'-')));
        let end = digits(start);
        if end == start {
            return false;
        }
        i = end;
    }
    i == b.len()
}

/// Whether `t` is a plain `'…'` literal: no prefix (`E`, `B`, `X`, `U&`,
/// `N`) and no backslash, whose meaning depends on
/// `standard_conforming_strings` (a session setting the cache does not key
/// on).
fn plain_string(t: &Token) -> bool {
    t.kind == TokenKind::Str && !t.prefixed && !t.text.contains('\\')
}

/// A cursor over the tokens.
struct Parser<'a> {
    toks: &'a [Token],
    at: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&'a Token> {
        self.toks.get(self.at)
    }

    fn eat_word(&mut self, w: &str) -> bool {
        let hit = self.peek().is_some_and(|t| t.is_word(w));
        self.at += usize::from(hit);
        hit
    }

    fn eat_punct(&mut self, p: &str) -> bool {
        let hit = self.peek().is_some_and(|t| t.is_punct(p));
        self.at += usize::from(hit);
        hit
    }

    fn eat_one_of(&mut self, words: &[&str]) -> Option<String> {
        let t = self.peek()?;
        let w = words.iter().find(|w| t.is_word(w))?;
        self.at += 1;
        Some((*w).to_string())
    }

    /// An identifier, lower-cased, and as written when that differs.
    fn spelled_ident(&mut self) -> Option<(String, Option<String>)> {
        let t = self.peek().filter(|t| is_ident(t))?;
        self.at += 1;
        Some((t.text.clone(), t.written.clone()))
    }

    fn colref(&mut self) -> Option<ColRef> {
        let (first, first_written) = self.spelled_ident()?;
        if self.eat_punct(".") {
            let (name, written) = self.spelled_ident()?;
            Some(ColRef {
                qual: Some(first_written.unwrap_or(first)),
                name,
                written,
            })
        } else {
            Some(ColRef {
                qual: None,
                name: first,
                written: first_written,
            })
        }
    }

    /// `[[AS] ident]`.
    fn alias(&mut self) -> Option<Option<Alias>> {
        let with_as = self.eat_word("as");
        match self.spelled_ident() {
            Some((name, written)) => Some(Some(Alias {
                with_as,
                spelling: written.unwrap_or_else(|| name.clone()),
                name,
            })),
            None if with_as => None,
            None => Some(None),
        }
    }

    fn item(&mut self) -> Option<Item> {
        let t = self.peek()?;
        let aggregate = t.kind == TokenKind::Word
            && AGGREGATES.contains(&t.text.as_str())
            && self.toks.get(self.at + 1).is_some_and(|n| n.is_punct("("));
        let expr = if aggregate {
            let func = t.text.clone();
            self.at += 2;
            if func == "count" && self.eat_punct("*") {
                if !self.eat_punct(")") {
                    return None;
                }
                Expr::Agg {
                    func,
                    distinct: false,
                    arg: None,
                }
            } else {
                let distinct = self.eat_word("distinct");
                let arg = self.colref()?;
                if !self.eat_punct(")") {
                    return None;
                }
                Expr::Agg {
                    func,
                    distinct,
                    arg: Some(arg),
                }
            }
        } else {
            Expr::Col(self.colref()?)
        };
        Some(Item {
            expr,
            alias: self.alias()?,
        })
    }

    /// `literal`, appended to `out`.
    fn literal(&mut self, out: &mut Vec<Part>) -> Option<()> {
        let t = self.peek()?;
        if t.kind == TokenKind::Op && t.text == "-" {
            let n = self.toks.get(self.at + 1)?;
            if n.kind != TokenKind::Number || !plain_number(&n.text) {
                return None;
            }
            out.push(Part::Tok('o', "-".to_string()));
            out.push(Part::Tok('n', n.text.clone()));
            self.at += 2;
            return Some(());
        }
        let part = match t.kind {
            TokenKind::Number if plain_number(&t.text) => Part::Tok('n', t.text.clone()),
            TokenKind::Str if plain_string(t) => Part::Tok('s', t.text.clone()),
            TokenKind::Word if matches!(t.text.as_str(), "true" | "false" | "null") => {
                Part::Tok('k', t.text.clone())
            }
            _ => return None,
        };
        self.at += 1;
        out.push(part);
        Some(())
    }

    /// `pred`, appended to `out`.
    fn predicate(&mut self, out: &mut Vec<Part>) -> Option<()> {
        out.push(Part::Col(self.colref()?));
        let t = self.peek()?;
        if t.kind == TokenKind::Op && COMPARISONS.contains(&t.text.as_str()) {
            out.push(Part::Tok('o', t.text.clone()));
            self.at += 1;
            if self.literal(out).is_none() {
                out.push(Part::Col(self.colref()?));
            }
            return Some(());
        }
        let kw = |w: &str| Part::Tok('k', w.to_string());
        if self.eat_word("is") {
            out.push(kw("is"));
            if self.eat_word("not") {
                out.push(kw("not"));
            }
            out.push(kw(&self.eat_one_of(&["null", "true", "false"])?));
            return Some(());
        }
        if self.eat_word("not") {
            out.push(kw("not"));
        }
        let op = self.eat_one_of(&["in", "between", "like", "ilike"])?;
        out.push(kw(&op));
        match op.as_str() {
            "in" => {
                if !self.eat_punct("(") {
                    return None;
                }
                out.push(Part::Tok('u', "(".to_string()));
                self.literal(out)?;
                while self.eat_punct(",") {
                    out.push(Part::Tok('u', ",".to_string()));
                    self.literal(out)?;
                }
                if !self.eat_punct(")") {
                    return None;
                }
                out.push(Part::Tok('u', ")".to_string()));
            }
            "between" => {
                self.literal(out)?;
                if !self.eat_word("and") {
                    return None;
                }
                out.push(kw("and"));
                self.literal(out)?;
            }
            _ => {
                let t = self.peek().filter(|t| plain_string(t))?;
                out.push(Part::Tok('s', t.text.clone()));
                self.at += 1;
            }
        }
        Some(())
    }

    /// `bool_expr`, appended to `out`, without recursion: a parenthesized
    /// `bool_expr` only moves a depth counter.
    fn bool_expr(&mut self, out: &mut Vec<Part>) -> Option<()> {
        let mut depth = 0usize;
        loop {
            // bool_term := [NOT] (pred | '(' bool_expr ')')
            if self.eat_word("not") {
                out.push(Part::Tok('k', "not".to_string()));
            }
            if self.eat_punct("(") {
                out.push(Part::Tok('u', "(".to_string()));
                depth += 1;
                continue;
            }
            self.predicate(out)?;
            while depth > 0 && self.eat_punct(")") {
                out.push(Part::Tok('u', ")".to_string()));
                depth -= 1;
            }
            match self.eat_one_of(&["and", "or"]) {
                Some(w) => out.push(Part::Tok('k', w)),
                None => break,
            }
        }
        (depth == 0).then_some(())
    }

    fn order_item(&mut self) -> Option<OrderItem> {
        let col = self.colref()?;
        let direction = self.eat_one_of(&["asc", "desc"]);
        let nulls = if self.eat_word("nulls") {
            Some(self.eat_one_of(&["first", "last"])?)
        } else {
            None
        };
        Some(OrderItem {
            col,
            direction,
            nulls,
        })
    }

    fn int(&mut self) -> Option<String> {
        let t = self.peek().filter(|t| {
            t.kind == TokenKind::Number && t.text.bytes().all(|b| b.is_ascii_digit())
        })?;
        self.at += 1;
        Some(t.text.clone())
    }

    fn statement(&mut self) -> Option<Statement> {
        if !self.eat_word("select") {
            return None;
        }
        let quantifier = self.eat_one_of(&["all", "distinct"]);
        let items = if self.eat_punct("*") {
            None
        } else {
            let mut items = vec![self.item()?];
            while self.eat_punct(",") {
                items.push(self.item()?);
            }
            Some(items)
        };
        if !self.eat_word("from") {
            return None;
        }
        let first = self.spelled_ident()?;
        let ((relation, written), schema) = if self.eat_punct(".") {
            (self.spelled_ident()?, Some(first.1.unwrap_or(first.0)))
        } else {
            (first, None)
        };
        let relation_spelling = written.unwrap_or_else(|| relation.clone());
        let alias = match self.alias()? {
            // HeliosDB-Nano refuses these as an alias without `AS`, and the
            // form drops the alias.
            Some(a) if !a.with_as && NANO_RESERVED_FOR_TABLE_ALIAS.contains(&a.name.as_str()) => {
                return None
            }
            alias => alias,
        };
        let mut filter = Vec::new();
        if self.eat_word("where") {
            self.bool_expr(&mut filter)?;
        }
        let mut group_by = Vec::new();
        if self.eat_word("group") {
            if !self.eat_word("by") {
                return None;
            }
            group_by.push(self.colref()?);
            while self.eat_punct(",") {
                group_by.push(self.colref()?);
            }
        }
        let mut order_by = Vec::new();
        if self.eat_word("order") {
            if !self.eat_word("by") {
                return None;
            }
            order_by.push(self.order_item()?);
            while self.eat_punct(",") {
                order_by.push(self.order_item()?);
            }
        }
        let limit = if self.eat_word("limit") {
            Some(self.int()?)
        } else {
            None
        };
        let offset = if self.eat_word("offset") {
            Some(self.int()?)
        } else {
            None
        };
        self.eat_punct(";");
        (self.at == self.toks.len()).then_some(Statement {
            quantifier,
            items,
            schema,
            relation,
            relation_spelling,
            alias,
            filter,
            group_by,
            order_by,
            limit,
            offset,
        })
    }
}

impl Statement {
    /// Every column reference, in no particular order.
    fn colrefs(&self) -> impl Iterator<Item = &ColRef> {
        let items = self
            .items
            .iter()
            .flatten()
            .filter_map(|item| match &item.expr {
                Expr::Col(c) => Some(c),
                Expr::Agg { arg, .. } => arg.as_ref(),
            });
        let filter = self.filter.iter().filter_map(|p| match p {
            Part::Col(c) => Some(c),
            Part::Tok(..) => None,
        });
        items
            .chain(filter)
            .chain(&self.group_by)
            .chain(self.order_by.iter().map(|o| &o.col))
    }

    /// Whether every column reference names a column of the one relation
    /// unambiguously: its qualifier is spelled exactly like the relation's
    /// alias (or, without an alias, its bare name: HeliosDB-Lite does not
    /// fold names, so `T.id` does not resolve against the alias `t` there),
    /// and its name is neither, in any case (a whole-row reference in
    /// PostgreSQL, which folds them).
    fn columns_resolve(&self) -> bool {
        let qualifier = self
            .alias
            .as_ref()
            .map_or(self.relation_spelling.as_str(), |a| a.spelling.as_str());
        let alias = self.alias.as_ref().map(|a| a.name.as_str());
        self.colrefs().all(|c| {
            c.qual.as_deref().is_none_or(|q| q == qualifier)
                && c.name != self.relation
                && alias != Some(c.name.as_str())
        })
    }

    /// Whether every lone `GROUP BY` or `ORDER BY` name means one column on
    /// every backend: a lone GROUP BY name is an input column if the relation
    /// has one, else an output alias; a lone ORDER BY name is an output
    /// column first in PostgreSQL and an input column first in HeliosDB-Nano.
    /// So no unqualified one may equal an output alias. Every other one is
    /// serialised as written and sorts or groups by the same column on any
    /// backend.
    fn keys_unambiguous(&self) -> bool {
        let aliases: HashSet<&str> = self
            .items
            .iter()
            .flatten()
            .filter_map(|i| i.alias.as_ref().map(|a| a.name.as_str()))
            .collect();
        !self
            .group_by
            .iter()
            .chain(self.order_by.iter().map(|o| &o.col))
            .any(|c| c.qual.is_none() && aliases.contains(c.name.as_str()))
    }

    /// Whether the extracted `tables` name every relation the statement's
    /// text names: its one relation, as written, is among them (they are
    /// lower-case: `Orders` is another table than `orders` on a backend that
    /// does not fold names, which a TTL configured for `orders` must not
    /// reach), and no column reference is qualified. A qualified `q.name`
    /// that names no column of the relation is the call `name(q)`
    /// (attribute notation), which can read any table. Syntactic only: what
    /// the relation itself reads (a view's base tables, a row-level security
    /// policy's) is not seen.
    fn relations_complete(&self, tables: &[String]) -> bool {
        tables.contains(&self.relation_spelling) && self.colrefs().all(|c| c.qual.is_none())
    }
}

/// Whether one of HeliosDB-Nano's spacing-exact rewrites may fire on the
/// statement, in a way that depends on what the canonical form drops: two
/// consecutive words it may read as one of [`sql_text::NANO_REWRITE_TEXTS`]
/// ([`sql_text::nano_rewrite_words`]; whitespace decides, which the form
/// drops: `SELECT v AS of FROM t` loses its `FROM t` there, `SELECT v AS  of
/// FROM t` does not; `FROM xsecurity definer` reads the table `x`, `FROM
/// xsecurity  definer` the table `xsecurity` under an alias the form drops),
/// or a string literal holding one of them in any case (the time-travel
/// rewrite then cuts the statement at places that depend on the spacing
/// around its tokens; the others cut at offsets of the upper-cased text,
/// which a character that changes length when upper-cased shifts). Its
/// other rewrites of a `SELECT` (`DECIMAL` to `NUMERIC` as a whole word,
/// `IS [NOT] JSON`, which no statement of the grammar holds outside a
/// literal, SQLite's `DATETIME('now')`, a function call) take any spacing
/// and read every statement sharing a form alike.
fn nano_rewrites(toks: &[Token]) -> bool {
    let words = toks.windows(2).any(|pair| {
        pair[0].kind == TokenKind::Word
            && pair[1].kind == TokenKind::Word
            && sql_text::nano_rewrite_words(&pair[0].text, &pair[1].text)
    });
    words
        || toks
            .iter()
            .any(|t| t.kind == TokenKind::Str && sql_text::holds_nano_rewrite_text(&t.text))
}

/// Whether `comment` (delimiters included) may be dropped from a statement
/// of the grammar: only a `helios:` hint (`/*helios:…*/`, or `-- helios:…`
/// to the end of its line) whose body is a comma list of words and
/// `key=value` pieces, each of ASCII letters, digits, `_`, `.` and `-`, with
/// whitespace only around them. Any other comment is text a backend may read
/// (see the module documentation): a quote in it flips what HeliosDB-Lite's
/// `SIMILAR TO` and `COLLATE` rewrites take for a literal, and `AS OF` in it
/// sets off HeliosDB-Nano's and Lite's time travel. A hint like that holds
/// neither (no quote, and no space inside a piece).
fn inert_comment(comment: &str) -> bool {
    let body = match comment.strip_prefix("--") {
        Some(line) => line,
        None => &comment[2..comment.len() - 2],
    };
    let Some(hint) = body.trim_start().strip_prefix("helios:") else {
        return false;
    };
    let word = |w: &str| {
        !w.is_empty()
            && w.bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
    };
    hint.split(',').all(|piece| {
        let piece = piece.trim_matches(|c: char| c.is_ascii_whitespace() || c == '\x0B');
        piece.is_empty() || (piece.split('=').count() <= 2 && piece.split('=').all(word))
    })
}

#[cfg(test)]
thread_local! {
    /// Statements parsed on this thread, so tests can check that a read is
    /// parsed once.
    pub(crate) static PARSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Parse `sql` under the grammar; `None` when it is longer than
/// `max_bytes` (it is not even tokenized: the tokens and the parse take
/// about a hundred times the text's size), it names a catalog
/// HeliosDB-Nano emulates ([`sql_text::names_nano_catalog`]), it holds a
/// comment other than a plain hint ([`inert_comment`]), one of
/// HeliosDB-Nano's spacing-exact rewrites may fire on it ([`nano_rewrites`]),
/// it does not parse, a column reference is not one of the relation's
/// columns beyond doubt, or a lone `GROUP BY`/`ORDER BY` name equals an
/// output alias.
fn parse(sql: &str, max_bytes: usize) -> Option<Statement> {
    #[cfg(test)]
    PARSES.with(|n| n.set(n.get() + 1));
    if sql.len() > max_bytes {
        return None;
    }
    if sql_text::names_nano_catalog(sql.as_bytes()) {
        return None;
    }
    let toks = sql_text::tokens(sql, inert_comment).ok()?;
    if nano_rewrites(&toks) {
        return None;
    }
    let statement = Parser { toks: &toks, at: 0 }.statement()?;
    (statement.columns_resolve() && statement.keys_unambiguous()).then_some(statement)
}

/// Whether the extracted `tables` (the normalizer's: lower-case, schema
/// dropped) name every relation `sql`'s text names: `sql` (at most
/// `max_bytes` long) parses under the grammar, its one relation, as written,
/// is among them (so it is spelled in lower case: a backend that does not
/// fold names reads another table for `Orders`), and no column reference is
/// qualified (attribute notation can call a function). A per-table TTL
/// longer than `ttl_secs` applies only then;
/// otherwise a relation the extraction missed could carry a shorter one. The
/// check is syntactic only: a view, or a table whose row-level security
/// policy reads other tables, still reads relations the text never names
/// (the documentation says never to give one a longer TTL).
pub(crate) fn relations_complete(sql: &str, tables: &[String], max_bytes: usize) -> bool {
    parse(sql, max_bytes).is_some_and(|s| s.relations_complete(tables))
}

/// What the semantic tier compares of a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Canonical {
    /// Hash of `shape`: with the session context, the tier's bucket.
    pub(crate) guard: u64,
    /// Everything any hit requires to be identical: the serialised statement
    /// with each [`Self::columns`] slot's column replaced by a marker.
    pub(crate) shape: String,
    /// Hash of `select`.
    pub(crate) select_hash: u64,
    /// The serialised select items; an exact hit requires them identical too.
    pub(crate) select: String,
    /// Per select item, its column name as written when it is a
    /// substitutable column (unqualified, without an output alias, in an
    /// approximable statement), else `None`.
    pub(crate) columns: Vec<Option<String>>,
    /// Whether such columns may be substituted: no `DISTINCT` and no
    /// `GROUP BY`.
    pub(crate) approximable: bool,
    /// [`relations_complete`] of the statement: its relation is spelled in
    /// lower case (it is among the tables lower-cased by construction) and
    /// no column reference is qualified.
    pub(crate) relations_complete: bool,
}

/// Append one token unambiguously: a kind byte, the text, a NUL (which a
/// statement cannot contain).
fn push(out: &mut String, kind: char, text: &str) {
    out.push(kind);
    out.push_str(text);
    out.push('\0');
}

/// Append a column reference as written: kind `c` when bare, `p` when
/// qualified (the qualifier's spelling is dropped: it is always the one
/// relation, spelled exactly like its alias or name). A qualified `q.name`
/// is the column `name` when the relation has one and otherwise the call
/// `name(q)` (attribute notation), where a bare `name` is an error: the two
/// never share an entry.
fn push_col(out: &mut String, c: &ColRef) {
    push(out, if c.qual.is_some() { 'p' } else { 'c' }, c.spelling());
}

/// Serialise a select item: its expression, then its alias, both as written
/// (the output column's label is the alias, or else the column's name,
/// which HeliosDB-Nano reports as written); an aggregate is labelled by its
/// function's name, which every backend folds. `t.double precision` is
/// column `double` labelled `precision`, a bare `double precision` a type
/// name: [`push_col`] tells them apart.
fn push_item(out: &mut String, item: &Item) {
    match &item.expr {
        Expr::Col(c) => push_col(out, c),
        Expr::Agg {
            func,
            distinct,
            arg,
        } => {
            push(out, 'f', func);
            push(out, 'u', "(");
            if *distinct {
                push(out, 'k', "distinct");
            }
            match arg {
                Some(c) => push_col(out, c),
                None => push(out, 'u', "*"),
            }
            push(out, 'u', ")");
        }
    }
    if let Some(alias) = &item.alias {
        if alias.with_as {
            push(out, 'k', "as");
        }
        push(out, 'a', &alias.spelling);
    }
}

/// The canonical form of `sql`, or `None` when the semantic tier must not
/// serve it: it is longer than `max_bytes`, does not parse under the grammar,
/// or its relation, lower-cased, is not in `tables` (the normalizer's
/// extraction, lower-case like every write's: they invalidate the entry,
/// C-02, whatever the relation's spelling). Linear in the statement's
/// tokens.
pub(crate) fn canonicalize(sql: &str, tables: &[String], max_bytes: usize) -> Option<Canonical> {
    let s = parse(sql, max_bytes)?;
    if !tables.contains(&s.relation) {
        return None;
    }
    let distinct = s.quantifier.as_deref() == Some("distinct")
        || s.items
            .iter()
            .flatten()
            .any(|i| matches!(i.expr, Expr::Agg { distinct: true, .. }));
    let approximable = !distinct && s.group_by.is_empty();

    let mut shape = String::new();
    let mut select = String::new();
    push(&mut shape, 'k', "select");
    if let Some(q) = &s.quantifier {
        push(&mut shape, 'k', q);
    }
    let columns: Vec<Option<String>> = match &s.items {
        None => {
            push(&mut shape, 'u', "*");
            push(&mut select, 'u', "*");
            vec![None]
        }
        Some(items) => items
            .iter()
            .map(|item| {
                // Only an unqualified column is a slot: a qualified `q.name`
                // may be the call `name(q)`, which can return another number
                // of rows than a column (set-returning) or fail.
                let slot = match (&item.expr, &item.alias) {
                    (Expr::Col(c), None) if approximable && c.qual.is_none() => Some(c),
                    _ => None,
                };
                let mut text = String::new();
                push_item(&mut text, item);
                push(&mut shape, 'm', "item");
                push(&mut select, 'm', "item");
                match slot {
                    Some(_) => push(&mut shape, 'm', "slot"),
                    None => shape.push_str(&text),
                }
                select.push_str(&text);
                slot.map(|c| c.spelling().to_string())
            })
            .collect(),
    };
    push(&mut shape, 'k', "from");
    if let Some(schema) = &s.schema {
        push(&mut shape, 'r', schema);
        push(&mut shape, 'u', ".");
    }
    push(&mut shape, 'r', &s.relation_spelling);
    if !s.filter.is_empty() {
        push(&mut shape, 'k', "where");
        for part in &s.filter {
            match part {
                Part::Tok(kind, text) => push(&mut shape, *kind, text),
                Part::Col(c) => push_col(&mut shape, c),
            }
        }
    }
    if !s.group_by.is_empty() {
        push(&mut shape, 'k', "group by");
        for c in &s.group_by {
            push_col(&mut shape, c);
        }
    }
    if !s.order_by.is_empty() {
        push(&mut shape, 'k', "order by");
        for o in &s.order_by {
            push(&mut shape, 'm', "key");
            push_col(&mut shape, &o.col);
            if let Some(d) = &o.direction {
                push(&mut shape, 'k', d);
            }
            if let Some(n) = &o.nulls {
                push(&mut shape, 'k', "nulls");
                push(&mut shape, 'k', n);
            }
        }
    }
    if let Some(limit) = &s.limit {
        push(&mut shape, 'k', "limit");
        push(&mut shape, 'n', limit);
    }
    if let Some(offset) = &s.offset {
        push(&mut shape, 'k', "offset");
        push(&mut shape, 'n', offset);
    }
    let hash = |text: &str| {
        let mut h = DefaultHasher::new();
        text.hash(&mut h);
        h.finish()
    };
    Some(Canonical {
        guard: hash(&shape),
        select_hash: hash(&select),
        shape,
        select,
        columns,
        approximable,
        relations_complete: s.relations_complete(tables),
    })
}

/// The verbatim form of `sql` (`[cache] semantic_match = "verbatim"`), or
/// `None` when the semantic tier must not serve it: exactly when
/// [`canonicalize`] refuses it (the grammar, with its single relation among
/// `tables`, is still the eligibility, so C-02 invalidation stays exact).
/// Its key is the statement text exactly as the backend receives it, hint
/// comments included: removing a hint glues the tokens around it
/// (`FROM t/*helios:x*/x` would read `FROM tx`), and replacing it by a space
/// is not what HeliosDB-Nano reads either (its point-lookup fast path
/// declines any statement holding a comment, and its planner folds the name
/// the fast path looks up as written). Nothing is folded, so there are no
/// substitutable columns and the statement is not approximable.
pub(crate) fn verbatim(sql: &str, tables: &[String], max_bytes: usize) -> Option<Canonical> {
    let s = parse(sql, max_bytes)?;
    if !tables.contains(&s.relation) {
        return None;
    }
    let mut shape = String::with_capacity(sql.len() + 2);
    push(&mut shape, 'v', sql);
    let mut h = DefaultHasher::new();
    shape.hash(&mut h);
    Some(Canonical {
        guard: h.finish(),
        select_hash: 0,
        shape,
        select: String::new(),
        columns: Vec::new(),
        approximable: false,
        relations_complete: s.relations_complete(tables),
    })
}

/// The form the semantic tier keys `sql` by under `semantic_match`:
/// [`verbatim`] or [`canonicalize`].
pub(crate) fn semantic_form(
    sql: &str,
    tables: &[String],
    max_bytes: usize,
    semantic_match: crate::config::SemanticMatch,
) -> Option<Canonical> {
    match semantic_match {
        crate::config::SemanticMatch::Verbatim => verbatim(sql, tables, max_bytes),
        crate::config::SemanticMatch::Canonical => canonicalize(sql, tables, max_bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::QueryNormalizer;

    /// No statement-size bound (the bound has its own test).
    const UNBOUNDED: usize = usize::MAX;

    fn tables(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// The canonical form, with the tables the normalizer extracts.
    fn canon(sql: &str) -> Option<Canonical> {
        canonicalize(
            sql,
            &QueryNormalizer::new().normalize(sql).tables,
            UNBOUNDED,
        )
    }

    fn complete(sql: &str, names: &[&str]) -> bool {
        relations_complete(sql, &tables(names), UNBOUNDED)
    }

    fn accepted(sql: &str) -> Canonical {
        canon(sql).unwrap_or_else(|| panic!("{sql} refused"))
    }

    /// `a` and `b` are both accepted and never share an entry.
    fn distinct_forms(a: &str, b: &str) {
        let (ca, cb) = (accepted(a), accepted(b));
        assert_ne!(
            (&ca.shape, &ca.select),
            (&cb.shape, &cb.select),
            "{a} vs {b}"
        );
        assert_ne!(ca.guard, cb.guard, "{a} vs {b}");
    }

    /// `a` and `b` are both accepted and are one entry.
    fn same_form(a: &str, b: &str) {
        assert_eq!(accepted(a), accepted(b), "{a} vs {b}");
    }

    #[test]
    fn relations_complete_needs_one_grammar_relation_among_the_tables() {
        assert!(complete("SELECT v FROM a WHERE id = 1", &["a"]));
        assert!(complete("SELECT v FROM s1.a AS x WHERE id = 1", &["a"]));
        assert!(!complete("SELECT v FROM a", &["b"]));
        // A qualified column may be a function of the row (attribute
        // notation), which can read any table.
        assert!(!complete("SELECT x.v FROM s1.a AS x", &["a"]));
        assert!(!complete("SELECT v FROM a WHERE a.id = 1", &["a"]));
        for sql in [
            "SELECT * FROM a, b",
            "SELECT * FROM a JOIN b ON a.id = b.id",
            "SELECT * FROM a WHERE id IN (SELECT id FROM b)",
            "SELECT * FROM \"A\"",
            "SELECT * FROM generate_series(1, 3)",
            "SELECT * FROM ONLY a",
            "SELECT * FROM a, LATERAL (SELECT 1) l",
            "SELECT * FROM (VALUES (1)) v",
            "SELECT 1",
            "SELECT * FROM",
            "SELECT 'unterminated FROM a",
            "SELECT * FROM U&\"a\"",
            "SELECT * FROM a TABLESAMPLE SYSTEM (10)",
            "SELECT a FROM a",
        ] {
            assert!(!complete(sql, &["a", "b"]), "{sql}");
        }
        // The canonical form carries the same answer.
        for (sql, want) in [
            ("SELECT v FROM a t WHERE id = 1 ORDER BY v", true),
            ("SELECT t.v FROM a t", false),
            ("SELECT count(t.v) FROM a t", false),
            ("SELECT v FROM a t ORDER BY t.v", false),
            ("SELECT v FROM a GROUP BY a.v", false),
        ] {
            assert_eq!(accepted(sql).relations_complete, want, "{sql}");
            assert_eq!(complete(sql, &["a"]), want, "{sql}");
        }
    }

    /// PostgreSQL reads `q.name` as the column `name` when the relation has
    /// one and otherwise as the call `name(q)` (attribute notation, the
    /// "computed field" pattern), while a bare `name` is then an error. So a
    /// qualified and a bare spelling never share an entry (an entry the
    /// function filled would answer a statement PostgreSQL rejects), and a
    /// qualified column makes the relations incomplete (the function can
    /// read other tables, which a longer table TTL must not outlive).
    #[test]
    fn qualified_columns_may_be_functions() {
        for (a, b) in [
            (
                "SELECT c.somefunc FROM inventory_item c",
                "SELECT somefunc FROM inventory_item",
            ),
            (
                "SELECT inventory_item.somefunc FROM inventory_item",
                "SELECT somefunc FROM inventory_item",
            ),
            (
                "SELECT count(c.somefunc) FROM inventory_item c",
                "SELECT count(somefunc) FROM inventory_item",
            ),
            (
                "SELECT id FROM inventory_item c WHERE c.somefunc > 1",
                "SELECT id FROM inventory_item WHERE somefunc > 1",
            ),
            (
                "SELECT id FROM inventory_item c ORDER BY c.somefunc",
                "SELECT id FROM inventory_item ORDER BY somefunc",
            ),
            (
                "SELECT id FROM inventory_item c WHERE id = c.somefunc",
                "SELECT id FROM inventory_item WHERE id = somefunc",
            ),
            (
                "SELECT count(*) FROM inventory_item c GROUP BY c.somefunc",
                "SELECT count(*) FROM inventory_item GROUP BY somefunc",
            ),
            (
                "SELECT c.somefunc AS s FROM inventory_item c",
                "SELECT somefunc AS s FROM inventory_item",
            ),
        ] {
            distinct_forms(a, b);
            assert!(!accepted(a).relations_complete, "{a}");
            assert!(accepted(b).relations_complete, "{b}");
        }
        // The qualifier's spelling still does not matter.
        same_form(
            "SELECT c.somefunc FROM inventory_item c WHERE c.id = 1",
            "SELECT inventory_item.somefunc FROM inventory_item WHERE inventory_item.id = 1",
        );
        // The function-call spelling is outside the grammar.
        assert_eq!(canon("SELECT somefunc(c) FROM inventory_item c"), None);
    }

    #[test]
    fn aliases_qualifiers_keyword_case_and_from_as_fold() {
        let a = accepted("SELECT t.val FROM kv AS t WHERE t.id = 1");
        for sql in [
            "select x.val from kv x where x.id = 1",
            "SELECT kv.val FROM kv WHERE kv.id = 1;",
            "SELECT\n  t.val\nFROM kv t\nWHERE t.id = 1 -- helios:cache=semantic\n",
            "SELECT t . val FROM kv AS t WHERE t.id = 1 /*helios:cache=semantic*/",
        ] {
            assert_eq!(a, accepted(sql), "{sql}");
        }
        let bare = accepted("SELECT val FROM kv WHERE id = 1");
        for sql in [
            "select val from kv where id = 1",
            "SELECT val FROM kv t WHERE id = 1 /* helios:cache=semantic */",
            "SELECT val FROM kv AS t WHERE id = 1;",
        ] {
            assert_eq!(bare, accepted(sql), "{sql}");
        }
        // A qualified column is never a bare one (it may be a function).
        distinct_forms(
            "SELECT t.val FROM kv AS t WHERE t.id = 1",
            "SELECT val FROM kv WHERE id = 1",
        );
        distinct_forms(
            "SELECT t.val FROM kv AS t WHERE id = 1",
            "SELECT val FROM kv AS t WHERE t.id = 1",
        );
        // Parentheses are kept as written.
        assert_ne!(bare, accepted("SELECT val FROM kv WHERE (id = 1)"));
        assert!(a.approximable);
        // A qualified column is no slot (it may be a function); a bare one is.
        assert_eq!(a.columns, [None]);
        assert_eq!(bare.columns, [Some("val".to_string())]);
        // `!=` is `<>`.
        same_form(
            "SELECT v FROM t WHERE x != 5",
            "SELECT v FROM t WHERE x <> 5",
        );
        // The schema stays.
        same_form("SELECT kv.val FROM s1.kv", "SELECT k.val FROM s1.kv k");
        same_form("SELECT val FROM s1.kv", "SELECT val FROM s1.kv k");
        distinct_forms("SELECT val FROM s1.kv", "SELECT val FROM kv");
        distinct_forms("SELECT val FROM s1.kv", "SELECT val FROM s2.kv");
        // A schema named like the table or the alias is still the schema.
        distinct_forms("SELECT val FROM kv.kv", "SELECT val FROM kv");
        same_form(
            "SELECT kv.val FROM kv.kv WHERE kv.id = 1",
            "SELECT x.val FROM kv.kv x WHERE x.id = 1",
        );
        same_form(
            "SELECT s.val FROM s.kv s WHERE s.id = 1",
            "SELECT kv.val FROM s.kv WHERE kv.id = 1",
        );
    }

    #[test]
    fn rows_affecting_differences_never_share_a_bucket() {
        for (a, b) in [
            (
                "SELECT v FROM t LIMIT 10 OFFSET 20",
                "SELECT v FROM t LIMIT 20 OFFSET 10",
            ),
            ("SELECT v FROM t LIMIT 10", "SELECT v FROM t OFFSET 10"),
            (
                "SELECT v FROM t WHERE x > -100",
                "SELECT v FROM t WHERE x > 100",
            ),
            (
                "SELECT v FROM t WHERE f = TRUE",
                "SELECT v FROM t WHERE f = FALSE",
            ),
            (
                "SELECT v FROM t WHERE x IS NULL",
                "SELECT v FROM t WHERE x IS NOT NULL",
            ),
            ("SELECT v FROM t WHERE x < 5", "SELECT v FROM t WHERE x > 5"),
            (
                "SELECT v FROM t WHERE x = 5",
                "SELECT v FROM t WHERE x <> 5",
            ),
            (
                "SELECT v FROM t WHERE x = 5",
                "SELECT v FROM t WHERE x = '5'",
            ),
            (
                "SELECT v FROM t WHERE x = y",
                "SELECT v FROM t WHERE x = 'y'",
            ),
            (
                "SELECT v FROM t WHERE x IN (1, 2)",
                "SELECT v FROM t WHERE x NOT IN (1, 2)",
            ),
            (
                "SELECT v FROM t WHERE x BETWEEN 1 AND 5",
                "SELECT v FROM t WHERE x NOT BETWEEN 1 AND 5",
            ),
            (
                "SELECT v FROM t WHERE s LIKE 'a%'",
                "SELECT v FROM t WHERE s ILIKE 'a%'",
            ),
            (
                "SELECT v FROM t ORDER BY v ASC",
                "SELECT v FROM t ORDER BY v DESC",
            ),
            (
                "SELECT v FROM t ORDER BY v",
                "SELECT v FROM t ORDER BY v ASC",
            ),
            (
                "SELECT v FROM t ORDER BY v NULLS FIRST",
                "SELECT v FROM t ORDER BY v NULLS LAST",
            ),
            ("SELECT min(v) FROM t", "SELECT max(v) FROM t"),
            ("SELECT count(v) FROM t", "SELECT count(*) FROM t"),
            ("SELECT count(v) FROM t", "SELECT count(DISTINCT v) FROM t"),
            ("SELECT v FROM t", "SELECT DISTINCT v FROM t"),
            ("SELECT v FROM t", "SELECT ALL v FROM t"),
            (
                "SELECT v FROM t WHERE a = 1 AND b = 2",
                "SELECT v FROM t WHERE b = 1 AND a = 2",
            ),
            (
                "SELECT v FROM t WHERE a = 1 AND b = 2 OR c = 3",
                "SELECT v FROM t WHERE a = 1 AND (b = 2 OR c = 3)",
            ),
            (
                "SELECT v FROM t WHERE NOT a = 1 AND b = 2",
                "SELECT v FROM t WHERE NOT (a = 1 AND b = 2)",
            ),
            (
                "SELECT * FROM orders WHERE tenant_id = '1' AND customer_id = 2",
                "SELECT * FROM orders WHERE tenant_id = '2' AND customer_id = 1",
            ),
            (
                "SELECT v FROM t WHERE s = 'a'",
                "SELECT v FROM t WHERE s = 'A'",
            ),
            ("SELECT v FROM t GROUP BY v", "SELECT v FROM t GROUP BY w"),
            ("SELECT v AS a FROM t", "SELECT v AS b FROM t"),
            ("SELECT v AS a FROM t", "SELECT v a FROM t"),
            (
                "SELECT * FROM t WHERE x = 1.e3",
                "SELECT * FROM t WHERE x = 1.5",
            ),
        ] {
            distinct_forms(a, b);
        }
    }

    /// Every wrong-row shape the review rounds found against the earlier
    /// blacklist canonicalizer is outside the grammar.
    #[test]
    fn earlier_wrong_row_shapes_are_refused() {
        for sql in [
            // CAST / TREAT to a schema-qualified type, COLLATE, OPERATOR().
            "SELECT CAST(v AS s.mytype) FROM t",
            "SELECT TREAT(v AS s.mytype) FROM t s",
            "SELECT v FROM t WHERE v = 'a' COLLATE s.c",
            "SELECT v FROM t s WHERE v OPERATOR(s.=) 1",
            "SELECT v FROM t WHERE v OPERATOR(pg_catalog.=) 1",
            // `TABLE x` subqueries, GROUP BY ALL / DISTINCT.
            "SELECT v FROM t WHERE v IN (TABLE u)",
            "SELECT v FROM t GROUP BY ALL",
            "SELECT v FROM t GROUP BY DISTINCT v",
            // `1.e3` (1000) vs `1. e3` (`1.` labelled `e3`), junk numbers.
            "SELECT 1.e3 FROM t",
            "SELECT v FROM t WHERE x = 1. e3",
            "SELECT v FROM t WHERE x = 1./**/e3",
            "SELECT v FROM t WHERE x = 1_000",
            "SELECT v FROM t WHERE x = 0x1F",
            "SELECT v FROM t WHERE x = 1x",
            "SELECT v FROM t LIMIT 1.5",
            // Qualified words that would end a FROM list, three-part names.
            "SELECT i.name FROM items i JOIN menus m ON m.id = i.menu_id AND i.order = 1, prices p",
            "SELECT t.from, v FROM t WHERE t.order = 1",
            "SELECT kv.kv.val FROM kv.kv",
            "SELECT s1.kv.v FROM s1.kv",
            // Whole-row references and qualifiers that are not the relation.
            "SELECT t FROM kv t",
            "SELECT kv FROM kv",
            "SELECT kv FROM kv t",
            "SELECT count(t) FROM kv t",
            "SELECT v FROM kv t WHERE t = t",
            "SELECT kv.val FROM kv t WHERE t.id = 1",
            "SELECT x.val FROM kv",
            "SELECT s.val FROM s.kv t",
            // Reserved words, schema-qualified types and functions, casts.
            "SELECT action FROM audit WHERE user = 'alice'",
            "SELECT t.user FROM kv t",
            "SELECT current_date FROM kv",
            "SELECT x::s.mytype FROM kv s",
            "SELECT s.mytype 'x' FROM kv s",
            "SELECT s.\"f\"(x) FROM kv s",
            "SELECT v::text FROM t",
            "SELECT val isnull FROM kv",
            "SELECT val AS isnull FROM kv",
            "SELECT DISTINCT ON (t.id) t.id FROM kv t",
            "SELECT lower(email) FROM users",
            "SELECT now() FROM t",
            "SELECT CASE WHEN v THEN 1 END FROM t",
            "SELECT v + 1 FROM t",
            "SELECT v FROM t WHERE x = 1 + 1",
            "SELECT v FROM t WHERE x || 'a' = 'b'",
            "SELECT v FROM t WHERE x = -'1'",
            "SELECT v FROM t WHERE x=-1",
            "SELECT v FROM t WHERE x <-1",
            "SELECT t.* FROM t",
            "SELECT * , v FROM t",
            // Quoting, parameters, literals whose meaning is not their text.
            "SELECT \"V\" FROM t",
            "SELECT v FROM \"Kv\"",
            "SELECT v FROM kv WHERE id = $1",
            "SELECT v FROM kv WHERE s = $$x$$",
            "SELECT v FROM kv WHERE s = E'x'",
            "SELECT v FROM kv WHERE s = N'x'",
            "SELECT v FROM kv WHERE s = B'1'",
            "SELECT v FROM kv WHERE s = U&'x'",
            r"SELECT v FROM kv WHERE s = 'x\''",
            r"SELECT v FROM kv WHERE s = 'a\nb'",
            "SELECT v FROM kv WHERE s = 'a'\n'b'",
            "SELECT v FROM kv WHERE s = date '2020-01-01'",
            "SELECT v FROM kv WHERE s LIKE 'a' ESCAPE '!'",
            "SELECT v FROM kv WHERE s SIMILAR TO 'a'",
            "SELECT v FROM kv WHERE s IS DISTINCT FROM 1",
            "SELECT v FROM kv WHERE s IS UNKNOWN",
            "SELECT v FROM kv WHERE s BETWEEN SYMMETRIC 1 AND 2",
            "SELECT v FROM kv WHERE s IN (SELECT 1)",
            "SELECT v FROM kv WHERE s = ANY ('{1}')",
            // Structure outside the grammar.
            "SELECT 1",
            "SELECT v",
            "SELECT * FROM a, b",
            "SELECT * FROM a CROSS JOIN b",
            "SELECT * FROM a NATURAL JOIN b",
            "SELECT * FROM ONLY kv",
            "SELECT * FROM kv TABLESAMPLE SYSTEM (1)",
            "SELECT * FROM kv, LATERAL (SELECT 1) x",
            "SELECT * FROM generate_series(1, 3)",
            "SELECT * FROM (SELECT 1) x",
            "SELECT * FROM kv t(a, b)",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "SELECT v FROM t UNION SELECT v FROM u",
            "SELECT v FROM t GROUP BY v HAVING count(*) > 1",
            "SELECT v FROM t WINDOW w AS ()",
            "SELECT v FROM t FETCH FIRST 1 ROWS ONLY",
            "SELECT v FROM t FOR UPDATE",
            "SELECT v FROM t ORDER BY 1",
            "SELECT v FROM t GROUP BY 1",
            "SELECT v FROM t ORDER BY v USING <",
            "SELECT v FROM t OFFSET 1 LIMIT 2",
            "SELECT v FROM t LIMIT ALL",
            "SELECT v FROM t LIMIT -1",
            "SELECT v FROM t; SELECT 2",
            "SELECT v FROM t;;",
            "SELECT v FROM kv /* unterminated",
            "SELECT v FROM t WHERE (a = 1",
            "SELECT v FROM t WHERE a = 1)",
            "SELECT v FROM t WHERE NOT NOT a = 1",
            "SELECT v FROM t WHERE ()",
            "SELECT sum(*) FROM t",
            "SELECT count(DISTINCT *) FROM t",
            "SELECT count(ALL v) FROM t",
            "SELECT count FROM t",
            "SELECT v AS from FROM kv",
            "SELECT v FROM kv x y",
            "VALUES (1)",
            "TABLE kv",
        ] {
            assert_eq!(canon(sql), None, "{sql}");
            assert!(!complete(sql, &["t", "kv", "a", "b", "u"]), "{sql}");
        }
    }

    /// PostgreSQL truncates identifiers to 63 bytes: a longer word is never an
    /// identifier (two of them can name the same column or table).
    #[test]
    fn over_long_words_are_refused() {
        let longest = "c".repeat(MAX_IDENTIFIER_BYTES);
        let c = accepted(&format!("SELECT {longest} FROM kv"));
        assert_eq!(c.columns, [Some(longest.clone())]);
        assert_eq!(canon(&format!("SELECT {longest}x FROM kv")), None);
        let table = "t".repeat(MAX_IDENTIFIER_BYTES + 1);
        assert_eq!(
            canonicalize(&format!("SELECT v FROM {table}"), &[table], UNBOUNDED),
            None
        );
    }

    /// A word with a non-ASCII byte is never an identifier: in a
    /// `client_encoding` with ASCII-range trail bytes (SJIS `C2 83 41` and
    /// `C2 83 61` are two different identifiers, and valid UTF-8 too) case
    /// folding would merge two characters. String literals keep any bytes.
    #[test]
    fn non_ascii_words_are_never_identifiers() {
        for sql in [
            "SELECT id FROM t WHERE \u{83}A = 1",
            "SELECT id FROM t WHERE \u{83}a = 1",
            "SELECT \u{83}A FROM t",
            "SELECT val FROM t\u{e4}",
            "SELECT val FROM t AS \u{e4}",
            "SELECT val AS \u{e4}x FROM t",
            "SELECT id FROM t ORDER BY x\u{83}A",
        ] {
            assert_eq!(canon(sql), None, "{sql}");
            assert!(!complete(sql, &["t", "t\u{e4}"]), "{sql}");
        }
        distinct_forms(
            "SELECT id FROM t WHERE s = '\u{83}A'",
            "SELECT id FROM t WHERE s = '\u{83}a'",
        );
    }

    /// A statement longer than the bound is not even tokenized: outside the
    /// grammar, with incomplete relations.
    #[test]
    fn statements_over_the_size_bound_are_not_parsed() {
        let sql = "SELECT v FROM t WHERE id IN (1, 2, 3)";
        let t = tables(&["t"]);
        assert!(canonicalize(sql, &t, sql.len()).is_some());
        assert!(relations_complete(sql, &t, sql.len()));
        assert_eq!(canonicalize(sql, &t, sql.len() - 1), None);
        assert!(!relations_complete(sql, &t, sql.len() - 1));
        // A huge IN list is refused at once, whatever its size.
        let huge = format!("SELECT v FROM t WHERE id IN (1{})", ",1".repeat(4_000_000));
        let started = std::time::Instant::now();
        assert_eq!(canonicalize(&huge, &t, 16 * 1024), None);
        assert!(!relations_complete(&huge, &t, 16 * 1024));
        assert!(
            started.elapsed() < std::time::Duration::from_millis(100),
            "{:?}",
            started.elapsed()
        );
    }

    /// A lone ORDER BY name is an output column first in PostgreSQL but an
    /// input column first in HeliosDB-Nano (when the relation has a column of
    /// that name): a lone name equal to an output alias is refused, and every
    /// other ORDER BY column is serialised as written, so two statements share
    /// an entry only when they sort by the same words.
    #[test]
    fn order_by_names_are_kept_as_written() {
        // Nano sorts `ORDER BY price` by the input column `price`, PostgreSQL
        // by the output `price` (price_eur): it is never one entry with
        // `ORDER BY price_eur`, which both sort by price_eur.
        assert_eq!(
            canon("SELECT price_eur AS price FROM items ORDER BY price LIMIT 1"),
            None
        );
        accepted("SELECT price_eur AS price FROM items ORDER BY price_eur LIMIT 1");
        // Refused by the grammar itself, so the relations are incomplete too:
        // a hinted and an unhinted read get the same per-table TTL.
        assert!(!complete(
            "SELECT price_eur AS price FROM items ORDER BY price LIMIT 1",
            &["items"]
        ));
        assert!(complete(
            "SELECT price_eur AS price FROM items ORDER BY price_eur LIMIT 1",
            &["items"]
        ));
        let swapped = "SELECT t.a AS b, t.b AS a FROM t";
        for sql in [
            format!("{swapped} ORDER BY a"),
            format!("{swapped} ORDER BY t.a, B DESC"),
            "SELECT a AS x FROM t ORDER BY x".to_string(),
            "SELECT a AS a FROM t ORDER BY a".to_string(),
            "SELECT a AS b, b AS a FROM t ORDER BY a".to_string(),
            "SELECT count(*) AS n FROM t x GROUP BY x.g ORDER BY n DESC".to_string(),
            "SELECT a, b AS a FROM t ORDER BY a".to_string(),
            "SELECT v AS Total FROM t ORDER BY total".to_string(),
        ] {
            assert_eq!(canon(&sql), None, "{sql}");
            assert!(!complete(&sql, &["t"]), "{sql}");
        }
        // Qualified, a name is the input column on every backend.
        distinct_forms(
            &format!("{swapped} ORDER BY t.a"),
            &format!("{swapped} ORDER BY t.b"),
        );
        accepted("SELECT a AS x FROM t ORDER BY t.x");
        accepted("SELECT count(*) AS n FROM t GROUP BY g ORDER BY t.n");
        accepted("SELECT a, b AS a FROM t ORDER BY t.a");
        // The words written make the form, not what they resolve to.
        distinct_forms(
            "SELECT a AS x FROM t ORDER BY a",
            "SELECT a AS x FROM t ORDER BY t.a",
        );
        distinct_forms(
            "SELECT t.a AS x FROM t ORDER BY a",
            "SELECT x.a AS x FROM t x ORDER BY x.a",
        );
        distinct_forms(
            "SELECT DISTINCT a FROM t ORDER BY a",
            "SELECT DISTINCT a FROM t ORDER BY t.a",
        );
        same_form(
            "SELECT t.a FROM t ORDER BY t.a",
            "SELECT x.a FROM t x ORDER BY x.a",
        );
        same_form(
            "SELECT a AS x FROM t ORDER BY id DESC",
            "select a as x from t y order by id desc",
        );
        // An unaliased output column is named after its own column, so both
        // readings sort by that column.
        accepted("SELECT a, a FROM t ORDER BY a");
        accepted("SELECT a, t.a FROM t ORDER BY a");
    }

    /// An output column's label (its alias, or else its column's name):
    /// PostgreSQL folds it to lower case, HeliosDB-Nano reports it as
    /// written, so it keeps its case. Keywords and aggregate names fold.
    #[test]
    fn output_column_labels_keep_their_spelling() {
        for (a, b) in [
            ("SELECT v AS Total FROM t", "SELECT v AS total FROM t"),
            ("SELECT v Total FROM t", "SELECT v total FROM t"),
            ("SELECT count(*) AS N FROM t", "SELECT count(*) AS n FROM t"),
            ("SELECT DISTINCT VAL FROM kv", "SELECT DISTINCT val FROM kv"),
            (
                "SELECT x.VAL FROM kv x GROUP BY x.val",
                "SELECT x.val FROM kv x GROUP BY x.val",
            ),
        ] {
            distinct_forms(a, b);
        }
        // A bare column of an approximable statement is a slot: one bucket,
        // but never an exact hit (the select lists differ). The slot is the
        // column as written (another column, on HeliosDB-Lite).
        let (ca, cb) = (
            accepted("SELECT VAL FROM kv WHERE id = 1"),
            accepted("SELECT val FROM kv WHERE id = 1"),
        );
        assert_eq!((ca.guard, &ca.shape), (cb.guard, &cb.shape));
        assert_ne!((ca.select_hash, &ca.select), (cb.select_hash, &cb.select));
        assert_eq!(ca.columns, [Some("VAL".to_string())]);
        assert_eq!(cb.columns, [Some("val".to_string())]);
        // A qualified one is no slot: its label's case makes another bucket.
        distinct_forms(
            "SELECT x.VAL FROM kv x WHERE x.id = 1",
            "select x.val from kv x where x.id = 1",
        );
        // Keywords and aggregates (labelled by the function's name, which
        // every backend folds) fold; the qualifier's spelling is dropped.
        same_form(
            "SELECT COUNT(*), SUM(v), val FROM kv AS X WHERE id = 1 ORDER BY id",
            "select count(*), sum(v), val from kv x where id = 1 order by id",
        );
        same_form("SELECT X.val FROM kv X", "SELECT kv.val FROM kv");
        // An aliased column is labelled by its alias, but `VAL` is another
        // column than `val` on HeliosDB-Lite, which does not fold names.
        distinct_forms(
            "SELECT VAL AS v, x.Alt AS w FROM kv x WHERE id = 1",
            "SELECT val AS v, x.alt AS w FROM kv x WHERE id = 1",
        );
        // An alias is still matched case-insensitively by GROUP BY and ORDER
        // BY names.
        assert_eq!(canon("SELECT a AS B FROM t GROUP BY b"), None);
        assert_eq!(canon("SELECT a AS B FROM t ORDER BY b"), None);
    }

    /// PostgreSQL folds unquoted names; HeliosDB-Lite never does (`Orders`
    /// and `orders` are two tables there, `Name` and `name` two columns, and
    /// `T.id` does not resolve against the alias `t`), and HeliosDB-Nano's
    /// point lookups find a table as written. So every identifier keeps its
    /// spelling in the form, and a qualifier must be spelled exactly like
    /// the alias (or, without one, the relation): each of these shared one
    /// form, and the second was served the first one's rows.
    #[test]
    fn identifiers_keep_their_spelling() {
        for (a, b) in [
            (
                "SELECT id FROM Orders WHERE status = 'open'",
                "SELECT id FROM orders WHERE status = 'open'",
            ),
            (
                "SELECT Name FROM Users WHERE id = 1",
                "SELECT Name FROM users WHERE id = 1",
            ),
            ("SELECT id FROM s1.kv", "SELECT id FROM S1.kv"),
            (
                "SELECT id FROM kv WHERE ID = 1",
                "SELECT id FROM kv WHERE id = 1",
            ),
            ("SELECT count(V) FROM kv", "SELECT count(v) FROM kv"),
            (
                "SELECT id FROM kv GROUP BY Id",
                "SELECT id FROM kv GROUP BY id",
            ),
            (
                "SELECT id FROM kv ORDER BY Id",
                "SELECT id FROM kv ORDER BY id",
            ),
            ("SELECT t.Val FROM kv t", "SELECT t.val FROM kv t"),
            (
                "SELECT * FROM kv WHERE a = B",
                "SELECT * FROM kv WHERE a = b",
            ),
        ] {
            distinct_forms(a, b);
        }
        // A qualifier spelled otherwise than the alias or the relation.
        for sql in [
            "SELECT t.Name FROM Users t WHERE T.id = 1",
            "SELECT T.val FROM kv t",
            "SELECT KV.val FROM kv",
            "SELECT kv.val FROM KV",
            "SELECT val FROM kv WHERE Kv.id = 1",
        ] {
            assert_eq!(canon(sql), None, "{sql}");
            assert!(!complete(sql, &["kv", "users"]), "{sql}");
        }
        same_form(
            "SELECT T.val FROM kv T WHERE T.id = 1",
            "SELECT x.val FROM kv x WHERE x.id = 1",
        );
        same_form("SELECT KV.val FROM KV", "SELECT x.val FROM KV x");
        // A relation spelled with an upper-case letter is invalidated through
        // its lower-cased name, but never gets a longer table TTL: on a
        // backend that does not fold names it is another table than the one
        // `[[cache.tables]]` names.
        assert!(!accepted("SELECT val FROM Kv WHERE id = 1").relations_complete);
        assert!(!complete("SELECT val FROM Kv WHERE id = 1", &["kv"]));
        assert!(accepted("SELECT val FROM kv WHERE id = 1").relations_complete);
        // Keywords and aggregate names still fold.
        same_form(
            "SELECT COUNT(*) FROM kv WHERE id IS NOT NULL ORDER BY id DESC LIMIT 1",
            "select count(*) from kv where id is not null order by id desc limit 1",
        );
    }

    /// A lone GROUP BY name means an input column if the relation has one,
    /// else an output alias: a name equal to an alias is refused.
    #[test]
    fn group_by_names_equal_to_an_alias_are_refused() {
        for sql in [
            "SELECT a AS b FROM t GROUP BY b",
            "SELECT a AS a FROM t GROUP BY a",
        ] {
            assert_eq!(canon(sql), None, "{sql}");
            assert!(!complete(sql, &["t"]), "{sql}");
        }
        assert!(canon("SELECT a AS b FROM t GROUP BY t.b").is_some());
        same_form(
            "SELECT x.a FROM t x GROUP BY x.a",
            "SELECT t.a FROM t GROUP BY t.a",
        );
        distinct_forms("SELECT a FROM t GROUP BY a", "SELECT a FROM t GROUP BY t.a");
    }

    /// Without `AS`, a select item keeps whether its column was qualified:
    /// `t.double precision` is column `double` labelled `precision`, while a
    /// bare `double precision` reads as a type name.
    #[test]
    fn an_alias_without_as_keeps_the_qualification() {
        distinct_forms(
            "SELECT t.double precision FROM t",
            "SELECT double precision FROM t",
        );
        same_form(
            "SELECT t.double precision FROM t",
            "SELECT x.double precision FROM t x",
        );
        same_form("SELECT t.a AS b FROM t", "SELECT x.a AS b FROM t x");
        distinct_forms("SELECT t.a AS b FROM t", "SELECT a AS b FROM t");
    }

    /// The parse is linear and never recurses: deeply nested parentheses in
    /// WHERE are read in one pass, and deeply nested FROM-list subqueries or
    /// a run of FROMs are refused at once.
    #[test]
    fn deep_nesting_is_read_in_linear_time() {
        let depth = 40_000;
        let nested_where = format!(
            "SELECT v FROM t WHERE {}a = 1{}",
            "(".repeat(depth),
            ")".repeat(depth)
        );
        let nested_from = format!(
            "{}SELECT v FROM t{}",
            "SELECT v FROM t, (".repeat(depth),
            ") x".repeat(depth)
        );
        let froms = format!("SELECT v{}", " FROM t".repeat(depth));
        let not_parens = format!(
            "SELECT v FROM t WHERE {}a = 1{}",
            "NOT (".repeat(depth),
            ")".repeat(depth)
        );
        for (sql, accept) in [
            (nested_where, true),
            (not_parens, true),
            (nested_from, false),
            (froms, false),
        ] {
            let started = std::time::Instant::now();
            let c = canonicalize(&sql, &tables(&["t"]), UNBOUNDED);
            let is_complete = complete(&sql, &["t"]);
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "{:?}",
                started.elapsed()
            );
            assert_eq!((c.is_some(), is_complete), (accept, accept));
        }
    }

    /// Checking the names after the parse is linear too: tens of thousands
    /// of aliased items with as many `GROUP BY` or `ORDER BY` names (each
    /// checked against the aliases). Quadratic, these took minutes.
    #[test]
    fn wide_select_and_order_lists_are_resolved_in_linear_time() {
        let n = 50_000;
        let items = vec!["a"; n].join(", ");
        let ordered = format!("SELECT {items} FROM t ORDER BY {items}");
        let aliased = (0..n)
            .map(|i| format!("a AS x{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let grouped = format!("SELECT {aliased} FROM t GROUP BY {items}");
        let aliased_ordered = format!("SELECT {aliased} FROM t ORDER BY {items}");
        let qualified = format!(
            "SELECT {} FROM t ORDER BY {}",
            vec!["t.a"; n].join(", "),
            vec!["t.a"; n].join(", ")
        );
        for sql in [ordered, grouped, aliased_ordered, qualified] {
            let started = std::time::Instant::now();
            let c = canonicalize(&sql, &tables(&["t"]), UNBOUNDED);
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "{:?}",
                started.elapsed()
            );
            assert!(c.is_some());
        }
        // The check still refuses a name equal to an alias among many.
        let ambiguous = format!("SELECT {items}, b AS a FROM t ORDER BY {items}");
        assert_eq!(canonicalize(&ambiguous, &tables(&["t"]), UNBOUNDED), None);
    }

    /// HeliosDB-Nano (sqlparser 0.53 behind text rewrites) refuses or
    /// rewrites these, and each had the canonical form of a statement it
    /// runs, so the tier served rows where the backend errors (or reads
    /// other rows): a relation alias without `AS` that sqlparser reserves
    /// (`FROM kv sort`: "Expected: end of statement, found: sort"), a word it
    /// cannot read where a column reference starts (`trim.val`), a
    /// statement its `AS OF` / `VERSIONS BETWEEN` rewrite fires on depending
    /// on whitespace (`AS of` loses its `FROM kv`, `AS  of` does not), one
    /// naming a catalog it answers from a raw-text router, and one with a
    /// comment it (or HeliosDB-Lite) reads as text.
    #[test]
    fn shapes_heliosdb_nano_reads_otherwise_are_refused() {
        for word in NANO_RESERVED_FOR_TABLE_ALIAS {
            let sql = format!("SELECT val FROM kv {word} WHERE id = 1");
            assert_eq!(canon(&sql), None, "{sql}");
            assert!(!complete(&sql, &["kv"]), "{sql}");
        }
        for sql in [
            "SELECT val FROM kv sort WHERE id = 1",
            "SELECT val FROM kv VIEW",
            "SELECT sort.val FROM kv sort WHERE sort.id = 1",
            // Not a column reference's first word there.
            "SELECT trim.val FROM kv AS trim WHERE trim.id = 1",
            "SELECT x.val FROM trim x",
            "SELECT trim.val FROM trim",
            "SELECT top.val FROM kv AS top",
            "SELECT val FROM kv AS exists WHERE exists.id = 1",
            "SELECT interval FROM kv",
            "SELECT val FROM kv WHERE struct = 1",
            // The time-travel rewrite.
            "SELECT val AS of FROM kv WHERE id = 1",
            "SELECT val AS  of FROM kv WHERE id = 1",
            "SELECT val AS office FROM kv WHERE id = 1",
            "SELECT val FROM kv AS of WHERE id = 1",
            "SELECT val FROM kv WHERE versions BETWEEN 1 AND 5",
            "SELECT val FROM kv WHERE conversions BETWEEN 1 AND 5",
            "SELECT val FROM kv WHERE s = 'as of'",
            "SELECT val FROM kv WHERE s = 'x VERSIONS BETWEEN y'",
            "SELECT val FROM kv WHERE s = 'a\u{17f} of'",
            // The SECURITY DEFINER / INVOKER removal (one space, any case,
            // where the words run on too): `FROM x` and `SELECT xx` there,
            // while the form drops the alias.
            "SELECT val FROM xsecurity definer WHERE id = 1",
            "SELECT val FROM xsecurity  definer WHERE id = 1",
            "SELECT val FROM xsecurity/**/definer WHERE id = 1",
            "SELECT val FROM xSecurity INVOKER WHERE id = 1",
            "SELECT security definer FROM kv WHERE id = 1",
            "SELECT xsecurity definerx FROM kv",
            "SELECT xsecurity  definerx FROM kv",
            "SELECT val FROM kv WHERE s = 'a security definer'",
            "SELECT val FROM kv WHERE s = 'x Security Invoker y'",
            "SELECT val FROM kv WHERE s = 'security \u{131}nvoker'",
            // The AS IDENTITY (...) removal.
            "SELECT val AS identity FROM kv",
            "SELECT val FROM kv AS identity WHERE id = 1",
            "SELECT val FROM kv WHERE s = 'as identity (x)'",
            // Comments HeliosDB-Nano ends elsewhere.
            "SELECT val FROM kv /* /* */ WHERE id = 2 -- */ WHERE id = 1",
            "SELECT val FROM kv --\rWHERE id = 1",
            // Catalogs it answers from a router that matches the raw text,
            // spacing included: `name='work_mem'` keeps every `pg_settings`
            // row, `name = 'work_mem'` one; `tablename='kv'` answers
            // `pg_tables` with eight columns, `tablename = 'kv'` with five.
            "SELECT name, setting FROM pg_settings WHERE name='work_mem'",
            "SELECT name, setting FROM pg_settings WHERE name = 'work_mem'",
            "SELECT * FROM pg_tables WHERE tablename = 'kv'",
            "SELECT * FROM pg_tables WHERE tablename='kv'",
            "SELECT count(*) FROM pg_tables",
            "SELECT tablename FROM pg_catalog.pg_tables",
            "SELECT relname FROM PG_CLASS",
            "SELECT table_name FROM information_schema.tables",
            "SELECT val FROM kv WHERE s = 'pg_settings'",
            "SELECT val FROM app_pg_settings",
            // Comments it (and HeliosDB-Lite) reads: Nano's planner finds
            // `AS OF` in the raw text and scans `kv` as of then; Lite cuts
            // `as of now` up to `WHERE` and reads every row; a quote flips
            // Lite's literal tracking for `SIMILAR TO`. Only a plain hint is
            // dropped.
            "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/ \
             -- AS OF TIMESTAMP '2026-10-01 00:00:00'",
            "SELECT val FROM kv -- as of now\nWHERE id = 1 /*helios:cache=semantic*/",
            "SELECT val FROM kv WHERE id = 1 /* was offline */",
            "SELECT val FROM kv /* don't */ WHERE s = 'similar to x'",
            "SELECT val FROM kv WHERE id = 1 /* c */",
            "SELECT val FROM kv WHERE id = 1 -- c\n",
            "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic note*/",
            "SELECT val FROM kv WHERE id = 1 /*helios:cache='semantic'*/",
            "SELECT val FROM kv WHERE id = 1 /*helios:a=b=c*/",
            "SELECT val FROM kv WHERE id = 1 /*cache=semantic*/",
        ] {
            assert_eq!(canon(sql), None, "{sql:?}");
            assert!(!complete(sql, &["kv", "trim", "xsecurity"]), "{sql:?}");
        }
        // After `AS` sqlparser takes any word as the alias, and such a
        // qualifier reads as a column there.
        same_form(
            "SELECT val FROM kv AS sort WHERE id = 1",
            "SELECT val FROM kv WHERE id = 1",
        );
        same_form(
            "SELECT sort.val FROM kv AS sort WHERE sort.id = 1",
            "SELECT x.val FROM kv x WHERE x.id = 1",
        );
        // Hints are the comments a statement may hold.
        for sql in [
            "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/",
            "SELECT val FROM kv WHERE id = 1 /* helios:cache=semantic, cache_ttl=30 */",
            "SELECT val FROM kv /*helios:route=standby,cache=semantic*/ WHERE id = 1",
            "SELECT val FROM kv WHERE id = 1 -- helios:cache=semantic\n",
            "SELECT val FROM kv WHERE id = 1 /*helios:cache_ttl=1.5m,*/",
        ] {
            same_form(sql, "SELECT val FROM kv WHERE id = 1");
        }
        // Words HeliosDB-Nano reads as written are still accepted.
        for sql in [
            "SELECT val FROM kv sorted WHERE id = 1",
            "SELECT of FROM kv WHERE id = 1",
            "SELECT val AS v FROM kv WHERE s = 'as  of'",
            "SELECT val FROM kv WHERE versions = 1",
            "SELECT trimmed FROM kv",
            "SELECT val FROM xsecurity AS definer WHERE id = 1",
            "SELECT security FROM kv WHERE s = 'security  definer'",
            "SELECT val AS ident FROM kv WHERE s = 'as  identity'",
        ] {
            accepted(sql);
        }
    }

    #[test]
    fn approximable_class() {
        let c = accepted("SELECT val, alt, count(*), val AS v, min(x), t.w FROM kv t WHERE id = 1");
        assert!(c.approximable);
        assert_eq!(
            c.columns,
            [
                Some("val".to_string()),
                Some("alt".to_string()),
                None,
                None,
                None,
                None,
            ]
        );
        for sql in [
            "SELECT DISTINCT val FROM kv",
            "SELECT val, count(DISTINCT x) FROM kv",
            "SELECT val FROM kv GROUP BY val",
            "SELECT * FROM kv",
        ] {
            let c = accepted(sql);
            assert!(c.columns.iter().all(Option::is_none), "{sql}");
        }
        assert!(!accepted("SELECT DISTINCT val FROM kv").approximable);
        assert!(!accepted("SELECT val FROM kv GROUP BY val").approximable);
        // Same shape, different bare columns: one bucket, two selects.
        let a = accepted("SELECT val FROM kv WHERE id = 3");
        let b = accepted("SELECT alt FROM kv AS k WHERE id = 3");
        assert_eq!((a.guard, &a.shape), (b.guard, &b.shape));
        assert_ne!(a.select_hash, b.select_hash);
        // A qualified column is never a slot: `k.alt` may be the call
        // `alt(k)` (attribute notation), which can return another number of
        // rows (a set-returning function) or fail where `k.val` does not.
        let q = accepted("SELECT k.alt FROM kv AS k WHERE id = 3");
        assert_eq!(q.columns, [None]);
        assert_ne!(a.guard, q.guard);
        for (x, y) in [
            ("SELECT k.alt FROM kv k", "SELECT k.val FROM kv k"),
            (
                "SELECT k.alt FROM kv AS k WHERE id = 3",
                "SELECT kv.val FROM kv WHERE id = 3",
            ),
            (
                "SELECT o.totals FROM orders o WHERE customer_id = 7",
                "SELECT o.total FROM orders o WHERE customer_id = 7",
            ),
            (
                "SELECT t.val FROM kv t ORDER BY foo",
                "SELECT t.foo FROM kv t ORDER BY foo",
            ),
            ("SELECT val, k.alt FROM kv k", "SELECT val, k.val FROM kv k"),
        ] {
            let (cx, cy) = (accepted(x), accepted(y));
            assert_ne!((cx.guard, &cx.shape), (cy.guard, &cy.shape), "{x} vs {y}");
        }
        // Next to a qualified item, a bare one is still a slot.
        assert_eq!(
            accepted("SELECT val, k.alt FROM kv k").columns,
            [Some("val".to_string()), None]
        );
        // ORDER BY a column keeps the statement approximable; the sort key is
        // part of the guard.
        let o = accepted("SELECT val FROM kv ORDER BY id");
        assert!(o.approximable);
        assert_ne!(o.guard, accepted("SELECT val FROM kv ORDER BY val").guard);
        assert_eq!(
            accepted("SELECT val FROM kv ORDER BY id").guard,
            accepted("SELECT alt FROM kv ORDER BY id").guard
        );
        // Not in DISTINCT or grouped statements: every item is exact.
        let d = accepted("SELECT DISTINCT val FROM kv WHERE id = 3");
        assert_ne!(
            d.guard,
            accepted("SELECT DISTINCT alt FROM kv WHERE id = 3").guard
        );
    }

    #[test]
    fn numbers_and_literals_are_kept_verbatim() {
        for number in ["1", "1.", "1.5", ".5", "1.e3", "1.E+3", ".5e-2", "1e10"] {
            assert!(plain_number(number), "{number}");
            accepted(&format!("SELECT v FROM t WHERE x = {number}"));
        }
        for junk in ["1_000", "0x1F", "1x", "1e", "1e+", ".", "1..5", ""] {
            assert!(!plain_number(junk), "{junk}");
        }
        distinct_forms(
            "SELECT v FROM t WHERE x = 1.0",
            "SELECT v FROM t WHERE x = 1",
        );
        distinct_forms(
            "SELECT v FROM t WHERE x = 'a''b'",
            "SELECT v FROM t WHERE x = 'a'",
        );
        same_form(
            "SELECT v FROM t WHERE x = - 1",
            "SELECT v FROM t WHERE x = -1",
        );
        same_form(
            "SELECT v FROM t WHERE x IN (1,'a',TRUE,NULL)",
            "select v from t where x in ( 1 , 'a' , true , null )",
        );
    }

    /// The verbatim form (`semantic_match = "verbatim"`, the default) keys a
    /// statement by its exact text, hint comments included, and is eligible
    /// exactly when the canonical form is.
    #[test]
    fn verbatim_form_is_the_text_under_the_grammar() {
        let form = |sql: &str| {
            verbatim(
                sql,
                &QueryNormalizer::new().normalize(sql).tables,
                UNBOUNDED,
            )
        };
        let key = |sql: &str| form(sql).unwrap_or_else(|| panic!("{sql} refused"));
        // Removing the hint would glue `t` and `x` (the table `tx`):
        // PostgreSQL and both HeliosDB engines read `FROM t x` here.
        let glued = key("SELECT v FROM t/*helios:cache=semantic*/x WHERE id = 1");
        let tx = key("SELECT v FROM tx WHERE id = 1/*helios:cache=semantic*/");
        assert_ne!(glued.shape, tx.shape);
        assert_ne!(glued.guard, tx.guard);
        // The alias decides between HeliosDB-Nano's point-lookup fast path
        // (`Post` as written) and its planner (`post`): never one entry,
        // although the canonical form folds the alias.
        let bare = "SELECT * FROM Post WHERE id = 1 /*helios:cache=semantic*/";
        let aliased = "SELECT * FROM Post AS p WHERE id = 1 /*helios:cache=semantic*/";
        same_form(bare, aliased);
        assert_ne!(key(bare).shape, key(aliased).shape);
        // Case, whitespace and the hint's own text all count.
        for (a, b) in [
            (
                "SELECT v FROM kv WHERE id = 1 /*helios:cache=semantic*/",
                "select v from kv where id = 1 /*helios:cache=semantic*/",
            ),
            (
                "SELECT v FROM kv WHERE id = 1 /*helios:cache=semantic*/",
                "SELECT v FROM kv  WHERE id = 1 /*helios:cache=semantic*/",
            ),
            (
                "SELECT v FROM kv WHERE id = 1 /*helios:cache=semantic*/",
                "SELECT v FROM kv WHERE id = 1 /*helios:cache=semantic,cache_ttl=60*/",
            ),
            (
                "SELECT v FROM kv WHERE id = 1 /*helios:cache=semantic*/",
                "SELECT v FROM kv WHERE id = 2 /*helios:cache=semantic*/",
            ),
        ] {
            assert_ne!(key(a).shape, key(b).shape, "{a} vs {b}");
        }
        let same = "SELECT v FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        assert_eq!(key(same), key(same));
        let k = key(same);
        assert!(!k.approximable && k.columns.is_empty() && k.relations_complete);
        // A verbatim shape never equals a canonical one.
        assert_ne!(k.shape, accepted(same).shape);
        // What the grammar refuses stays refused.
        for sql in [
            "SELECT v FROM kv WHERE id = 1 /* note */",
            "SELECT v FROM kv JOIN t ON true",
            "SELECT v FROM pg_settings",
            "SELECT v FROM \"Kv\"",
        ] {
            assert!(form(sql).is_none(), "{sql}");
            assert!(canon(sql).is_none(), "{sql}");
        }
        assert_eq!(
            semantic_form(
                same,
                &tables(&["kv"]),
                UNBOUNDED,
                crate::config::SemanticMatch::Verbatim
            ),
            Some(k)
        );
        assert_eq!(
            semantic_form(
                same,
                &tables(&["kv"]),
                UNBOUNDED,
                crate::config::SemanticMatch::Canonical
            ),
            canon(same)
        );
    }
}
