use pyo3::prelude::*;
use rstest::*;

mod fixtures;
mod utils;

use fixtures::*;

#[rstest]
fn test_select_const() {
    let query = r#"
    SELECT
        'one' AS string_literal,
        'a' + 'b' + 'c' AS "ABC",
        2 * 10 < 3 * 7 AS boolean_result,
        NOT 10 < 4 * 5 / 2 + 1 AS inversion,
        (2 + 2 * 2) % 2 = 0 AS is_even,
        123456 * 1000 // 33 AS intdiv,
        3 + 4/(2*3*4) - 4/(4*5*6) + 4/(6*7*8) - 4/(8*9*10) + 4/(10*11*12) AS `PI`,
    "#;

    let result = sqc::query(query, None).unwrap();

    let expected = py!([{
        "string_literal": "one",
        "ABC": "abc",
        "boolean_result": True,
        "inversion": False,
        "is_even": True,
        "intdiv": 3741090,
        "PI": 3. + 4./24. - 4./120. + 4./336. - 4./720. + 4./1320.,
    }]);

    py_assert_eq!(result, expected);
}

#[rstest]
fn test_select_wildcard(repositories: &PyObject) {
    let query = r#"
    SELECT *
    FROM repositories
    WHERE name = 'tpope/vim-surround'
    "#;

    let tables = py!({"repositories": repositories});
    let result = sqc::query(query, Some(tables.into())).unwrap();
    let expected = py!([
        {"id": 51480, "name": "tpope/vim-surround", "url": "https://api.github.com/repos/tpope/vim-surround"}
    ]);
    py_assert_eq!(result, expected);
}

#[rstest]
fn test_table_alias() {
    let query = r#"
    SELECT t.a, t.c AS "b"
    FROM data AS t
    "#;

    let data = py!([{"a": 1, "b": 2, "c": False}]);
    let result = sqc::query(query, Some(data.into())).unwrap();
    let expected = py!([{"a": 1, "b": False }]);
    py_assert_eq!(result, expected);
}

#[rstest]
fn test_select_nested() {
    let query = r#"
    SELECT v['x'][p][p + 'y'][0]['key'] AS k
    FROM data
    WHERE v['x'][p][p + 'y'][1]['value'] > 30
    "#;

    let data = py!([
        {"p": "a", "v": {"x": {"a": {"ay": [{"key": 1}, {"value": 11}]}}}},
        {"p": "b", "v": {"x": {"b": {"by": [{"key": 2}, {"value": 22}]}}}},
        {"p": "c", "v": {"x": {"c": {"cy": [{"key": 3}, {"value": 33}]}}}},
    ]);
    let result = sqc::query(query, Some(data.into())).unwrap();
    let expected = py!([{"k": 3}]);
    py_assert_eq!(result, expected);
}

#[rstest]
fn test_join_multi(events: &PyObject, users: &PyObject, pull_requests: &PyObject) {
    let query = r#"
    SELECT
        pr.id,
        pr.title,
        u.login
    FROM events ev
    JOIN pull_requests pr
        ON ev.payload['pull_request'] = pr.id
    JOIN users u ON u.id = pr.user
    WHERE
        ev.type = 'PullRequestEvent'
        AND ev.public
        AND ev.payload['action'] = 'opened'
        AND pr.commits > 100
    ORDER BY pr.title
    "#;

    let tables = py!({
        "events": events,
        "users": users,
        "pull_requests": pull_requests
    });

    let result = sqc::query(query, Some(tables.into())).unwrap();
    let expected = py!([
        {"id": 26744152, "title": "Merge v2 to trunk", "login": "danarmak" },
        {"id": 26743810, "title": "Pulling version 1.9.2", "login": "lunarok" },
        {"id": 26743767, "title": "Update from master", "login": "OQO" },
    ]);
    py_assert_eq!(result, expected);
}

#[rstest]
fn test_order_by(issues: &PyObject) {
    let query = r#"
    SELECT id, title
    FROM issues
    WHERE `user` = 1063152
    ORDER BY comments DESC, created_at
    "#;

    let tables = py!({"issues": issues});

    let result = sqc::query(query, Some(tables.into())).unwrap();
    let expected = py!([
        {"id": 40068791, "title": "Moderator page"},
        {"id": 12787733, "title": "Moderator tab"},
        {"id": 12787823, "title": "Chat banning"},
        {"id": 12787902, "title": "Chat logging"},
    ]);
    py_assert_eq!(result, expected);
}

#[rstest]
fn test_function_call(users: &PyObject) {
    let query = r#"
    SELECT id, login
    FROM users
    WHERE length(login) > 15
    LIMIT 3
    "#;

    let tables = py!({"users": users});
    let result = sqc::query(query, Some(tables.into())).unwrap();
    let expected = py!([
        {"id": 121686, "login": "JohnathonReasons"},
        {"id": 368838, "login": "fredrik-johansson"},
        {"id": 564592, "login": "HeinrichApfelmus"},
    ]);
    py_assert_eq!(result, expected);
}
