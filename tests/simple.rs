use pyo3::{types::*, Py, Python};
use rstest::*;

mod utils;

use utils::*;

#[rstest]
fn test_select_constants() {
    let query = r#"
        SELECT ((3 + 4) * 3 - 1 ) / 2 AS a
    "#;
    Python::with_gil(|py| {
        let data = PyNone::get_bound(py);
        let result = sqc::query(py, query, &data).unwrap();
        let expected = vec![pydict!(py, "a" => 10)];
        pyo3::py_run!(py, result expected, r#"assert result == expected"#);
    })
}

#[rstest]
fn test_select_with_filter(ad_data: &Py<PyList>) {
    let query = r#"
    SELECT id AS "ID", spend AS "Spend, $"
    FROM dataset
    WHERE campaign_id = 20
    "#;
    Python::with_gil(|py| {
        let result = sqc::query(py, query, ad_data.bind(py)).unwrap();
        let expected = vec![pydict!(py, "ID" => 3, "Spend, $" => 30)];
        pyo3::py_run!(py, result expected, r#"assert result == expected"#);
    })
}
