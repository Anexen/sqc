use pyo3::{types::PyList, Py, Python};
use rstest::*;

mod utils;
use utils::*;

#[rstest]
fn test_agg_query(ad_data: &Py<PyList>) {
    let query = r#"
    SELECT sum(spend) AS "Spend"
    FROM dataset
    WHERE campaign_id = 10
    "#;
    Python::with_gil(|py| {
        let result = sqc::query(py, query, ad_data.bind(py)).unwrap();
        let expected = vec![pydict!(py, "Spend" => 30)];
        pyo3::py_run!(py, result expected, r#"assert result == expected"#);
    });
}
