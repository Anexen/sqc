use pyo3::{types::PyList, Py, Python};
use rstest::*;

#[macro_export]
macro_rules! pydict {
    ($py:expr) => {
        ::pyo3::types::PyDict::new_bound($py)
    };
    ($py:expr, $($key:expr => $value:expr), *) => {
        {
            use ::pyo3::types::PyDictMethods;
            let _dict = ::pyo3::types::PyDict::new_bound($py);
            $(
                _dict.set_item($key, $value).unwrap();
            )*
            _dict
        }
    };
}

#[fixture]
#[once]
pub fn ad_data() -> Py<PyList> {
    Python::with_gil(|py| {
        let data = vec![
            pydict!(py, "id" => 1, "campaign_id" => 10, "spend" => 10),
            pydict!(py, "id" => 2, "campaign_id" => 10, "spend" => 20),
            pydict!(py, "id" => 3, "campaign_id" => 20, "spend" => 30),
        ];
        PyList::new_bound(py, data).into()
    })
}
