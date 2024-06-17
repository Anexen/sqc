use pyo3::prelude::*;
use rstest::*;

#[fixture]
#[once]
pub fn users() -> PyObject {
    load_json("users.json").unwrap()
}

#[fixture]
#[once]
pub fn repositories() -> PyObject {
    load_json("repositories.json").unwrap()
}

#[fixture]
#[once]
pub fn organizations() -> PyObject {
    load_json("organizations.json").unwrap()
}

#[fixture]
#[once]
pub fn milestones() -> PyObject {
    load_json("milestones.json").unwrap()
}

#[fixture]
#[once]
pub fn releases() -> PyObject {
    load_json("releases.json").unwrap()
}

#[fixture]
#[once]
pub fn issues() -> PyObject {
    load_json("issues.json").unwrap()
}

#[fixture]
#[once]
pub fn pull_requests() -> PyObject {
    load_json("pull_requests.json").unwrap()
}

#[fixture]
#[once]
pub fn forks() -> PyObject {
    load_json("forks.json").unwrap()
}

#[fixture]
#[once]
pub fn comments() -> PyObject {
    load_json("comments.json").unwrap()
}

#[fixture]
#[once]
pub fn events() -> PyObject {
    load_json("events.json").unwrap()
}

static PY_LOAD_JSON: &str = r#"
import json

def load_json(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(row) for row in f]
"#;

fn load_json(filename: &str) -> PyResult<PyObject> {
    let path = format!("./tests/data/{filename}");
    Python::with_gil(|py| {
        let loader = PyModule::from_code_bound(py, PY_LOAD_JSON, "loader.py", "loader")?;
        Ok(loader.getattr("load_json")?.call1((path,))?.unbind())
    })
}
