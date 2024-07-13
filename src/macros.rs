#[macro_export]
macro_rules! NameError {
    ($($args:tt)*) => {
        exception!(PyNameError, $($args)*)
    };
}

#[macro_export]
macro_rules! ValueError {
    ($($args:tt)*) => {
        exception!(PyValueError, $($args)*)
    };
}

#[macro_export]
macro_rules! TypeError {
    ($($args:tt)*) => {
        exception!(PyTypeError, $($args)*)
    };
}

#[macro_export]
macro_rules! NotImplementedError {
    ($($args:tt)*) => {
        exception!(PyNotImplementedError, $($args)*)
    };
}

macro_rules! exception {
    ($name:ident,$($args:tt)*) => {
        ::pyo3::exceptions::$name::new_err(format!($($args)*))
    };
}
