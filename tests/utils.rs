#[macro_export]
macro_rules! py_assert_eq {
    ($a:expr, $b:expr) => {
        use ::pyo3::types::PyAnyMethods;

        ::pyo3::Python::with_gil(|py| {
            let a = &$a.into_bound(py);
            let b = &$b.into_bound(py);
            assert!(a.eq(b).unwrap(), "{} != {}", a, b)
        })
    };
}

#[macro_export]
macro_rules! py {
    // Hide distracting implementation details from the generated rustdoc.
    ($($json:tt)+) => {
        py_internal!($($json)+)
    };
}

#[macro_export]
macro_rules! py_internal {
    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an array [...]. Produces a vec![...]
    // of the elements.
    //
    // Must be invoked as: py_internal!(@array [] $($tt)*)
    //////////////////////////////////////////////////////////////////////////

    // Done with trailing comma.
    (@array [$($elems:expr,)*]) => {
        py_internal_vec![$($elems,)*]
    };

    // Done without trailing comma.
    (@array [$($elems:expr),*]) => {
        py_internal_vec![$($elems),*]
    };

    // Next element is `null`.
    (@array [$($elems:expr,)*] None $($rest:tt)*) => {
        py_internal!(@array [$($elems,)* py_internal!(None)] $($rest)*)
    };

    // Next element is `true`.
    (@array [$($elems:expr,)*] True $($rest:tt)*) => {
        py_internal!(@array [$($elems,)* py_internal!(True)] $($rest)*)
    };

    // Next element is `false`.
    (@array [$($elems:expr,)*] False $($rest:tt)*) => {
        py_internal!(@array [$($elems,)* py_internal!(False)] $($rest)*)
    };

    // Next element is an array.
    (@array [$($elems:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
        py_internal!(@array [$($elems,)* py_internal!([$($array)*])] $($rest)*)
    };

    // Next element is a map.
    (@array [$($elems:expr,)*] {$($map:tt)*} $($rest:tt)*) => {
        py_internal!(@array [$($elems,)* py_internal!({$($map)*})] $($rest)*)
    };

    // Next element is an expression followed by comma.
    (@array [$($elems:expr,)*] $next:expr, $($rest:tt)*) => {
        py_internal!(@array [$($elems,)* py_internal!($next),] $($rest)*)
    };

    // Last element is an expression with no trailing comma.
    (@array [$($elems:expr,)*] $last:expr) => {
        py_internal!(@array [$($elems,)* py_internal!($last)])
    };

    // Comma after the most recent element.
    (@array [$($elems:expr),*] , $($rest:tt)*) => {
        py_internal!(@array [$($elems,)*] $($rest)*)
    };

    // Unexpected token after most recent element.
    (@array [$($elems:expr),*] $unexpected:tt $($rest:tt)*) => {
        py_unexpected!($unexpected)
    };

    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an object {...}. Each entry is
    // inserted into the given map variable.
    //
    // Must be invoked as: py_internal!(@object $map () ($($tt)*) ($($tt)*))
    //
    // We require two copies of the input tokens so that we can match on one
    // copy and trigger errors on the other copy.
    //////////////////////////////////////////////////////////////////////////

    // Done.
    (@object $object:ident () () ()) => {};

    // Insert the current entry followed by trailing comma.
    (@object $object:ident [$($key:tt)+] ($value:expr) , $($rest:tt)*) => {
        let _ = $object.set_item(($($key)+), $value).unwrap();
        py_internal!(@object $object () ($($rest)*) ($($rest)*));
    };

    // Current entry followed by unexpected token.
    (@object $object:ident [$($key:tt)+] ($value:expr) $unexpected:tt $($rest:tt)*) => {
        py_unexpected!($unexpected);
    };

    // Insert the last entry without trailing comma.
    (@object $object:ident [$($key:tt)+] ($value:expr)) => {
        let _ = $object.set_item(($($key)+), $value).unwrap();
    };

    // Next value is `null`.
    (@object $object:ident ($($key:tt)+) (: None $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object [$($key)+] (py_internal!(None)) $($rest)*);
    };

    // Next value is `true`.
    (@object $object:ident ($($key:tt)+) (: True $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object [$($key)+] (py_internal!(True)) $($rest)*);
    };

    // Next value is `false`.
    (@object $object:ident ($($key:tt)+) (: False $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object [$($key)+] (py_internal!(False)) $($rest)*);
    };

    // Next value is an array.
    (@object $object:ident ($($key:tt)+) (: [$($array:tt)*] $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object [$($key)+] (py_internal!([$($array)*])) $($rest)*);
    };

    // Next value is a map.
    (@object $object:ident ($($key:tt)+) (: {$($map:tt)*} $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object [$($key)+] (py_internal!({$($map)*})) $($rest)*);
    };

    // Next value is an expression followed by comma.
    (@object $object:ident ($($key:tt)+) (: $value:expr , $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object [$($key)+] (py_internal!($value)) , $($rest)*);
    };

    // Last value is an expression with no trailing comma.
    (@object $object:ident ($($key:tt)+) (: $value:expr) $copy:tt) => {
        py_internal!(@object $object [$($key)+] (py_internal!($value)));
    };

    // Missing value for last entry. Trigger a reasonable error message.
    (@object $object:ident ($($key:tt)+) (:) $copy:tt) => {
        // "unexpected end of macro invocation"
        py_internal!();
    };

    // Missing colon and value for last entry. Trigger a reasonable error
    // message.
    (@object $object:ident ($($key:tt)+) () $copy:tt) => {
        // "unexpected end of macro invocation"
        py_internal!();
    };

    // Misplaced colon. Trigger a reasonable error message.
    (@object $object:ident () (: $($rest:tt)*) ($colon:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `:`".
        py_unexpected!($colon);
    };

    // Found a comma inside a key. Trigger a reasonable error message.
    (@object $object:ident ($($key:tt)*) (, $($rest:tt)*) ($comma:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `,`".
        py_unexpected!($comma);
    };

    // Key is fully parenthesized. This avoids clippy double_parens false
    // positives because the parenthesization may be necessary here.
    (@object $object:ident () (($key:expr) : $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object ($key) (: $($rest)*) (: $($rest)*));
    };

    // Refuse to absorb colon token into key expression.
    (@object $object:ident ($($key:tt)*) (: $($unexpected:tt)+) $copy:tt) => {
        json_expect_expr_comma!($($unexpected)+);
    };

    // Munch a token into the current key.
    (@object $object:ident ($($key:tt)*) ($tt:tt $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object ($($key)* $tt) ($($rest)*) ($($rest)*));
    };

    //////////////////////////////////////////////////////////////////////////
    // The main implementation.
    //
    // Must be invoked as: py_internal!($($json)+)
    //////////////////////////////////////////////////////////////////////////

    (None) => {
        None::<Option<u8>>
    };

    (True) => {
        true
    };

    (False) => {
        false
    };

    ([]) => {
        ::pyo3::Python::with_gil(|py| {
            ::pyo3::types::PyList::empty_bound(py).unbind()
        })
    };

    ([ $($tt:tt)+ ]) => {
        ::pyo3::Python::with_gil(|py| {
            let _items = py_internal!(@array [] $($tt)+);
            ::pyo3::types::PyList::new_bound(py, _items).unbind()
        })
    };

    ({}) => {
        ::pyo3::Python::with_gil(|py| {
            ::pyo3::types::PyDict::new_bound(py).unbind()
        })
    };

    ({ $($tt:tt)+ }) => {
        ::pyo3::Python::with_gil(|py| {
            use ::pyo3::types::PyDictMethods;
            let _dict = ::pyo3::types::PyDict::new_bound(py);
            py_internal!(@object _dict () ($($tt)+) ($($tt)+));
            _dict.unbind()
        })
    };

    // Any Serialize type: numbers, strings, struct literals, variables etc.
    // Must be below every other rule.
    ($other:expr) => {
        {
            use ::pyo3::{IntoPy, PyAny, Py};
            ::pyo3::Python::with_gil(|py| -> Py<PyAny> { $other.into_py(py) } )
        }
    };
}

#[macro_export]
macro_rules! py_internal_vec {
    ($($content:tt)*) => {
        vec![$($content)*]
    };
}

#[macro_export]
macro_rules! py_unexpected {
    () => {};
}
