#[macro_export]
macro_rules! query {
    ($query:expr) => {
        ::pyo3::Python::with_gil(|py| ::sqc::query(py, $query, None))
    };
    ($query:expr, $ctx:expr) => {
        ::pyo3::Python::with_gil(|py| ::sqc::query(py, $query, Some($ctx.into())))
    };
}

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
    ($($json:tt)+) => {{
        use ::pyo3::{prelude::*};
        ::pyo3::Python::with_gil(|py| {
            py_internal!(py, $($json)+)
        })
    }};
}

#[macro_export]
macro_rules! py_internal {
    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an tuple.
    //
    // Must be invoked as: py_internal!(@tuple () $($tt)*)
    //////////////////////////////////////////////////////////////////////////

    // Done with trailing comma.
    ($py:ident, @tuple ($($elems:expr,)*)) => {
        py_internal_vec![$($elems,)*]
    };

    // Done without trailing comma.
    ($py:ident, @tuple ($($elems:expr),*)) => {
        py_internal_vec![$($elems),*]
    };

    // Next element is `null`.
    ($py:ident, @tuple ($($elems:expr,)*) None $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, None)) $($rest)*)
    };

    // Next element is `true`.
    ($py:ident, @tuple ($($elems:expr,)*) True $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, True)) $($rest)*)
    };

    // Next element is `false`.
    ($py:ident, @tuple ($($elems:expr,)*) False $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, False)) $($rest)*)
    };

    // Next element is an array.
    ($py:ident, @tuple ($($elems:expr,)*) [$($array:tt)*] $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, [$($array)*])) $($rest)*)
    };

    // Next element is a map.
    ($py:ident, @tuple ($($elems:expr,)*) {$($map:tt)*} $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, {$($map)*})) $($rest)*)
    };

    // Next element is a tuple.
    ($py:ident, @tuple ($($elems:expr,)*) ($($tuple:tt)*) $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, ($($tuple)*))) $($rest)*)
    };

    // Next element is an expression followed by comma.
    ($py:ident, @tuple ($($elems:expr,)*) $next:expr, $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, $next),) $($rest)*)
    };

    // Last element is an expression with no trailing comma.
    ($py:ident, @tuple ($($elems:expr,)*) $last:expr) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, $last)))
    };

    // Comma after the most recent element.
    ($py:ident, @tuple ($($elems:expr),*) , $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)*) $($rest)*)
    };

    // Unexpected token after most recent element.
    ($py:ident, @tuple ($($elems:expr),*) $unexpected:tt $($rest:tt)*) => {
        py_unexpected!($unexpected)
    };

    //////////////////////////////////////////////////////////////////////////
    // TT muncher for parsing the inside of an array [...].
    //
    // Must be invoked as: py_internal!(@array [] $($tt)*)
    //////////////////////////////////////////////////////////////////////////

    // Done with trailing comma.
    ($py:ident, @array [$($elems:expr,)*]) => {
        py_internal_vec![$($elems,)*]
    };

    // Done without trailing comma.
    ($py:ident, @array [$($elems:expr),*]) => {
        py_internal_vec![$($elems),*]
    };

    // Next element is `null`.
    ($py:ident, @array [$($elems:expr,)*] None $($rest:tt)*) => {
        py_internal!($py, @array [$($elems,)* py_internal!($py, None)] $($rest)*)
    };

    // Next element is `true`.
    ($py:ident, @array [$($elems:expr,)*] True $($rest:tt)*) => {
        py_internal!($py, @array [$($elems,)* py_internal!($py, True)] $($rest)*)
    };

    // Next element is `false`.
    ($py:ident, @array [$($elems:expr,)*] False $($rest:tt)*) => {
        py_internal!($py, @array [$($elems,)* py_internal!($py, False)] $($rest)*)
    };

    // Next element is an array.
    ($py:ident, @array [$($elems:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
        py_internal!($py, @array [$($elems,)* py_internal!($py, [$($array)*])] $($rest)*)
    };

    // Next element is a map.
    ($py:ident, @array [$($elems:expr,)*] {$($map:tt)*} $($rest:tt)*) => {
        py_internal!($py, @array [$($elems,)* py_internal!($py, {$($map)*})] $($rest)*)
    };

    // Next element is a tuple.
    ($py:ident, @array [$($elems:expr,)*] ($($tuple:tt)*) $($rest:tt)*) => {
        py_internal!($py, @tuple ($($elems,)* py_internal!($py, ($($tuple)*))) $($rest)*)
    };

    // Next element is an expression followed by comma.
    ($py:ident, @array [$($elems:expr,)*] $next:expr, $($rest:tt)*) => {
        py_internal!($py, @array [$($elems,)* py_internal!($py, $next),] $($rest)*)
    };

    // Last element is an expression with no trailing comma.
    ($py:ident, @array [$($elems:expr,)*] $last:expr) => {
        py_internal!($py, @array [$($elems,)* py_internal!($py, $last)])
    };

    // Comma after the most recent element.
    ($py:ident, @array [$($elems:expr),*] , $($rest:tt)*) => {
        py_internal!($py, @array [$($elems,)*] $($rest)*)
    };

    // Unexpected token after most recent element.
    ($py:ident, @array [$($elems:expr),*] $unexpected:tt $($rest:tt)*) => {
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
    ($py:ident, @object $object:ident () () ()) => {};

    // Insert the current entry followed by trailing comma.
    ($py:ident, @object $object:ident [$($key:tt)+] ($value:expr) , $($rest:tt)*) => {
        $object.set_item(($($key)+), $value).unwrap();
        py_internal!($py, @object $object () ($($rest)*) ($($rest)*));
    };

    // Current entry followed by unexpected token.
    ($py:ident, @object $object:ident [$($key:tt)+] ($value:expr) $unexpected:tt $($rest:tt)*) => {
        py_unexpected!($unexpected);
    };

    // Insert the last entry without trailing comma.
    ($py:ident, @object $object:ident [$($key:tt)+] ($value:expr)) => {
        $object.set_item(($($key)+), $value).unwrap();
    };

    // Next value is `null`.
    ($py:ident, @object $object:ident ($($key:tt)+) (: None $($rest:tt)*) $copy:tt) => {
        py_internal!($py, @object $object [$($key)+] (py_internal!($py, None)) $($rest)*);
    };

    // Next value is `true`.
    ($py:ident, @object $object:ident ($($key:tt)+) (: True $($rest:tt)*) $copy:tt) => {
        py_internal!($py, @object $object [$($key)+] (py_internal!($py, True)) $($rest)*);
    };

    // Next value is `false`.
    ($py:ident, @object $object:ident ($($key:tt)+) (: False $($rest:tt)*) $copy:tt) => {
        py_internal!($py, @object $object [$($key)+] (py_internal!($py, False)) $($rest)*);
    };

    // Next value is an array.
    ($py:ident, @object $object:ident ($($key:tt)+) (: [$($array:tt)*] $($rest:tt)*) $copy:tt) => {
        py_internal!($py, @object $object [$($key)+] (py_internal!($py, [$($array)*])) $($rest)*);
    };

    // Next value is a tuple.
    ($py:ident, @object $object:ident ($($key:tt)+) (: ($($tuple:tt)*) $($rest:tt)*) $copy:tt) => {
        py_internal!($py, @object $object [$($key)+] (py_internal!($py, ($($tuple)*))) $($rest)*);
    };

    // Next value is a map.
    ($py:ident, @object $object:ident ($($key:tt)+) (: {$($map:tt)*} $($rest:tt)*) $copy:tt) => {
        py_internal!($py, @object $object [$($key)+] (py_internal!($py, {$($map)*})) $($rest)*);
    };

    // Next value is an expression followed by comma.
    ($py:ident, @object $object:ident ($($key:tt)+) (: $value:expr , $($rest:tt)*) $copy:tt) => {
        py_internal!($py, @object $object [$($key)+] (py_internal!($py, $value)) , $($rest)*);
    };

    // Last value is an expression with no trailing comma.
    ($py:ident, @object $object:ident ($($key:tt)+) (: $value:expr) $copy:tt) => {
        py_internal!($py, @object $object [$($key)+] (py_internal!($py, $value)));
    };

    // Missing value for last entry. Trigger a reasonable error message.
    ($py:ident, @object $object:ident ($($key:tt)+) (:) $copy:tt) => {
        // "unexpected end of macro invocation"
        py_internal!();
    };

    // Missing colon and value for last entry. Trigger a reasonable error
    // message.
    ($py:ident, @object $object:ident ($($key:tt)+) () $copy:tt) => {
        // "unexpected end of macro invocation"
        py_internal!();
    };

    // Misplaced colon. Trigger a reasonable error message.
    ($py:ident, @object $object:ident () (: $($rest:tt)*) ($colon:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `:`".
        py_unexpected!($colon);
    };

    // Found a comma inside a key. Trigger a reasonable error message.
    ($py:ident, @object $object:ident ($($key:tt)*) (, $($rest:tt)*) ($comma:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `,`".
        py_unexpected!($comma);
    };

    // Key is fully parenthesized. This avoids clippy double_parens false
    // positives because the parenthesization may be necessary here.
    ($py:ident, @object $object:ident () (($key:expr) : $($rest:tt)*) $copy:tt) => {
        py_internal!(@object $object ($key) (: $($rest)*) (: $($rest)*));
    };

    // Refuse to absorb colon token into key expression.
    ($py:ident, @object $object:ident ($($key:tt)*) (: $($unexpected:tt)+) $copy:tt) => {
        json_expect_expr_comma!($($unexpected)+);
    };

    // Munch a token into the current key.
    ($py:ident, @object $object:ident ($($key:tt)*) ($tt:tt $($rest:tt)*) $copy:tt) => {
        py_internal!($py, @object $object ($($key)* $tt) ($($rest)*) ($($rest)*));
    };

    //////////////////////////////////////////////////////////////////////////
    // The main implementation.
    //
    // Must be invoked as: py_internal!($($json)+)
    //////////////////////////////////////////////////////////////////////////

    ($py:ident, None) => {
        $py.None()
    };

    ($py:ident, True) => {
        true.into_py($py)
    };

    ($py:ident, False) => {
        false.into_py($py)
    };

    ($py:ident, ()) => {
        ::pyo3::types::PyTuple::empty_bound($py).to_object($py)
    };

    ($py:ident, ( $($tt:tt)+ )) => {{
        let _items = py_internal!($py, @array [] $($tt)+);
        ::pyo3::types::PyTuple::new_bound($py, _items).to_object($py)
    }};

    ($py:ident, []) => {
        ::pyo3::types::PyList::empty_bound($py).to_object($py)
    };

    ($py:ident, [ $($tt:tt)+ ]) => {{
        let _items = py_internal!($py, @array [] $($tt)+);
        ::pyo3::types::PyList::new_bound($py, _items).to_object($py)
    }};

    ($py:ident, {}) => {
        ::pyo3::types::PyDict::new_bound($py).to_object($py)
    };

    ($py:ident, { $($tt:tt)+ }) => {{
        use ::pyo3::types::PyDictMethods;
        let _dict = ::pyo3::types::PyDict::new_bound($py);
        py_internal!($py, @object _dict () ($($tt)+) ($($tt)+));
        _dict.to_object($py)
    }};

    // Any Serialize type: numbers, strings, struct literals, variables etc.
    // Must be below every other rule.
    ($py:ident, $other:expr) => {
        $other.to_object($py)
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
