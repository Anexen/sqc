use pyo3::{exceptions::*, Python};

use rstest::*;

mod fixtures;
mod utils;

#[rstest]
fn test_select_const() {
    let query = r#"
    SELECT
        'Côte d\'Ivoire' AS string_literal,
        'a' + "b" + '''c''' + """d""" AS `ABCD`,
        2 * 10 < 3 * 7 AS boolean_result,
        NOT 10 < 4 * 5 / 2 + 1 AS inversion,
        (2 + 2 * 2) % 2 = 0 AS is_even,
        123456 * 1000 // 33 AS intdiv,
        3 + 4/(2*3*4) - 4/(4*5*6) + 4/(6*7*8) - 4/(8*9*10) + 4/(10*11*12) AS `PI`,
        2 ** (1 + 2) as power,
        None as is_none,
        0.75 -> as_integer_ratio()[0] as numerator,
    "#;

    let result = query!(query).unwrap();

    let expected = py!([{
        "string_literal": "Côte d'Ivoire",
        "ABCD": "abcd",
        "boolean_result": True,
        "inversion": False,
        "is_even": True,
        "intdiv": 3741090,
        "PI": 3. + 4./24. - 4./120. + 4./336. - 4./720. + 4./1320.,
        "power": 8,
        "is_none": None,
        "numerator": 3,
    }]);

    py_assert_eq!(result, expected);
}

#[rstest]
fn test_is_operator() {
    let query = r#"
    SELECT
        None is None as is_none,
        1 is None as one_is_none,
        None is not 1 as none_is_not_1,
        1 < 2 is True as is_true,
        @a is @b and @b is not @c and @c is @c and @c is not 257 and @a is not None AS int_trick,
    "#;

    // CPython trick: when creating an int value in the range of -5 to 256, a reference to an
    // existing object is returned. However, values outside this range are created as separate
    // objects. So, any integer > 256 will have different id
    let a = py!(257);
    let ctx = py!({"@a": &a, "@b": &a, "@c": 257});

    let result = query!(query, ctx).unwrap();

    let expected = py!([{
        "is_none": True,
        "one_is_none": False,
        "none_is_not_1": True,
        "is_true": True,
        "int_trick": True,
    }]);

    py_assert_eq!(result, expected);
}

#[rstest]
fn test_tuples() {
    let query = r#"
    SELECT
        (3) as is_not_tuple,
        (3, ) as trailing_comma,
        (2 + 2 * 2, str(12 // 5), not 1 < 4, None) AS heterogeneous,
        (1, (2, (3), 4), 5) AS nested,
        ('a', ['b', 'c', ['d', 'e']])[1][2][0] as access,
        (1, (2, 3, 4), 5)[-2][-1] as negative_access,
        (1, 2)[100] as out_of_bound,
        (1, 2) + (True, 'a') + (None,) as concat,
        (1, 2) > (1, 1) and (1, 2) < (1, 3) as compare,
        (1, 2) * 2 as repeat,
        len((1,)) as len,
        tuple('abc') as new_from_string,
        tuple([3, None]) as new_from_array,
        tuple(repeat('a', 2)) as wrapping,
    "#;

    let result = query!(query).unwrap();

    let expected = py!([{
        "is_not_tuple": 3,
        "trailing_comma": (3,),
        "heterogeneous": (6, "2", False, None),
        "nested": (1, (2, 3, 4), 5),  // 3 is not a tuple
        "access": "d",
        "negative_access": 4,
        "out_of_bound": None,
        "concat": (1, 2, True, "a", None),
        "compare": True,
        "repeat": (1, 2, 1, 2),
        "len": 1,
        "new_from_string": ("a", "b", "c"),
        "new_from_array": (3, None),
        "wrapping": ("a", "a"),
    }]);

    py_assert_eq!(result, expected);
}

#[rstest]
fn test_tuple_errors() {
    Python::with_gil(|py| {
        let result = query!("select (1,) + [1]").unwrap_err();
        result.is_instance_of::<PyTypeError>(py);

        let result = query!("select tuple(1, 2)").unwrap_err();
        result.is_instance_of::<PyTypeError>(py);

        let result = query!("select (1, 2) < False").unwrap_err();
        result.is_instance_of::<PyTypeError>(py);
    });
}

#[rstest]
fn test_dictionaries() {
    let query = r#"
    SELECT
        {} as empty,
        {'a': [1, 2]}['a'][-1] as access,
        {'a': 1}['b'][100] as missing_key,
        {True: 1, False: 0}[True] as non_string_key,
        {(1, 2): {'s': [{1: 11}]}} as complex_key,
        {'a': 1} | {'b': 2} as concat,
        len({'a': 1, 'b': 2}) as len,
        dict([(1, (2, [False][1])), (3, {4: 5})]) as constructor,
        list({'a': 1, 'B': 2} -> keys()) as keys,
    "#;

    let result = query!(query).unwrap();

    let expected = py!([{
        "empty": {},
        "access": 2,
        "missing_key": None,
        "non_string_key": 1,
        "complex_key": {(1, 2): {"s": [{1: 11}]}},
        "concat": {"a": 1, "b": 2},
        "len": 2,
        "constructor": {1: (2, None), 3: {4: 5}},
        "keys": ["a", "B"],
    }]);

    py_assert_eq!(result, expected);
}
