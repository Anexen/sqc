from sqc import sqc

data = [
    {"a": 1, "b": 1},
    {"a": 2, "b": 1},
    {"a": 3, "b": 2},
]


def test_select():
    result = sqc("SELECT a").execute(data)
    assert result == [{"a": 1}, {"a": 2}, {"a": 3}]


def test_where():
    result = sqc("SELECT a WHERE b > 1").execute(data)
    assert result == [{"a": 3}]
