import operator

from convtools import conversion as c
from sqloxide import parse_sql


mapping = {"modulo": "mod", "or": "or_", "and": "and_"}


class SqlVisitor:
    def __init__(self, query):
        self.query = query

    def walk(self):
        queries = parse_sql(self.query, "generic")

        if not queries:
            return []

        return self.visit__Query(queries[0]["Query"]).gen_converter()

    def visit(self, x):
        [(k, v)] = x.items()
        return getattr(self, f"visit__{k}")(v)

    def visit__Query(self, data):
        return self.visit__Select(data["body"]["Select"])

    def visit__Select(self, data):
        return c.iter(
            self.visit__projection(data["projection"]),
            where=self.visit__selection(data["selection"]),
        )

    def visit__projection(self, data):
        return dict(map(self.visit, data))

    def visit__selection(self, data):
        if data is not None:
            return self.visit__BinaryOp(data["BinaryOp"])

    def visit__UnnamedExpr(self, data):
        i = data["Identifier"]
        return i["value"], self.visit__Identifier(i)

    def visit__Identifier(self, data):
        return c.item(data["value"])

    def visit__Nested(self, data):
        return self.visit(data)

    def visit__BinaryOp(self, data):
        op_name = data["op"].lower()
        op = getattr(operator, mapping.get(op_name, op_name))
        return op(self.visit(data["left"]), self.visit(data["right"]))

    def visit__Value(self, data):
        return self.visit(data)

    def visit__Number(self, data):
        return int(data[0])

    def visit__SingleQuotedString(self, data):
        return data
