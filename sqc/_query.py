from ._transpiler import SqlVisitor


class SqcQuery:
    def __init__(self, query: str):
        self._code = None
        self.query = query

    def execute(self, data, **kwargs):
        return list(self.iter(data, **kwargs))

    def scalar(self, data, **kwargs):
        pass

    def iter(self, data, **kwargs):
        if self._code is None:
            self._code = SqlVisitor(self.query).walk()

        return self._code(data)
