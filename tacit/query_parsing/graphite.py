"""Extract exact series operands from a bounded subset of Graphite targets.

Only concrete paths and verified alias/scale/divideSeries/sumSeries signatures
are accepted. Dynamic selectors and unknown functions cannot prove a complete
operand set, so they return no operands rather than a partial extraction.
"""

from __future__ import annotations

import re

_MAX_EXPRESSION_CHARACTERS = 65_536
_MAX_FUNCTION_DEPTH = 32
_MAX_OPERANDS = 256
_PATH = re.compile(r"[A-Za-z0-9_:-]+(?:\.[A-Za-z0-9_:-]+)*")
_NUMBER = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")


class _UnsupportedTarget(ValueError):
    pass


class _OperandParser:
    def __init__(self, expression: str) -> None:
        self.expression = expression
        self.position = 0

    def _skip_space(self) -> None:
        while self.position < len(self.expression) and self.expression[self.position].isspace():
            self.position += 1

    def _take(self, token: str) -> bool:
        self._skip_space()
        if self.expression.startswith(token, self.position):
            self.position += len(token)
            return True
        return False

    def _require(self, token: str) -> None:
        if not self._take(token):
            raise _UnsupportedTarget

    def _string(self) -> None:
        self._skip_space()
        if self.position >= len(self.expression) or self.expression[self.position] not in "\"'":
            raise _UnsupportedTarget
        quote = self.expression[self.position]
        self.position += 1
        while self.position < len(self.expression):
            character = self.expression[self.position]
            self.position += 1
            if character == quote:
                return
            if character == "\\":
                if self.position >= len(self.expression):
                    raise _UnsupportedTarget
                self.position += 1
            elif character in "\r\n":
                raise _UnsupportedTarget
        raise _UnsupportedTarget

    def _number(self) -> None:
        self._skip_space()
        number = _NUMBER.match(self.expression, self.position)
        if number is None:
            raise _UnsupportedTarget
        self.position = number.end()

    def series(self, depth: int = 0) -> set[str]:
        if depth > _MAX_FUNCTION_DEPTH:
            raise _UnsupportedTarget
        self._skip_space()
        path = _PATH.match(self.expression, self.position)
        if path is None:
            raise _UnsupportedTarget
        name = path.group()
        self.position = path.end()
        if not self._take("("):
            if _NUMBER.fullmatch(name):
                raise _UnsupportedTarget
            return {name}

        # Argument roles are explicit: quoted labels and scalar factors are
        # never series, and unknown signatures never yield partial operands.
        if name not in {"alias", "scale", "divideSeries", "sumSeries"}:
            raise _UnsupportedTarget
        operands = self.series(depth + 1)
        if name == "sumSeries":
            while self._take(","):
                operands.update(self.series(depth + 1))
                if len(operands) > _MAX_OPERANDS:
                    raise _UnsupportedTarget
        else:
            self._require(",")
            if name == "alias":
                self._string()
            elif name == "scale":
                self._number()
            else:
                operands.update(self.series(depth + 1))
        self._require(")")
        if len(operands) > _MAX_OPERANDS:
            raise _UnsupportedTarget
        return operands


def extract_metrics_from_graphite(expression: str) -> set[str]:
    """Return all proven concrete operands, or none for unsupported targets.

    These bounds are local parser limits; the compiler still performs its
    existing aggregate query-text admission before invoking this helper.
    """
    if len(expression) > _MAX_EXPRESSION_CHARACTERS:
        return set()
    parser = _OperandParser(expression)
    try:
        operands = parser.series()
        parser._skip_space()
        return operands if parser.position == len(expression) else set()
    except _UnsupportedTarget:
        return set()
