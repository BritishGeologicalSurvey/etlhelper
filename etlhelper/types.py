from __future__ import annotations

from collections.abc import (
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from typing import (
    Any,
    Protocol,
    TypeAlias,
)
from typing_extensions import Self

# InputRow is the typehint for our function arguments
# whereas Row is the typehint for return values.
InputRow: TypeAlias = Mapping[str, Any] | Sequence[Any]
# The exact type of a Row depends on the row_factory and transform functions used
# and so, it is given the generic type Any
Row: TypeAlias = Any
Chunk: TypeAlias = list[Row]
Parameters: TypeAlias = Mapping[str, Any] | Sequence[Any]
Transform: TypeAlias = Callable[[Chunk], Chunk]


class Cursor(Protocol):
    @property
    def rowcount(self) -> int: ...

    @property
    def arraysize(self) -> int | None: ...

    def execute(self, *args: Any, **kwargs: Any) -> Cursor | None: ...
    def executemany(self, *args: Any, **kwargs: Any) -> Cursor | None: ...
    def fetchone(self) -> Row | None: ...
    def fetchmany(self, size: int, /) -> list[Row]: ...
    def fetchall(self) -> Iterable[Row]: ...
    def close(self) -> None: ...


class Connection(Protocol):
    def close(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

    def cursor(self, *args: Any, **kwargs: Any) -> Cursor: ...

    def __enter__(self) -> Self: ...
    def __exit__(self, *args: Any, **kwargs: Any) -> bool | None: ...
