from __future__ import annotations

from collections.abc import (
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from typing import (
    overload,
    Any,
    Protocol,
    TypeAlias,
    TypeVar
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
    rowcount: int
    def execute(self, *args: Any, **kwargs: Any) -> Self: ...
    def executemany(self, *args: Any, **kwargs: Any) -> None: ...
    def fetchone(self) -> Row: ...
    def fetchmany(self, *args: Any, **kwarg) -> Iterable[Row]: ...
    def fetchall(self) -> Iterable[Row]: ...
    def close(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *args: Any, **kwargs: Any) -> bool | None: ...


# Define a type variable that represents any Cursor.
# `bound` forces it to Cursor's interface.  Covariant means it is only used
# as a return value.
C = TypeVar('C', bound=Cursor, covariant=True)


class Connection(Protocol[C]):
    def close(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

    # Overload cursor specifications as different DBAPI drivers have different
    # signatures.
    @overload
    def cursor(self) -> C: ...
    @overload
    def cursor(self, factory: Any) -> C: ...
    def cursor(self, *args: Any, **kwargs: Any) -> C: ...

    def __enter__(self) -> Self: ...
    def __exit__(self, *args: Any, **kwargs: Any) -> bool | None: ...


DBAPIConnection: TypeAlias = Connection[Cursor]
