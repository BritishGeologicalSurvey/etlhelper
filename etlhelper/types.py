from typing import (
    Any,
    Protocol,
    Sequence,
)


class Connection(Protocol):
    def close(self) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    def cursor(self):  # noqa Cursor Protocol not defined
        ...


Row = Sequence[Any]
Chunk = list[Row]
