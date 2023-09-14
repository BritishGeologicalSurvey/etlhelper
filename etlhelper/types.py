from typing import (
    Any,
    Collection,
    Protocol,
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


Row = Collection[Any]
Chunk = list[Row]
