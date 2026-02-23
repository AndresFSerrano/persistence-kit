from __future__ import annotations

from abc import ABC, abstractmethod

from typing import Optional, Sequence, TypeVar, Generic, Hashable, Mapping, Any

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)

class Repository(ABC, Generic[T, TId]):
    @abstractmethod
    async def add(self, entity: T) -> None: ...

    @abstractmethod
    async def get(self, entity_id: TId) -> Optional[T]: ...

    @abstractmethod
    async def list(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> Sequence[T]: ...

    @abstractmethod
    async def update(self, entity: T) -> None: ...

    @abstractmethod
    async def delete(self, entity_id: TId) -> None: ...

    @abstractmethod
    async def get_by_index(self, index: str, value: Hashable) -> Optional[T]: ...

    @abstractmethod
    async def list_by_fields(
        self,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
        *,
        offset: int = 0,
        limit: Optional[int] = 50,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> Sequence[T]: ...
