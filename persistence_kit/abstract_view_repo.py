from __future__ import annotations

from abc import ABC, abstractmethod

from typing import Optional, Sequence, TypeVar, Generic, Hashable, Mapping, Any

T = TypeVar("T")
TId = TypeVar("TId", bound=Hashable)

class ViewRepository(ABC, Generic[T, TId]):
    @abstractmethod
    async def get_with(self, entity_id: TId, include: Sequence[str]) -> Optional[dict]: ...

    @abstractmethod
    async def get_by_index_with(
        self,
        index: str,
        value: Hashable,
        include: Sequence[str],
    ) -> Optional[dict]: ...

    @abstractmethod
    async def list_with(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        include: Sequence[str] = (),
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> list[dict]: ...

    @abstractmethod
    async def list_by_fields(
        self,
        criteria: Mapping[str, Hashable | list[Hashable] | Mapping[str, Any]],
        *,
        offset: int = 0,
        limit: Optional[int] = 50,
        include: Sequence[str] = (),
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> list[dict]: ...
