from __future__ import annotations

import dataclasses
import typing
from typing import Any, Callable

from pydantic import BaseModel, TypeAdapter

from persistence_kit.restclient.contracts import RestResponse

Selector = str | Callable[[Any], Any]


def _is_plain_callable(model: Any) -> bool:
    """True for a function/lambda mapper, False for types and typing constructs
    such as ``list[Pais]`` (which are callable but must go through TypeAdapter)."""
    if isinstance(model, type):
        return False
    if typing.get_origin(model) is not None:
        return False
    return callable(model)


def _select(data: Any, selector: Selector | None) -> Any:
    if selector is None:
        return data
    if callable(selector):
        return selector(data)
    current = data
    for part in selector.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


def decode(source: Any, model: Any, *, select: Selector | None = None) -> Any:
    """Map a response (or raw data) into ``model``.

    ``model`` can be a Pydantic model, a typing construct such as
    ``list[Pais]``, a dataclass, or a plain callable ``data -> T``. When
    ``source`` is a RestResponse its body is decoded by Content-Type first.
    ``select`` extracts a nested field (dotted path or callable) before mapping,
    e.g. ``select="data"`` for ``{"data": [...]}``.
    """
    data = source.body() if isinstance(source, RestResponse) else source
    data = _select(data, select)

    if _is_plain_callable(model):
        return model(data)
    return TypeAdapter(model).validate_python(data)


def encode(
    dto: Any,
    *,
    by_alias: bool = True,
    exclude_none: bool = False,
) -> Any:
    """Serialize a DTO into a JSON-able structure suitable as a request body.

    Handles Pydantic models, dataclasses and sequences of them; anything else is
    returned unchanged.
    """
    if isinstance(dto, BaseModel):
        return dto.model_dump(mode="json", by_alias=by_alias, exclude_none=exclude_none)
    if isinstance(dto, (list, tuple)):
        return [encode(item, by_alias=by_alias, exclude_none=exclude_none) for item in dto]
    if dataclasses.is_dataclass(dto) and not isinstance(dto, type):
        return dataclasses.asdict(dto)
    return dto


class ModelMappingMixin:
    """Adds typed helpers to any RestClient: map responses to DTOs and DTOs to
    request bodies."""

    async def request_as(
        self,
        method: str,
        service: str,
        model: Any,
        *,
        dto: Any = None,
        select: Selector | None = None,
        by_alias: bool = True,
        exclude_none: bool = False,
        json: Any = None,
        **kwargs: Any,
    ) -> Any:
        if dto is not None and json is None:
            json = encode(dto, by_alias=by_alias, exclude_none=exclude_none)
        response = await self.request(method, service, json=json, **kwargs)
        return decode(response, model, select=select)

    async def get_as(self, service: str, model: Any, **kwargs: Any) -> Any:
        return await self.request_as("GET", service, model, **kwargs)

    async def post_as(self, service: str, model: Any, **kwargs: Any) -> Any:
        return await self.request_as("POST", service, model, **kwargs)

    async def put_as(self, service: str, model: Any, **kwargs: Any) -> Any:
        return await self.request_as("PUT", service, model, **kwargs)
