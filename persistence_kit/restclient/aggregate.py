from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Sequence

Fetch = Callable[[Any], Awaitable[Sequence[Any]]]
Attach = "str | Callable[[Any, list[Any]], None]"


def _attach(node: Any, children: list[Any], attach: Any) -> None:
    if callable(attach):
        attach(node, children)
    elif isinstance(node, dict):
        node[attach] = children
    else:
        setattr(node, attach, children)


@dataclass(slots=True)
class Expansion:
    """One level of a hierarchy: how to fetch a node's children and where to
    place them on the node."""

    fetch: Fetch
    attach: Any


async def expand(
    nodes: Sequence[Any],
    fetch: Fetch,
    attach: Any,
    *,
    concurrency: int = 8,
) -> list[Any]:
    """Fetch the children of every node concurrently, attach them, and return the
    flattened next frontier so levels can be chained.

    ``fetch`` is any async ``node -> children``; it needs no knowledge of the
    transport. ``attach`` is a field name (attribute or dict key) or a callable
    ``(node, children) -> None``.
    """
    nodes = list(nodes)
    if not nodes:
        return []

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def run(node: Any) -> list[Any]:
        async with semaphore:
            children = list(await fetch(node))
        _attach(node, children, attach)
        return children

    results = await asyncio.gather(*(run(node) for node in nodes))

    frontier: list[Any] = []
    for children in results:
        frontier.extend(children)
    return frontier


async def expand_chain(
    roots: Sequence[Any],
    expansions: Sequence[Expansion],
    *,
    concurrency: int = 8,
) -> list[Any]:
    """Apply several expansion levels in a single call. Each level expands the
    frontier produced by the previous one, so ``roots`` ends up fully nested."""
    frontier: list[Any] = list(roots)
    for expansion in expansions:
        frontier = await expand(
            frontier, expansion.fetch, expansion.attach, concurrency=concurrency
        )
    return list(roots)


async def expand_recursive(
    nodes: Sequence[Any],
    fetch: Fetch,
    attach: Any,
    *,
    concurrency: int = 8,
    max_depth: int | None = None,
) -> list[Any]:
    """Expand a self-referential hierarchy of homogeneous nodes until no children
    are returned or ``max_depth`` is reached."""
    roots = list(nodes)
    frontier = roots
    depth = 0
    while frontier and (max_depth is None or depth < max_depth):
        frontier = await expand(frontier, fetch, attach, concurrency=concurrency)
        depth += 1
    return roots
