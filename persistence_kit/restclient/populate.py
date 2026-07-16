from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

from persistence_kit.restclient.aggregate import expand

ParamsBuilder = Callable[[Any], Mapping[str, Any]]


@dataclass(slots=True)
class RestRelation:
    """A relation reachable through a REST call: which service returns the
    children, how to derive their query params from the parent, and where to
    attach them. ``child_resource`` names the resource whose own relations apply
    when nesting deeper (defaults to ``attach``)."""

    service: str
    model: Any
    params: ParamsBuilder
    attach: str
    many: bool = True
    child_resource: str | None = None


@dataclass(slots=True)
class RestResource:
    relations: dict[str, RestRelation] = field(default_factory=dict)


class RestRelationRegistry:
    """Declares, per resource, the relations reachable through REST calls, the
    counterpart to the entity relation registry used by the DB populate."""

    def __init__(self) -> None:
        self._resources: dict[str, RestResource] = {}

    def relation(
        self,
        resource: str,
        name: str,
        *,
        service: str,
        model: Any,
        params: ParamsBuilder,
        attach: str | None = None,
        many: bool = True,
        child_resource: str | None = None,
    ) -> "RestRelationRegistry":
        res = self._resources.setdefault(resource, RestResource())
        res.relations[name] = RestRelation(
            service=service,
            model=model,
            params=params,
            attach=attach or name,
            many=many,
            child_resource=child_resource,
        )
        return self

    def relations_of(self, resource: str) -> dict[str, RestRelation]:
        res = self._resources.get(resource)
        return res.relations if res else {}


def _split_include(include: Iterable[str]) -> dict[str, list[str]]:
    tree: dict[str, list[str]] = {}
    for inc in include:
        root, _, rest = inc.partition(".")
        tree.setdefault(root, [])
        if rest:
            tree[root].append(rest)
    return tree


def _set_field(node: Any, name: str, value: Any) -> None:
    if isinstance(node, dict):
        node[name] = value
    else:
        setattr(node, name, value)


async def populate(
    client: Any,
    resource: str,
    nodes: Any,
    include: Iterable[str],
    *,
    registry: RestRelationRegistry,
    concurrency: int = 8,
    cache: dict[Any, Any] | None = None,
) -> list[Any]:
    """Hydrate ``nodes`` in place following ``include`` paths, recursing into
    nested relations. ``include=["departamentos.municipios"]`` fetches each
    node's departments and, for every department, its municipalities.

    Mirrors the DB populate: declarative relations, dotted include paths,
    recursion, and a per-run cache that dedupes identical child requests.
    """
    nodes = list(nodes)
    if not nodes:
        return nodes
    if cache is None:
        cache = {}

    relations = registry.relations_of(resource)
    tree = _split_include(include)

    for name, child_includes in tree.items():
        relation = relations.get(name)
        if relation is None:
            continue

        model = list[relation.model] if relation.many else relation.model

        async def fetch(parent: Any, relation: RestRelation = relation, model: Any = model) -> list[Any]:
            params = dict(relation.params(parent))
            key = (relation.service, tuple(sorted(params.items())))
            if key in cache:
                return cache[key]
            result = await client.request_as("GET", relation.service, model, params=params)
            resolved = list(result) if relation.many else ([result] if result is not None else [])
            cache[key] = resolved
            return resolved

        if relation.many:
            frontier = await expand(nodes, fetch, relation.attach, concurrency=concurrency)
        else:
            attach_name = relation.attach

            def attach_one(node: Any, children: list[Any], attach_name: str = attach_name) -> None:
                _set_field(node, attach_name, children[0] if children else None)

            frontier = await expand(nodes, fetch, attach_one, concurrency=concurrency)

        if child_includes and frontier:
            child_resource = relation.child_resource or relation.attach
            await populate(
                client,
                child_resource,
                frontier,
                child_includes,
                registry=registry,
                concurrency=concurrency,
                cache=cache,
            )

    return nodes
