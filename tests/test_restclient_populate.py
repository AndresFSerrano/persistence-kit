import json

import pytest
from pydantic import BaseModel

from persistence_kit.restclient import (
    Expansion,
    MemoryRestClient,
    RestRelationRegistry,
    expand,
    expand_chain,
    expand_recursive,
    populate,
)
from persistence_kit.restclient.contracts import RestResponse


class City(BaseModel):
    id: int
    name: str


class State(BaseModel):
    id: int
    name: str
    country: int
    cities: list[City] = []


class Country(BaseModel):
    id: int
    name: str
    states: list[State] = []


STATES = {
    1: [
        {"id": 10, "name": "State A", "country": 1},
        {"id": 11, "name": "State B", "country": 1},
    ]
}
CITIES = {
    (1, 10): [{"id": 100, "name": "City A"}],
    (1, 11): [{"id": 101, "name": "City B"}],
}


def _json_response(payload):
    return RestResponse(
        200,
        {"Content-Type": "application/json"},
        json.dumps(payload).encode("utf-8"),
        "memory",
    )


def _geo_client():
    client = MemoryRestClient()
    client.stub("GET", "countries", json=[{"id": 1, "name": "Country X"}])

    def states(request):
        return _json_response(STATES.get(int(request.params["country"]), []))

    def cities(request):
        key = (int(request.params["country"]), int(request.params["state"]))
        return _json_response(CITIES.get(key, []))

    client.stub("GET", "states", handler=states)
    client.stub("GET", "cities", handler=cities)
    return client


@pytest.mark.asyncio
async def test_expand_attaches_and_returns_frontier():
    parents = [{"id": 1}]

    async def fetch(parent):
        return STATES.get(parent["id"], [])

    frontier = await expand(parents, fetch, "states")

    assert len(parents[0]["states"]) == 2
    assert len(frontier) == 2


@pytest.mark.asyncio
async def test_expand_chain_builds_country_state_city():
    client = _geo_client()
    countries = await client.get_as("countries", list[Country], params={})

    await expand_chain(
        countries,
        [
            Expansion(
                fetch=lambda c: client.get_as(
                    "states", list[State], params={"country": c.id}
                ),
                attach="states",
            ),
            Expansion(
                fetch=lambda s: client.get_as(
                    "cities", list[City], params={"country": s.country, "state": s.id}
                ),
                attach="cities",
            ),
        ],
    )

    assert countries[0].states[0].cities[0].name == "City A"
    assert countries[0].states[1].cities[0].name == "City B"


@pytest.mark.asyncio
async def test_populate_declarative_include():
    client = _geo_client()
    registry = RestRelationRegistry()
    registry.relation(
        "country",
        "states",
        service="states",
        model=State,
        params=lambda c: {"country": c.id},
        child_resource="state",
    )
    registry.relation(
        "state",
        "cities",
        service="cities",
        model=City,
        params=lambda s: {"country": s.country, "state": s.id},
    )

    countries = await client.get_as("countries", list[Country], params={})
    await populate(client, "country", countries, include=["states.cities"], registry=registry)

    assert len(countries[0].states) == 2
    assert countries[0].states[0].cities[0].name == "City A"


@pytest.mark.asyncio
async def test_expand_recursive_walks_homogeneous_tree():
    tree = {1: [2, 3], 2: [4], 3: [], 4: []}
    root = {"id": 1}
    seen = {}

    async def fetch(node):
        children = [{"id": child} for child in tree[node["id"]]]
        seen[node["id"]] = [c["id"] for c in children]
        return children

    await expand_recursive([root], fetch, "children")

    assert seen == {1: [2, 3], 2: [4], 3: [], 4: []}
    assert root["children"][0]["children"][0]["id"] == 4
