from collections.abc import Awaitable, Callable, Iterable
from typing import Protocol


class Seeder(Protocol):
    async def run(self) -> None: ...


SeederTask = Seeder | Callable[[], Awaitable[None]]


class SeederProvider:
    _instance: "SeederProvider | None" = None

    def __new__(cls) -> "SeederProvider":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._seeders = []
            cls._instance._initialized = False
        return cls._instance

    def register(self, seeder: SeederTask | Iterable[SeederTask]) -> None:
        if isinstance(seeder, Iterable) and not isinstance(seeder, (str, bytes)):
            self._seeders.extend(seeder)
            return
        self._seeders.append(seeder)

    async def run_all(self) -> None:
        if self._initialized:
            return

        for seeder in self._seeders:
            if hasattr(seeder, "run"):
                await seeder.run()  # type: ignore[union-attr]
            else:
                await seeder()

        self._initialized = True
