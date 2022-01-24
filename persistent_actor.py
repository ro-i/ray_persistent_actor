#!/usr/bin/env python3

from functools import wraps as functools_wraps
from sqlitedict import SqliteDict
from typing import Any, Dict, Iterable, Protocol, Tuple


############################################
# This code would integrate with the ray API
############################################

class PersistentActor(Protocol):
    @property
    def state(self) -> Any:
        ...

# see https://stackoverflow.com/a/69528375
class PersistentActorMethod(Protocol):
    def __call__(_self, self: PersistentActor, *args: Any, **kwargs: Any) -> Any:
        ...


# see also https://docs.ray.io/en/latest/ray-design-patterns/\
# fault-tolerance-actor-checkpointing.html
class ActorState:
    def __init__(self, db_path: str) -> None:
        self.db: SqliteDict = SqliteDict(db_path)

    @property
    def data(self) -> Dict[str, Any]:
        return dict(self.db.items())

    def get(self, attr: str, default: Any) -> Any:
        if attr in self.db:
            return self.db[attr]
        self.db[attr] = default
        self.db.commit()
        return default

    def update(self, data: Iterable[Tuple[str, Any]]) -> None:
        for attr, value in data:
            self.db[attr] = value
        self.db.commit()


def safe_state(*attrs: str) -> Any:
    def outer_wrapper(func: PersistentActorMethod) -> Any:
        @functools_wraps(func)
        def wrapper(actor_instance: "PersistentActor", *args: Any, **kwargs: Any) -> Any:
            result = func(actor_instance, *args, **kwargs)
            actor_instance.state.update(
                (attr, actor_instance.__dict__[attr])
                for attr in attrs
            )
            return result
        return wrapper
    return outer_wrapper
