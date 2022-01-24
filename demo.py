#!/usr/bin/env python3

from tempfile import NamedTemporaryFile

import ray

from persistent_actor import ActorState, safe_state


@ray.remote(max_restarts=-1, max_task_retries=-1)
class Counter:
    def __init__(self, state_file_name: str) -> None:
        self._state: ActorState = ActorState(state_file_name)
        # Load the pervious state or initialize to zero if there is no state yet.
        self.counter: int = self.state.get("counter", 0)

    @property
    def state(self) -> ActorState:
        return self._state

    @safe_state("counter")
    def increment(self) -> int:
        self.counter += 1
        return self.counter


def main() -> None:
    print("Initializing ray ...")
    ray.init()
    print("Starting Counter loop ...")
    with NamedTemporaryFile(mode="w+b") as state_file:
        for _ in range(3):
            counter_actor = Counter.remote(state_file.name)
            print(ray.get(counter_actor.increment.remote()))
    print("Finished Counter loop.")
    ray.shutdown()
    print("Ray shutdown successfully!")


if __name__ == "__main__":
    main()
