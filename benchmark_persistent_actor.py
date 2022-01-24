#!/usr/bin/env python3

"""Benchmark for rays ownership system.

Based on https://github.com/stephanie-wang/ownership-nsdi2021-artifact/\
blob/main/recovery-microbenchmark/reconstruction.py
"""

import abc
import argparse
import csv
import numpy as np
import os
import os.path
import time
from queue import SimpleQueue as Queue
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any, Final, Generic, List, Sequence, Type, TypeVar, Union, final

import ray
from ray.cluster_utils import Cluster
from ray.node import Node

from persistent_actor import ActorState, safe_state


LARGE_ARRAY_SIZE: Final[int] = 10 * 1024 * 1024
NUM_NODES: Final[int] = 1
T = TypeVar('T')


class TestActor(abc.ABC, Generic[T]):
    """Abstract base class for actor classes used for this benchmark."""

    def __init__(self, _: str = "", can_die: bool = False) -> None:
        self.can_die: bool = can_die

    @final
    def die(self) -> None:
        if not self.can_die:
            return
        self.can_die = False
        os._exit(0)

    @final
    def enable_die(self) -> None:
        self.can_die = True

    @abc.abstractmethod
    def get_data(self) -> T:
        ...

    @abc.abstractmethod
    def _process(self, arg: T) -> T:
        ...

    @final
    def process(self, delay_ms: float, arg: T) -> T:
        time.sleep(delay_ms / 1000)
        return self._process(arg)

    @abc.abstractmethod
    def to_string(self) -> str:
        ...


@ray.remote(max_restarts=-1, max_task_retries=-1)
class SmallActor(TestActor[int]):
    def __init__(self, _: str = "", can_die: bool = False) -> None:
        TestActor.__init__(self, can_die=can_die)
        self.counter: int = 1

    def get_data(self) -> int:
        return self.counter

    def _process(self, arg: int) -> int:
        self.counter += arg
        return self.counter

    def to_string(self) -> str:
        return f"{self.counter}"


@ray.remote(max_restarts=-1, max_task_retries=-1)
class SmallActorSafe(TestActor[int]):
    def __init__(self, state_file_path: str, can_die: bool = False) -> None:
        TestActor.__init__(self, state_file_path, can_die)
        self.state: ActorState = ActorState(state_file_path)
        self.counter: int = self.state.get("counter", 1)

    def get_data(self) -> int:
        return self.counter

    @safe_state("counter")
    def _process(self, arg: int) -> int:
        self.counter += arg
        return self.counter

    def to_string(self) -> str:
        return f"{self.counter}"


@ray.remote(max_restarts=-1, max_task_retries=-1)
class LargeActor(TestActor[np.ndarray]):
    def __init__(self, _: str = "", can_die: bool = False) -> None:
        TestActor.__init__(self, can_die=can_die)
        self.array: np.ndarray = np.ones(LARGE_ARRAY_SIZE, dtype=np.uint8)

    def get_data(self) -> np.ndarray:
        return self.array

    def _process(self, arg: np.ndarray) -> np.ndarray:
        self.array += arg
        return self.array

    def to_string(self) -> str:
        return f"{self.array}"


@ray.remote(max_restarts=-1, max_task_retries=-1)
class LargeActorSafe(TestActor[np.ndarray]):
    def __init__(self, state_file_path: str, can_die: bool = False) -> None:
        TestActor.__init__(self, state_file_path, can_die)
        self.state: ActorState = ActorState(state_file_path)
        self.array: np.ndarray = self.state.get("array", np.ones(LARGE_ARRAY_SIZE, dtype=np.uint8))

    def get_data(self) -> np.ndarray:
        return self.array

    @safe_state("array")
    def _process(self, arg: np.ndarray) -> np.ndarray:
        self.array += arg
        return self.array

    def to_string(self) -> str:
        return f"{self.array}"


def benchmark(args: argparse.Namespace, state_dir: str,) -> None:
    actor_class: Type[TestActor[Any]]

    nodes: List[Node] = ray.nodes()
    while len(nodes) < NUM_NODES + 1:
        time.sleep(1)
        print(f"{len(nodes)} nodes found, waiting for nodes to join")
        nodes = ray.nodes()
    print("All nodes joined")

    print(f"Running {args.num_rounds} rounds of {args.delay_ms} ms each")

    if args.safe_state:
        actor_class = LargeActorSafe if args.large else SmallActorSafe
    else:
        actor_class = LargeActor if args.large else SmallActor
    print(f"Create {args.num_actors} actors ...")
    actor_handles: Final[List["ray.ObjectRef[TestActor[Any]]"]] = [
        create_actor(actor_class, state_dir, i) for i in range(args.num_actors)
    ]
    # Force initialization of actors and print their state.
    print(
        "Initial state:",
        ' '.join(str(ray_get_single(ah.get_data.remote())) for ah in actor_handles)
    )

    print(f"Start running benchmark ...")
    result: Queue[float] = Queue()
    thread: Thread = Thread(target=run, args=(actor_handles, args.num_rounds, args.delay_ms, result))
    thread.start()
    if args.failure:
        sleep: float = (args.num_rounds * args.num_actors * (args.delay_ms / 1000)) / 2
        print(f"Failure in {sleep} seconds")
        kill_actors(actor_handles, sleep, args.num_actors_kill)
    thread.join()
    duration: float = result.get_nowait()
    print(f"Task delay {args.delay_ms} ms. Duration {duration}")

    if args.output:
        file_exists: bool = os.path.exists(args.output)
        with open(args.output, 'a') as csvfile:
            fieldnames: Sequence[str] = ['system', 'large', 'delay_ms', 'duration', 'failure']
            writer: csv.DictWriter[Any] = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                'system': "safe_actor" if args.safe_state else "default",
                'large': args.large,
                'delay_ms': args.delay_ms,
                'duration': duration,
                'failure': args.failure,
            })

    if args.timeline:
        ray.timeline(filename=args.timeline)


def create_actor(
    actor_class: Type[TestActor[Any]],
    state_dir: str,
    i: int
) -> "ray.ObjectRef[TestActor[Any]]":
    return actor_class.remote(f"{state_dir}{os.path.sep}{i}")


def kill_actors(
    actor_handles: List["ray.ObjectRef[TestActor[Any]]"],
    sleep: float,
    num_actors_kill: int
) -> None:
    time.sleep(sleep)
    # Try to select the actors evenly distributed and rather in the middle.
    # Example for num_actors_kill = 3 and num_actors = 20:
    # --x------x------x---
    index_step: int = len(actor_handles) // (num_actors_kill + 1)
    for index in range(index_step // 2, len(actor_handles), index_step):
        data: Any = ray_get_single(actor_handles[index].get_data.remote())
        print(f"Killing {actor_handles[index]} ... State before killing: {data}")
        # Caution: We need to ensure that an actor cannot die an infinite number
        # of times! Otherwise, we get stuck in a loop, because technically, the
        # die() method of the actor fails, so that it gets re-executed again and
        # again.
        ray_get_single(actor_handles[index].enable_die.remote())
        ray_get_single(actor_handles[index].die.remote())
        data = ray_get_single(actor_handles[index].get_data.remote())
        print(f"Killed {actor_handles[index]}. State after killing: {data}")


def ray_get_single(ref: "ray.ObjectRef[Any]") -> Any:
    result: Union[Any, List[Any]] = ray.get(ref)
    return result[0] if isinstance(result, list) else result


def run(
    actor_handles: List[TestActor[Any]],
    num_rounds: int,
    delay_ms: float,
    queue: Queue[float]
) -> None:
    # Actors need to be stored as variable.
    # see https://github.com/ray-project/ray/issues/6265
    # Build intermediate results.
    # see https://github.com/ray-project/ray/issues/3644
    start: float = time.time()
    for i in range(num_rounds):
        print(f"Run round {i + 1} ...")
        current: Any = ray_get_single(actor_handles[0 if i % 2 else -1].get_data.remote())
        for ah in actor_handles[1:] if i % 2 == 0 else actor_handles[-2::-1]:
            current = ray_get_single(ah.process.remote(delay_ms, current))
    duration: float = time.time() - start
    print(' '.join(map(lambda ah: str(ray_get_single(ah.get_data.remote())), actor_handles)))
    queue.put(duration)


def main() -> None:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--delay-ms", required=True, type=int,
        help="amount of milliseconds to delay actor method execution"
    )
    parser.add_argument(
        "--failure", action="store_true",
        help="kill actors during execution"
    )
    parser.add_argument(
        "--large", action="store_true",
        help="enable large actor state"
    )
    parser.add_argument(
        "--num-actors", type=int, default=20,
        help="number of actors to run"
    )
    parser.add_argument(
        "--num-actors-kill", type=int, default=3,
        help="number of actors to kill when --failure is specified"
    )
    parser.add_argument(
        "--num-rounds", type=int, default=20,
        help="number of rounds to run the code"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="filename for csv data"
    )
    parser.add_argument(
        "--safe-state", action="store_true",
        help="use prototype for persistent actor state"
    )
    parser.add_argument(
        "--timeline", type=str, default=None,
        help="output filename for timeline"
    )
    args: argparse.Namespace = parser.parse_args()

    # see https://docs.ray.io/en/latest/auto_examples/testing-tips.html\
    # #tip-4-create-a-mini-cluster-with-ray-cluster-utils-cluster
    # Start a head-node for the cluster.
    cluster: Cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "num_cpus": 4,
        }
    )
    print(f"Started local cluster on {cluster.address}")
    for _ in range(NUM_NODES):
        node: Node = cluster.add_node()
        print(f"Added node {node.node_ip_address}:{node.node_manager_port}")

    print(ray.init(address=cluster.address))
    with TemporaryDirectory() as state_dir:
        benchmark(args, state_dir)
    ray.shutdown()


if __name__ == "__main__":
    main()
