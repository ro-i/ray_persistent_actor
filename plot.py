#!/usr/bin/env python3

"""Adapted from the original benchmark.

Source: https://github.com/stephanie-wang/ownership-nsdi2021-artifact/\
blob/main/recovery-microbenchmark/plot.py
"""

import argparse
import csv
from collections import defaultdict, namedtuple
from typing import DefaultDict, Dict, List, Set, Tuple


Point = namedtuple("Point", ["system", "large", "delay_ms", "failure"])


def load(filename: str) -> Tuple[Dict[Point, float], List[int]]:
    keys: Set[int] = set()
    points: Dict[Point, float] = {}
    with open(filename, 'r') as file:
        reader: csv.DictReader[str] = csv.DictReader(file)
        for line in reader:
            if line["system"] == "system":
                continue
            delay_ms: int = int(line["delay_ms"])
            point: Point = Point(
                line["system"],
                line["large"] == "True",
                delay_ms, 
                line["failure"] == "True"
            )
            points[point] = float(line["duration"])
            keys.add(delay_ms)
    return points, sorted(keys)

if __name__ == '__main__':
    parser: argparse.ArgumentParser = argparse.ArgumentParser()
    parser.add_argument("--large", action="store_true")
    parser.add_argument("--filename", required=True)
    args: argparse.Namespace = parser.parse_args()

    print("Loading results from file", args.filename)
    points, keys = load(args.filename)

    DEFAULT: str = "default"
    SAFE_ACTOR: str = "safe_actor"
    systems: List[str] = [DEFAULT, SAFE_ACTOR]

    curves: DefaultDict[Tuple[str, bool], List[float]] = defaultdict(list)

    for key in keys:
        basepoint: Point = Point(system=SAFE_ACTOR, large=args.large, delay_ms=key, failure=False)
        baseline: float = points[basepoint]
        for system in systems:
            for failure in [True, False]:
                k: Point = Point(system=system, large=args.large, delay_ms=key, failure=failure)
                curves[(system, failure)].append(points[k] / baseline)

    for system in systems:
        for failure in [False, True]:
            print(system, "failure" if failure else "no failure")
            for key, pt in zip(keys, curves[(system, failure)]):
                print(key, pt)
