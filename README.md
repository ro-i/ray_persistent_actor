Prototype for Actor Fault Tolerance for Ray
===========================================

- `./demo.py`: a small demo for the basic functionality.  
  The state of the counter actor is saved across multiple invocations.  
  Run with: `python3 ./demo.py`
- `./persistent_actor.py`: the implementation of the prototype.  
  This would be part of the API.
- `./benchmark_persistent_actor.py`: the benchmark code.  
  Run with (e.g.):
  - `python3 ./benchmark_persistent_actor.py --delay-ms=10`  
    The default Ray system without object reconstruction.
  - `python3 ./benchmark_persistent_actor.py --delay-ms=10 --safe-state`  
    Our prototype for persistent actor state.
  - `python3 ./benchmark_persistent_actor.py --delay-ms=10 --large`  
    Large actor state without object reconstruction.
  - `python3 ./benchmark_persistent_actor.py --delay-ms=10 --large --safe-state`  
    Large actor state with our prototype for persistent actor state.
  Let actors die during execution:
  - `python3 ./benchmark_persistent_actor.py --delay-ms=10 --failure`
  - `python3 ./benchmark_persistent_actor.py --delay-ms=10 --failure --safe-state`
  - `python3 ./benchmark_persistent_actor.py --delay-ms=10 --large --failure`
  - `python3 ./benchmark_persistent_actor.py --delay-ms=10 --large --failure --safe-state`
- `./run-benchmark.sh`: run the benchmark with some configurations.  
  Run with: `bash ./run-benchmark.sh`

The help output of the benchmark script:
```
usage: benchmark_persistent_actor.py [-h] --delay-ms DELAY_MS [--failure] [--large] [--num-actors NUM_ACTORS] [--num-actors-kill NUM_ACTORS_KILL] [--num-rounds NUM_ROUNDS] [--output OUTPUT]
                                     [--safe-state] [--timeline TIMELINE]

optional arguments:
  -h, --help            show this help message and exit
  --delay-ms DELAY_MS   amount of milliseconds to delay actor method execution (default: None)
  --failure             kill actors during execution (default: False)
  --large               enable large actor state (default: False)
  --num-actors NUM_ACTORS
                        number of actors to run (default: 20)
  --num-actors-kill NUM_ACTORS_KILL
                        number of actors to kill when --failure is specified (default: 3)
  --num-rounds NUM_ROUNDS
                        number of rounds to run the code (default: 20)
  --output OUTPUT       filename for csv data (default: None)
  --safe-state          use prototype for persistent actor state (default: False)
  --timeline TIMELINE   output filename for timeline (default: None)
```
