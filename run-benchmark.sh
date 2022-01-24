#!/bin/bash

# Adapted from the original benchmark code:
# - https://github.com/stephanie-wang/ownership-nsdi2021-artifact/\
# blob/main/recovery-microbenchmark/run-benchmark.sh
# - https://github.com/stephanie-wang/ownership-nsdi2021-artifact/\
# blob/main/recovery-microbenchmark/run.sh

output="output-$(date).csv"

echo "Writing output to $output"

for safe_state in "" "--safe"; do
	for delay in 1000 500 100 50 10; do
		python3 ./benchmark_persistent_actor.py $safe_state --delay-ms $delay --output "$output"
		python3 ./benchmark_persistent_actor.py $safe_state --delay-ms $delay --failure --output "$output"
		python3 ./benchmark_persistent_actor.py $safe_state --delay-ms $delay --large --output "$output"
		python3 ./benchmark_persistent_actor.py $safe_state --delay-ms $delay --failure --large --output "$output"
	done
done
