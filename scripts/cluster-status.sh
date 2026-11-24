#!/bin/bash

set -e

for node in 1 2 3; do
	port=$((8080 + node))

	echo "Node $node"
	echo "------"

	if ! curl -fsS \
		"http://127.0.0.1:$port/status"; then
		echo "UNREACHABLE"
	fi

	echo
done
