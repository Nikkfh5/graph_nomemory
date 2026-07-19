set -eu

binary=$1
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

input=$scratch/edges.csv
output=$scratch/pagerank.csv
configured_output=$scratch/pagerank-configured.csv
invalid_output=$scratch/invalid-output.csv
non_converged_output=$scratch/non-converged.csv

printf 'from,to\n1,2\n2,3\n3,1\n4,1\n' > "$input"

"$binary" --help > "$scratch/help.txt"
expected_help='Usage: tbank INPUT.csv OUTPUT.csv [CONFIG]

Configuration: https://github.com/Nikkfh5/graph_nomemory/blob/main/docs/configuration.md'
test "$(cat "$scratch/help.txt")" = "$expected_help"

"$binary" "$input" "$output" > "$scratch/stdout.txt" 2> "$scratch/stderr.txt"
test ! -s "$scratch/stdout.txt"
test ! -s "$scratch/stderr.txt"
test "$(sed -n '1p' "$output")" = 'vertex,rank'
test "$(wc -l < "$output")" -eq 5

printf 'schema=tbank-run-config-v1\npagerank.max_iterations=200\n' \
    > "$scratch/valid.conf"
"$binary" "$input" "$configured_output" "$scratch/valid.conf"
test "$(cat "$configured_output")" = "$(cat "$output")"

printf 'schema=tbank-run-config-v1\nunknown.key=1\n' > "$scratch/invalid.conf"
if "$binary" "$input" "$invalid_output" "$scratch/invalid.conf" \
    > "$scratch/invalid.stdout" 2> "$scratch/invalid.stderr"; then
    echo 'invalid config unexpectedly succeeded' >&2
    exit 1
else
    status=$?
fi
test "$status" -eq 2
test ! -e "$invalid_output"

printf 'schema=tbank-run-config-v1\npagerank.max_iterations=1\n' \
    > "$scratch/non-converged.conf"
if "$binary" "$input" "$non_converged_output" "$scratch/non-converged.conf" \
    > "$scratch/non-converged.stdout" 2> "$scratch/non-converged.stderr"; then
    echo 'one-iteration PageRank unexpectedly converged' >&2
    exit 1
else
    status=$?
fi
test "$status" -eq 5
test ! -e "$non_converged_output"

set -- "$scratch"/.tbank-run-*
test ! -e "$1"
