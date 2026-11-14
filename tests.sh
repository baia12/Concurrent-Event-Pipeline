#!/usr/bin/env bash
# Reproduces every required test case (A-F, bad CLI option, >128-byte line).
# Run from the project root (where event_pipeline.c lives):  bash tests.sh
set -u
cc -std=c11 -O2 -Wall -Wextra -pthread event_pipeline.c -o event_pipeline || exit 1

W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
BIN=./event_pipeline
RES=tests_results.txt; : > "$RES"
FAILS=0
log(){ echo "$*" | tee -a "$RES"; }
check(){ # name, condition-result
  if [ "$2" = ok ]; then log "  [ok]   $1"; else log "  [FAIL] $1"; FAILS=$((FAILS+1)); fi
}

printf '1,INFO,5\n2,WARN,7\n3,ERROR,11\n4,INFO,-3\n5,ERROR,4\n' > "$W/hand.csv"
: > "$W/empty.csv"
printf '1,ERROR,5' > "$W/nolf.csv"
printf '1,INFO,5\n2,WARN,7\n3,DEBUG,9\n' > "$W/bad.csv"
python3 - "$W" <<'PY'
import sys, random
w = sys.argv[1]
random.seed(1)
with open(f"{w}/big.csv", "w", newline="\n") as f:
    for i in range(1, 20001):
        f.write(f"{i},{random.choice(['INFO','WARN','ERROR'])},{random.randint(-1000000,1000000)}\n")
open(f"{w}/long.csv", "w", newline="\n").write("1,INFO,5\n" + "x" * 5000 + "\n")
PY

# run_case NAME INPUT P Q B EXPECT_SUMMARY(optional)
run_ok(){
  local name=$1 in=$2 p=$3 q=$4 b=$5 want=${6:-}
  log "Case $name: input=$(basename "$in") P=$p Q=$q B=$b"
  local out="$W/par_$name.csv" ref="$W/ser_$name.csv"
  local line; line=$(timeout 60 $BIN --mode parallel --input "$in" --output "$out" \
                      --threads "$p" --queue "$q" --batch "$b" --verify 2>"$W/err"); local rc=$?
  log "  final line: $line"
  check "exit status 0" "$([ $rc -eq 0 ] && echo ok)"
  check "verify=PASS" "$(echo "$line" | grep -q 'verify=PASS' && echo ok)"
  timeout 60 $BIN --mode serial --input "$in" --output "$ref" >/dev/null 2>&1
  check "alerts byte-identical to serial" "$(cmp -s "$out" "$ref" && echo ok)"
  [ -n "$want" ] && check "expected summary" "$(echo "$line" | grep -q "$want" && echo ok)"
}

run_ok A "$W/hand.csv"  1 2 2 "records=5 info=2 warn=1 error=2 amount_sum=24 alerts=2"
printf 'id,amount\n3,11\n5,4\n' > "$W/expect_A.csv"
check "Case A alerts file exactly the 3-line example" "$(cmp -s "$W/par_A.csv" "$W/expect_A.csv" && echo ok)"
run_ok B "$W/hand.csv"  4 1 1 "records=5 info=2 warn=1 error=2 amount_sum=24 alerts=2"
run_ok C "$W/empty.csv" 4 1 1 "records=0"
check "Case C output is header only" "$([ "$(cat "$W/par_C.csv")" = "id,amount" ] && echo ok)"
run_ok D "$W/nolf.csv"  3 1 4 "records=1 info=0 warn=0 error=1 amount_sum=5 alerts=1"

log "Case E: valid prefix then 3,DEBUG,9; P=4 Q=1 B=1; existing output must stay unchanged"
printf 'OLD OUTPUT FILE\n' > "$W/e.csv"; cp "$W/e.csv" "$W/e.orig"
timeout 60 $BIN --mode parallel --input "$W/bad.csv" --output "$W/e.csv" --threads 4 --queue 1 --batch 1 --verify >"$W/o" 2>"$W/err"; rc=$?
log "  exit=$rc  stderr: $(tr '\n' ' ' < "$W/err")"
check "nonzero exit" "$([ $rc -ne 0 ] && [ $rc -ne 124 ] && echo ok)"
check "existing output unchanged" "$(cmp -s "$W/e.csv" "$W/e.orig" && echo ok)"
check "no temp file left" "$([ -z "$(ls "$W" | grep '\.tmp\.')" ] && echo ok)"

for i in 1 2 3 4 5; do run_ok "F$i" "$W/big.csv" 8 2 17; done

log "Invalid CLI option (--threads 0)"
timeout 20 $BIN --mode parallel --input "$W/hand.csv" --output "$W/x.csv" --threads 0 --queue 1 --batch 1 >/dev/null 2>"$W/err"; rc=$?
log "  exit=$rc stderr: $(tr '\n' ' ' < "$W/err")"
check "nonzero exit" "$([ $rc -ne 0 ] && [ $rc -ne 124 ] && echo ok)"

log "Line longer than 128 bytes (parallel and serial)"
for m in "parallel --threads 2 --queue 1 --batch 1" "serial"; do
  timeout 20 $BIN --mode $m --input "$W/long.csv" --output "$W/l.csv" >/dev/null 2>"$W/err"; rc=$?
  log "  mode=${m%% *} exit=$rc stderr: $(tr '\n' ' ' < "$W/err")"
  check "terminates with nonzero exit" "$([ $rc -ne 0 ] && [ $rc -ne 124 ] && echo ok)"
done

log ""; [ $FAILS -eq 0 ] && log "ALL TESTS PASSED" || log "$FAILS CHECK(S) FAILED"
exit $FAILS
