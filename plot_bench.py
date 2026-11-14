#!/usr/bin/env python3
"""Reads bench_raw.csv -> bench_table.md, bench_table.csv, bench_graph.png"""
import csv, statistics
rows = list(csv.DictReader(open("bench_raw.csv")))
by = {}
for r in rows:
    by.setdefault(int(r["P"]), []).append(float(r["seconds"]))
ps = sorted(by)
med = {p: statistics.median(by[p]) for p in ps}
base = med[ps[0]]
with open("bench_table.md", "w") as f, open("bench_table.csv", "w") as c:
    f.write("| Workers P | Run 1 (s) | Run 2 (s) | Run 3 (s) | Median (s) | Speedup vs P=1 |\n|---|---|---|---|---|---|\n")
    c.write("P,median_seconds,speedup\n")
    for p in ps:
        v = by[p] + [float("nan")] * (3 - len(by[p]))
        f.write(f"| {p} | {v[0]:.4f} | {v[1]:.4f} | {v[2]:.4f} | {med[p]:.4f} | {base/med[p]:.2f}x |\n")
        c.write(f"{p},{med[p]:.6f},{base/med[p]:.4f}\n")
print(open("bench_table.md").read())
try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(1, 2, figsize=(9, 3.4))
    ax[0].plot(ps, [med[p] for p in ps], "o-"); ax[0].set_xlabel("Workers P"); ax[0].set_ylabel("Median wall time (s)")
    ax[0].set_title("Runtime vs workers"); ax[0].set_xticks(ps); ax[0].set_ylim(bottom=0); ax[0].grid(alpha=.3)
    ax[1].plot(ps, [base / med[p] for p in ps], "o-", label="measured")
    ax[1].plot(ps, [p / ps[0] for p in ps], "--", color="gray", label="ideal")
    ax[1].set_xlabel("Workers P"); ax[1].set_ylabel("Speedup vs P=1"); ax[1].set_title("Speedup")
    ax[1].set_xticks(ps); ax[1].legend(); ax[1].grid(alpha=.3)
    fig.tight_layout(); fig.savefig("bench_graph.png", dpi=160)
    print("wrote bench_graph.png")
except ImportError:
    print("matplotlib missing: pip install matplotlib  (then rerun: python3 plot_bench.py)")
