#!/usr/bin/env python3
import argparse, csv
from pathlib import Path

def run(inputs, output):
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    wrote_header = False
    with out.open("w", newline="", encoding="utf-8") as fout:
        for p in inputs:
            pp = Path(p)
            if not pp.exists() or pp.suffix.lower() != ".csv":
                continue
            with pp.open("r", newline="", encoding="utf-8") as fin:
                rows = list(csv.reader(fin))
                if not rows:
                    continue
                if writer is None:
                    writer = csv.writer(fout)
                header, data = rows[0], rows[1:]
                if not wrote_header:
                    writer.writerow(header)
                    wrote_header = True
                for r in data:
                    writer.writerow(r)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True)
    ap.add_argument("--inputs", nargs="+", required=True, help="one or more CSV paths")
    args = ap.parse_args()
    run(args.inputs, args.output)

if __name__ == "__main__":
    main()
