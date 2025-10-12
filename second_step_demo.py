#!/usr/bin/env python3
import argparse, csv, shutil
from pathlib import Path

def run(inputs, output):
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)

    csv_inputs = [Path(p) for p in inputs if Path(p).exists() and Path(p).suffix.lower() == ".csv"]
    if not csv_inputs:
        out.write_text("", encoding="utf-8")
        return

    if len(csv_inputs) == 1:
        shutil.copyfile(csv_inputs[0], out)
        return

    writer = None
    wrote_header = False
    with out.open("w", newline="", encoding="utf-8") as fout:
        for pp in csv_inputs:
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
    ap.add_argument("--inputs", nargs="+", required=True)
    args = ap.parse_args()
    run(args.inputs, args.output)

if __name__ == "__main__":
    main()
