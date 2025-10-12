#!/usr/bin/env python3
from __future__ import annotations

import argparse
import difflib
import glob
import json
import os
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import subprocess, sys
import shlex

import yaml

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore

#### global variable 
BK_ROOT = Path("bk")
SMALL_LIMIT = 64 * 1024   # 64 KiB -> send full text
SNIP = 2048               # for big files, send head/tail (bytes), then decode UTF-8
DIFF_MAX_LINES = 200      # cap unified diff lines to keep tokens bounded


### define data class schema for the file pack
@dataclass
class DocPack:
    rel: str
    current_exists: bool
    previous_exists: bool
    current_bytes: int
    previous_bytes: int
    current_text_full: Optional[str]
    previous_text_full: Optional[str]
    current_head: Optional[str]
    current_tail: Optional[str]
    previous_head: Optional[str]
    previous_tail: Optional[str]
    text_diff: Optional[str]   # unified diff (first DIFF_MAX_LINES)
    current_mtime: Optional[float] = None
    previous_mtime: Optional[float] = None


#########  utilility function for the function

#### build command ro run the workflow
def build_runner_cmd(wf_spec: Dict[str, Any], input_paths: List[str], output_paths: List[str]) -> List[str]:
    wf_name = wf_spec.get("name", "wf")
    runner = (wf_spec.get("runner") or {})
    rtype = (runner.get("type") or "python").lower()
    inputs_str = " ".join(shlex.quote(p) for p in input_paths)
    output0 = shlex.quote(output_paths[0]) if output_paths else ""

    if rtype == "python":
        script = runner.get("script")
        if not script:
            raise ValueError(f"[{wf_name}] runner.type=python missing script")
        #### return run script command ex: python script.py --output out.csv --inputs in1.csv in2.csv...
        return [sys.executable, script, "--output", output_paths[0], "--inputs", *input_paths]

    if rtype == "shell":
        cmd_tpl = runner.get("command")
        if not cmd_tpl:
            raise ValueError(f"[{wf_name}] runner.type=shell missing command")
        cmd_str = cmd_tpl.format(output0=output0, inputs=inputs_str)
        #### return bash -lc command ex: bash -lc "some shell command with {inputs} and {output0} replaced"
        return ["bash", "-lc", cmd_str]

    raise ValueError(f"[{wf_name}] Unsupported runner.type: {rtype}")

#### backup input files to bk folder
def bk_dir_for(workflow_name: str) -> Path:
    return BK_ROOT / workflow_name


def is_probably_text(b: bytes) -> bool:
    try:
        b.decode("utf-8")
        return True
    except Exception:
        return False

#### get file timestamp
def _file_mtime(path: Path) -> Optional[float]:
    try:
        st = path.stat()
        return st.st_mtime
    except FileNotFoundError:
        return None

#### return the lastest timestamp of input files
def _max_input_mtime(input_rels: List[str]) -> Optional[float]:
    mtimes = []
    for rel in input_rels:
        t = _file_mtime(Path(rel))
        if t is not None:
            mtimes.append(t)
    return max(mtimes) if mtimes else None

#### return the lastest timestamp of output files
def _min_output_mtime(output_paths: List[str]) -> Optional[float]:
    mtimes = []
    for p in output_paths:
        t = _file_mtime(Path(p))
        if t is not None:
            mtimes.append(t)
    return min(mtimes) if mtimes else None


def fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts).isoformat(timespec="seconds")


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

#### get inputs from config file
def expand_inputs(items: List[Dict[str, str]]) -> List[str]:
    paths: List[str] = []
    for it in items or []:
        if "path" in it:
            paths.append(it["path"])
        elif "glob" in it:
            paths.extend(sorted(glob.glob(it["glob"], recursive=True)))
        else:
            raise ValueError(f"Unsupported input item: {it}")
    out: List[str] = []
    cwd = Path.cwd()
    seen = set()
    for p in paths:
        rel = str((cwd / p).resolve().relative_to(cwd))
        if rel not in seen:
            out.append(rel)
            seen.add(rel)
    return out

#### attach timestamp info to the per_file dict
def _attach_timestamp_info(per_file: List[Dict[str, Any]], packs: List[DocPack]) -> List[Dict[str, Any]]:
    pack_by_rel = {p.rel: p for p in packs}
    out: List[Dict[str, Any]] = []
    for item in per_file:
        rel = item.get("rel")
        p = pack_by_rel.get(rel)
        if p is None:
            out.append(item)
            continue
        ts_changed = (p.current_mtime != p.previous_mtime)
        item = dict(item)  # copy
        item["timestamp_changed"] = bool(ts_changed)
        item["current_mtime"] = fmt_ts(p.current_mtime) if p.current_mtime is not None else None
        item["previous_mtime"] = fmt_ts(p.previous_mtime) if p.previous_mtime is not None else None
        out.append(item)
    return out


#### read current and previous version of a file, prepare DocPack
def read_pack(rel: str, workflow_name: str) -> DocPack:
    cur = Path(rel)
    prv = bk_dir_for(workflow_name) / rel

    cur_b = cur.read_bytes() if cur.exists() and cur.is_file() else b""
    prv_b = prv.read_bytes() if prv.exists() and prv.is_file() else b""

    def mk_text(b: bytes) -> (Optional[str], Optional[str], Optional[str]):
        if not b:
            return None, None, None
        if len(b) <= SMALL_LIMIT and is_probably_text(b):
            return b.decode("utf-8", errors="replace"), None, None
        if is_probably_text(b):
            head = b[:SNIP].decode("utf-8", errors="replace")
            tail = b[-SNIP:].decode("utf-8", errors="replace") if len(b) > SNIP else ""
            return None, head, tail
        return None, None, None

    cur_full, cur_head, cur_tail = mk_text(cur_b)
    prv_full, prv_head, prv_tail = mk_text(prv_b)

    text_diff = None

    def combine_head_tail(full: Optional[str], head: Optional[str], tail: Optional[str]) -> str:
        if full is not None:
            return full
        parts: List[str] = []
        if head:
            parts += ["[HEAD]", head]
        if tail:
            parts += ["[TAIL]", tail]
        return "\n".join(parts)

    cur_for_diff = combine_head_tail(cur_full, cur_head, cur_tail)
    prv_for_diff = combine_head_tail(prv_full, prv_head, prv_tail)
    if cur_for_diff or prv_for_diff:
        cur_lines = cur_for_diff.splitlines(keepends=False)
        prv_lines = prv_for_diff.splitlines(keepends=False)
        diff_lines = list(
            difflib.unified_diff(
                prv_lines, cur_lines,
                fromfile=f"previous/{rel}", tofile=f"current/{rel}", lineterm=""
            )
        )
        if diff_lines:
            if len(diff_lines) > DIFF_MAX_LINES:
                diff_lines = diff_lines[:DIFF_MAX_LINES] + ["... (diff truncated)"]
            text_diff = "\n".join(diff_lines)

    return DocPack(
        rel=rel,
        current_exists=cur.exists() and cur.is_file(),
        previous_exists=prv.exists() and prv.is_file(),
        current_bytes=len(cur_b),
        previous_bytes=len(prv_b),
        current_text_full=cur_full,
        previous_text_full=prv_full,
        current_head=cur_head,
        current_tail=cur_tail,
        previous_head=prv_head,
        previous_tail=prv_tail,
        text_diff=text_diff,
        current_mtime=_file_mtime(cur),
        previous_mtime=_file_mtime(prv),
    )

#### read imformation form the DocPack and render to text block for LLM input
def render_block(p: DocPack, wf_name: str) -> str:
    lines = [f"## [{wf_name}] File: {p.rel}"]
    lines.append(f"current_exists: {p.current_exists}, previous_exists: {p.previous_exists}")
    lines.append(f"current_bytes: {p.current_bytes}, previous_bytes: {p.previous_bytes}")
    # Current
    if p.current_text_full is not None:
        lines.append("Current version (full):\n" + p.current_text_full)
    elif p.current_head is not None:
        lines.append("Current version (head):\n" + p.current_head)
        if p.current_tail:
            lines.append("Current version (tail):\n" + p.current_tail)
    else:
        lines.append("Current version: <non-text or empty>")
    # Previous
    if p.previous_text_full is not None:
        lines.append("Previous version (full):\n" + p.previous_text_full)
    elif p.previous_head is not None:
        lines.append("Previous version (head):\n" + p.previous_head)
        if p.previous_tail:
            lines.append("Previous version (tail):\n" + p.previous_tail)
    else:
        lines.append("Previous version: <non-text or empty>")
    # Diff
    if p.text_diff:
        lines.append("Heuristic unified diff:\n" + p.text_diff)
    return "\n".join(lines)


#### call LLM Chat completion API 
def _llm_once(client, system_prompt: str, user_text: str) -> Dict[str, Any]:
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_text}],
        temperature=0,
    )
    content = resp.choices[0].message.content.strip()
    if content.startswith("```"):
        content = content.strip("`")
        content = content.split("\n", 1)[1] if "\n" in content else content
    return json.loads(content)


def _approx_tokens(s: str) -> int:
    return max(1, len(s) // 4)


def _chunk_blocks(blocks: List[str], max_tokens: int) -> List[List[str]]:
    batches: List[List[str]] = []
    cur: List[str] = []
    cur_tokens = 0
    for b in blocks:
        t = _approx_tokens(b)
        if cur and cur_tokens + t > max_tokens:
            batches.append(cur)
            cur, cur_tokens = [], 0
        cur.append(b)
        cur_tokens += t
    if cur:
        batches.append(cur)
    return batches


#### call LLM per file (one call per file)
def call_llm_per_file(yaml_text: str, packs: List[DocPack], wf_name: str) -> Dict[str, Any]:
    if OpenAI is None:
        per_file: List[Dict[str, Any]] = []
        rerun = False
        for p in packs:
            changed = (p.current_bytes != p.previous_bytes) or (p.current_exists != p.previous_exists)
            rerun = rerun or changed
            per_file.append({
                "rel": p.rel,
                "semantic_changed": bool(changed),
                "why": "fallback: bytes/exists differ" if changed else "fallback: no evidence"
            })
        per_file = _attach_timestamp_info(per_file, packs)
        reason = "fallback heuristic indicates changes" if rerun else "LLM unavailable; conservative fallback"
        return {"rerun": rerun, "reason": reason, "per_file": per_file}

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    SYSTEM_ONE = """You are a build orchestrator. Given CURRENT and PREVIOUS for a SINGLE file,
decide if there is a SEMANTIC/MATERIAL change that could affect downstream results.
Treat a NEW file (previous_exists=false) or a DELETED file (current_exists=false) as a semantic change.
Return ONLY JSON: {\"rel\": string, \"semantic_changed\": bool, \"why\": string}."""

    USER_ONE_TMPL = """YAML (verbatim):
---
{yaml_text}
---

Review this file only.

{block}

Decide and return compact JSON."""

    per_file_results: List[Dict[str, Any]] = []
    for p in packs:
        block = render_block(p, wf_name)
        user_text = USER_ONE_TMPL.format(yaml_text=yaml_text, block=block)
        out = _llm_once(client, SYSTEM_ONE, user_text)
        if "rel" not in out:
            out["rel"] = p.rel
        per_file_results.append(out)

    changed_files = [x.get("rel") for x in per_file_results if x.get("semantic_changed")]
    rerun = bool(changed_files)
    reason = "semantic changes in: " + ", ".join(changed_files) if changed_files else "no semantic change across inputs"

    per_file_with_ts = _attach_timestamp_info(per_file_results, packs)
    return {"rerun": rerun, "reason": reason, "per_file": per_file_with_ts}

#### call LLM batched (chunked map-reduce)
def call_llm_batched(yaml_text: str, packs: List[DocPack], wf_name: str) -> Dict[str, Any]:
    if OpenAI is None:
        per_file: List[Dict[str, Any]] = []
        rerun = False
        for p in packs:
            changed = (p.current_bytes != p.previous_bytes) or (p.current_exists != p.previous_exists)
            rerun = rerun or changed
            per_file.append({
                "rel": p.rel,
                "semantic_changed": bool(changed),
                "why": "fallback: bytes/exists differ" if changed else "fallback: no evidence"
            })
        per_file = _attach_timestamp_info(per_file, packs)
        reason = "fallback heuristic indicates changes" if rerun else "LLM unavailable; conservative fallback"
        return {"rerun": rerun, "reason": reason, "per_file": per_file}

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    blocks_by_file = [render_block(p, wf_name) for p in packs]
    MAX_INPUT_TOKENS = 100_000
    batches = _chunk_blocks(blocks_by_file, MAX_INPUT_TOKENS)

    SYSTEM_PER_BATCH = """You are a build orchestrator. For the provided subset of files, decide per-file whether there is a SEMANTIC change.
Return ONLY JSON: {\"per_file\": [{\"rel\": string, \"semantic_changed\": bool, \"why\": string}]}
No extra text.
"""
    USER_TMPL_PER_BATCH = """YAML (verbatim):
---
{yaml_text}
---

FILES (subset):

{blocks}

Assess each file independently and return JSON for this subset only.
"""

    combined_per_file: List[Dict[str, Any]] = []
    for subset in batches:
        user_text = USER_TMPL_PER_BATCH.format(yaml_text=yaml_text, blocks="\n\n".join(subset))
        out = _llm_once(client, SYSTEM_PER_BATCH, user_text)
        pf = out.get("per_file", [])
        if not isinstance(pf, list):
            pf = []
        combined_per_file.extend(pf)

    changed_files = [x.get("rel") for x in combined_per_file if x.get("semantic_changed")]
    rerun = bool(changed_files)
    reason = "semantic changes in: " + ", ".join(changed_files) if changed_files else "no semantic change across inputs"
    per_file_with_ts = _attach_timestamp_info(combined_per_file, packs)
    return {"rerun": rerun, "reason": reason, "per_file": per_file_with_ts}


def call_llm(yaml_text: str, packs: List[DocPack], wf_name: str, mode: str = "per-file") -> Dict[str, Any]:
    if mode == "batched":
        return call_llm_batched(yaml_text, packs, wf_name)
    return call_llm_per_file(yaml_text, packs, wf_name)


#### backup current input files to bk folder as backup files
def mirror_current_to_bk(packs: List[DocPack], workflow_name: str) -> None:
    base = bk_dir_for(workflow_name)
    for p in packs:
        src = Path(p.rel)
        dst = base / p.rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.exists() and src.is_file():
            dst.write_bytes(src.read_bytes())


#### define main funciton
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Semantic LLM rerun decider (+ timestamp rule, multi-workflow, no hash)")
    ap.add_argument("--config", required=True, help="YAML config path")
    ap.add_argument("--dry-run", action="store_true", help="Only print decision; do not execute or refresh bk/ unless --refresh")
    ap.add_argument("--force", action="store_true", help="Force rerun regardless of LLM or rules")
    ap.add_argument("--refresh", action="store_true", help="Refresh bk/ even if no rerun")
    ap.add_argument("--llm-mode", choices=["per-file", "batched"], default="per-file", help="Use one-call-per-file (default) or chunked batched map-reduce")
    ap.add_argument("--rule-stale-outputs", action="store_true", help="If outputs are older than inputs, force rerun (per workflow)")
    args = ap.parse_args(argv)

    ypath = Path(args.config)
    raw_yaml = ypath.read_text(encoding="utf-8")
    yd = load_yaml(ypath)

    # Detect workflows spec (backward compatible)
    if "workflows" in yd and yd["workflows"]:
        wf_specs: List[Dict[str, Any]] = yd["workflows"]
    else:
        wf_specs = [{
            "name": yd.get("name", "default"),
            "inputs": yd.get("inputs", []),
            "outputs": yd.get("outputs", []),
        }]

    all_decisions: List[Dict[str, Any]] = []
    rerun_any = False

    for wf in wf_specs:
        wf_name = wf.get("name", "wf")
        input_paths = expand_inputs(wf.get("inputs", []))
        output_paths = [o.get("path") for o in (wf.get("outputs") or []) if "path" in o]

        packs = [read_pack(rel, wf_name) for rel in input_paths]

        #### check timestamp rule (per workflow)
        hard_reasons: List[str] = []
        if args.rule_stale_outputs and output_paths:
            mx_in = _max_input_mtime(input_paths)
            mn_out = _min_output_mtime(output_paths)
            if mx_in is not None and mn_out is not None and mn_out < mx_in:
                hard_reasons.append(
                    f"outputs stale: min(output mtime)={fmt_ts(mn_out)} < max(input mtime)={fmt_ts(mx_in)}"
                )

        
        llm_decision = call_llm(raw_yaml, packs, wf_name, mode=args.llm_mode)
        final_rerun = bool(args.force) or bool(hard_reasons) or bool(llm_decision.get("rerun"))
        reasons: List[str] = []
        if args.force:
            reasons.append("forced by --force")
        if hard_reasons:
            reasons.append(" / ".join(hard_reasons))
        if llm_decision.get("reason"):
            reasons.append(f"llm: {llm_decision['reason']}")

        decision = {
            "workflow": wf_name,
            "rerun": final_rerun,
            "reason": " | ".join(reasons) if reasons else "",
            "per_file": llm_decision.get("per_file", []),
        }

        print(f"\n=== [{wf_name}] LLM SEMANTIC DECISION (combined) ===")
        print(json.dumps(decision, indent=2, ensure_ascii=False))

        if final_rerun and not args.dry_run:
            print(f"[run][{wf_name}] pipeline would run here.")
            if output_paths:
                cmd = build_runner_cmd(wf, input_paths, output_paths) 
                print(f"[run][{wf_name}] exec: {' '.join(shlex.quote(c) for c in cmd)}")
                subprocess.run(cmd, check=True)
            mirror_current_to_bk(packs, wf_name)
            print(f"[bk][{wf_name}] updated from current inputs.")
        else:
            print(f"[skip][{wf_name}] No rerun.")
            if args.refresh:
                mirror_current_to_bk(packs, wf_name)
                print(f"[bk][{wf_name}] refreshed (--refresh).")

        missing = [p.rel for p in packs if not p.current_exists]
        if missing:
            print(f"[warn][{wf_name}] Missing current inputs: {missing}")

        all_decisions.append(decision)
        rerun_any = rerun_any or final_rerun

    # Summary across workflows
    summary = {"rerun_any": bool(rerun_any), "workflows": all_decisions}
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


#### improvment ideas
##1. LLM output format validation(json format)
