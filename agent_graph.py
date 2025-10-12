# flow_agent_graph.py
from __future__ import annotations
import argparse
import json
import shlex
import subprocess
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict
from langchain.callbacks.tracers import LangChainTracer
from langsmith import traceable, trace
from langgraph.graph import StateGraph, END
import logging, sys
from logging.handlers import TimedRotatingFileHandler
from flowtracer_agent import (
    DocPack, load_yaml, expand_inputs, read_pack,
    call_llm, build_runner_cmd, mirror_current_to_bk,
    _max_input_mtime, _min_output_mtime, fmt_ts,
)

#### define global variables
GLOBAL_LOG_DIR = "graph_log"
GLOBAL_LOG_FILE = "flow_all.log"   

def _ensure_dir(d: str | Path) -> Path:
    p = Path(d)
    p.mkdir(parents=True, exist_ok=True)
    return p

def init_global_logger() -> logging.Logger:
    _ensure_dir(GLOBAL_LOG_DIR)
    logger = logging.getLogger("flow.ALL")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(ch)

    fh = TimedRotatingFileHandler(
        filename=str(Path(GLOBAL_LOG_DIR) / GLOBAL_LOG_FILE),
        when="midnight", interval=1, backupCount=14, encoding="utf-8", utc=False
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(fh)

    logger.info("[logger] global logger initialized → %s", Path(GLOBAL_LOG_DIR) / GLOBAL_LOG_FILE)
    return logger


def glog(state: "WFState", msg: str, level: int = logging.INFO):
    lg: Optional[logging.Logger] = state.get("global_logger")  
    wf_name = state.get("wf_name", "wf")
    attempt = state.get("attempt", 0) or 0
    prefix = f"[{wf_name}]"
    text = f"{prefix} {msg}"
    if lg is None:
        print(text)
    else:
        lg.log(level, text)

#### define graph state
class WFState(TypedDict, total=False):
    config_path: str
    raw_yaml_text: str
    yaml_dict: Dict[str, Any]
    wf_spec: Dict[str, Any]
    wf_name: str
    input_paths: List[str]
    output_paths: List[str]
    packs: List[DocPack]
    llm_decision: Dict[str, Any]
    timestamp_misaligned: bool
    final_rerun: bool
    reason: str
    attempt: int
    max_attempts: int
    force: bool
    rule_stale_outputs: bool
    llm_mode: str
    dry_run: bool
    refresh: bool
    validated: bool
    global_logger: logging.Logger

#### define each nodes for graph
@traceable(name="prepare_io")
def n_prepare_io(state: WFState) -> WFState:
    wf = state["wf_spec"]
    wf_name = wf.get("name", "wf")
    state["wf_name"] = wf_name
    input_paths = expand_inputs(wf.get("inputs", []))
    output_paths = [o.get("path") for o in (wf.get("outputs") or []) if "path" in o]
    packs = [read_pack(rel, wf_name) for rel in input_paths]
    state.update(input_paths=input_paths, output_paths=output_paths, packs=packs)
    glog(state, f"[prepare_io] inputs={len(input_paths)} outputs={len(output_paths)}")
    return state

@traceable(name="decide_semantic")
def n_decide_semantic(state: WFState) -> WFState:
    wf_name = state["wf_name"]
    mode = state.get("llm_mode", "per-file")
    dec = call_llm(state["raw_yaml_text"], state["packs"], wf_name, mode=mode)
    state["llm_decision"] = dec
    glog(state, f"[decide_semantic] llm.rerun={dec.get('rerun')} reason={dec.get('reason')}")
    return state

@traceable(name="rule_timestamp")
def n_rule_timestamp(state: WFState) -> WFState:
    wf_name = state["wf_name"]
    ins, outs = state["input_paths"], state["output_paths"]
    mis = False
    if state.get("rule_stale_outputs", False) and outs:
        mx_in = _max_input_mtime(ins)
        mn_out = _min_output_mtime(outs)
        mis = (mx_in is not None and mn_out is not None and mn_out < mx_in)
        if mis:
            glog(state, f"[rule_ts] outputs stale: min(out)={fmt_ts(mn_out)} < max(in)={fmt_ts(mx_in)}")
    state["timestamp_misaligned"] = mis
    return state

@traceable(name="combine")
def n_combine(state: WFState) -> WFState:
    force = state.get("force", False)
    llm_rerun = bool(state["llm_decision"].get("rerun"))
    mis = state.get("timestamp_misaligned", False)
    final = bool(force or llm_rerun or mis)
    parts = []
    if force: parts.append("forced")
    if mis: parts.append("outputs stale vs inputs")
    if llm_rerun: parts.append("llm: " + state["llm_decision"].get("reason", ""))
    state.update(final_rerun=final, reason=" | ".join(parts))
    glog(state, f"[combine] final_rerun={final} reason={state['reason']}")
    return state

@traceable(name="run_workflow")
def n_run_workflow(state: WFState) -> WFState:
    if not state["final_rerun"]:
        glog(state, "[run_workflow] skipped (no rerun)")
        return state
    if state.get("dry_run", False):
        glog(state, "[run_workflow] --dry-run")
        return state

    wf = state["wf_spec"]
    cmd = build_runner_cmd(wf, state["input_paths"], state["output_paths"])
    pretty = ' '.join(shlex.quote(c) for c in cmd)
    glog(state, f"[run_workflow] exec: {pretty}")

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as p:
        assert p.stdout
        for line in p.stdout:
            glog(state, line.rstrip("\n"), level=logging.DEBUG)
        rc = p.wait()
        if rc != 0:
            glog(state, f"[run_workflow] exit code={rc}", level=logging.ERROR)
            raise subprocess.CalledProcessError(rc, cmd)
    return state

@traceable(name="mirror_bk")
def n_mirror_bk(state: WFState) -> WFState:
    if state.get("dry_run", False):
        glog(state, "[bk] --dry-run (skip mirror)")
        return state
    mirror_current_to_bk(state["packs"], state["wf_name"])
    glog(state, "[bk] updated")
    return state

@traceable(name="validate")
def n_validate(state: WFState) -> WFState:
    ins, outs = state["input_paths"], state["output_paths"]
    ok = True
    if outs:
        mx_in = _max_input_mtime(ins)
        mn_out = _min_output_mtime(outs)
        ok = not (mx_in is not None and mn_out is not None and mn_out < mx_in)
    if state.get("final_rerun", False):
        ok = False
    state["validated"] = ok
    glog(state, f"[validate] validated={ok}")
    return state

# ---------- graph ----------
def build_graph_raw():
    g = StateGraph(WFState)
    g.add_node("prepare_io", n_prepare_io)
    g.add_node("decide_semantic", n_decide_semantic)
    g.add_node("rule_timestamp", n_rule_timestamp)
    g.add_node("combine", n_combine)
    g.add_node("run_workflow", n_run_workflow)
    g.add_node("mirror_bk", n_mirror_bk)
    g.add_node("validate", n_validate)

    g.set_entry_point("prepare_io")
    g.add_edge("prepare_io", "decide_semantic")
    g.add_edge("decide_semantic", "rule_timestamp")
    g.add_edge("rule_timestamp", "combine")
    g.add_edge("combine", "run_workflow")
    g.add_edge("run_workflow", "mirror_bk")
    g.add_edge("mirror_bk", "validate")

    def should_loop(state: WFState) -> str:
        attempt = state.get("attempt", 0) + 1
        state["attempt"] = attempt
        max_attempts = state.get("max_attempts", 3)
        if state.get("validated", False):
            return END
        if attempt >= max_attempts:
            glog(state, f"[loop] reach max_attempts={max_attempts}, stop.")
            return END
        glog(state, f"[loop] not validated, retry attempt={attempt+1}")
        return "prepare_io"

    g.add_conditional_edges("validate", should_loop, {"prepare_io": "prepare_io", END: END})
    return g

def build_graph():
    return build_graph_raw().compile()

#### draw flowchart export
def export_flowcharts(out_base: str = "graph"):
    raw = build_graph_raw()      

    app = raw.compile()
    try:
        viz = app.get_graph()          
    except AttributeError:
        try:
            viz = raw.get_graph()
        except AttributeError as e:
            raise RuntimeError(
                "Your LangGraph version does not support calling get_graph() on StateGraph; "
                "please upgrade (`pip install -U langgraph`), or use compiled.get_graph() instead."
            ) from e

    mermaid = viz.draw_mermaid()
    with open(f"{out_base}.mmd", "w", encoding="utf-8") as f:
        f.write(mermaid)

    try:
        viz.draw_png(f"{out_base}.png")
        viz.draw_svg(f"{out_base}.svg")
    except Exception as e:
        print(f"[warn] Unable to output PNG/SVG (Graphviz may not be installed or PATH not set): {e}")



def run_all(config_path: str, force: bool, rule_stale_outputs: bool,
            llm_mode: str, max_attempts: int, dry_run: bool, refresh: bool) -> int:
    global_logger = init_global_logger()

    ypath = Path(config_path)
    raw_yaml = ypath.read_text(encoding="utf-8")
    yd = load_yaml(ypath)

    if "workflows" in yd and yd["workflows"]:
        wf_specs: List[Dict[str, Any]] = yd["workflows"]
    else:
        wf_specs = [{
            "name": yd.get("name", "default"),
            "inputs": yd.get("inputs", []),
            "outputs": yd.get("outputs", []),
            "runner": yd.get("runner"),
        }]

    compiled = build_graph()
    tracer = LangChainTracer(project_name=os.environ.get("LANGCHAIN_PROJECT", "flowtracer"))
    with trace("multi_demo", metadata={"workflows": [wf.get("name", "wf") for wf in wf_specs]}):
        any_rerun = False
        reports: List[Dict[str, Any]] = []

        for wf in wf_specs:
            wf_name = wf.get("name", "wf")
            global_logger.info(f"[{wf_name}] ========= WORKFLOW: {wf_name} =========")

            init_state: WFState = {
                "config_path": config_path,
                "raw_yaml_text": raw_yaml,
                "yaml_dict": yd,
                "wf_spec": wf,
                "attempt": 0,
                "max_attempts": max_attempts,
                "force": force,
                "rule_stale_outputs": rule_stale_outputs,
                "llm_mode": llm_mode,
                "dry_run": dry_run,
                "refresh": refresh,
                "global_logger": global_logger,  
            }
            #final_state: WFState = compiled.invoke(init_state)
            final_state: WFState = compiled.invoke(
                init_state,
                config={"callbacks": [tracer], "run_name": wf_name, "tags": ["agent_graph"]}
            )
            report = {
                "workflow": wf_name,
                "attempts": final_state.get("attempt", 0),
                "final_rerun": final_state.get("final_rerun", False),
                "reason": final_state.get("reason", ""),
                "validated": final_state.get("validated", False),
                "llm_decision": final_state.get("llm_decision", {}),
            }
            reports.append(report)
            any_rerun = any_rerun or bool(report["final_rerun"])

            if refresh and not final_state.get("final_rerun", False):
                from flowtracer_agent import mirror_current_to_bk 
                packs = final_state.get("packs", [])
                mirror_current_to_bk(packs, wf_name)
                global_logger.info(f"[{wf_name}] [bk] refreshed (--refresh)")

    summary = {"rerun_any": bool(any_rerun), "workflows": reports}
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="LangGraph agentic workflow (importing shared core)")
    ap.add_argument("--config", required=False)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--rule-stale-outputs", action="store_true")
    ap.add_argument("--llm-mode", choices=["per-file", "batched"], default="per-file")
    ap.add_argument("--max-attempts", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--draw-graph", action="store_true",help="Only output the flowchart (Mermaid/PNG/SVG) and then exit")
    ap.add_argument("--graph-out", default="graph",help="Prefix for flowchart output filename (default: graph)")

    args = ap.parse_args(argv)

    if args.draw_graph:
        export_flowcharts(args.graph_out)
        print(f"Saved: {args.graph_out}.mmd (+ .png/.svg if Graphviz is available)")
        return 0
    if not args.config:
        ap.error("--config is a required argument (unless using --draw-graph)")

    return run_all(
        config_path=args.config,
        force=args.force,
        rule_stale_outputs=args.rule_stale_outputs,
        llm_mode=args.llm_mode,
        max_attempts=args.max_attempts,
        dry_run=args.dry_run,
        refresh=args.refresh,
    )

if __name__ == "__main__":
    raise SystemExit(main())
