#!/bin/bash
# 簡單版：執行 agent 並導出 log

CONFIG="input.yaml"   # 你的 YAML 設定檔
SCRIPT="flowtracer_agent.py"  # 你的 Python agent 主程式
LOGDIR="logs"

mkdir -p "$LOGDIR"
TS=$(date +"%Y%m%d-%H%M%S")
LOGFILE="$LOGDIR/agent_$TS.log"

echo "[INFO] Running agent at $TS"
echo "[INFO] Config = $CONFIG"
echo "[INFO] Log = $LOGFILE"

# 直接執行，stdout+stderr 都輸出到 log，也同時顯示在螢幕
python "$SCRIPT" --config "$CONFIG" --rule-stale-outputs 2>&1 | tee "$LOGFILE"

