#!/usr/bin/env bash

#### get input file
INPUT_FILE="${1:-input_file}"
[[ -f "$INPUT_FILE" ]] || { echo "❌ file not found: $INPUT_FILE"; exit 1; }

PROMPT_CONTENT="$(cat "$INPUT_FILE")"

#### define workflow policy
POLICY="$(cat <<'EOF'
System instruction:
- You are a **Flow Validation Agent** inside a build orchestration system.
- Check files dependencies in input.yaml and determine if the workflow should be rerun.
  1. If the inputs are newer than the outputs, the flow should be rerun.
  2. If the semantic of the inputs is different from files in bk folder, the flow should be rerun.
  3. If the bk folder is not created, create it and copy all current input files into it.
  4. Copy all current input files into the bk folder to serve as the new baseline for future runs.
- In fresh run and rerun, each workflow should be run once.
- If the output folder is not created, create it as results folder. 
- Put all the workflow outputs into the results folder.
- Output the result as result.rpt to explain the reason why the workflow should be rerun or not.
- Never modify the input.yaml file and input files in input_data folder.
- Never modify the workflow script.  
- Ensure all output files are correctly generated and placed in the results folder.
EOF
)"


FULL_MSG="${POLICY}

${PROMPT_CONTENT}"

#### clean cline instance and start new one
echo "==> Cleaning up"
cline instance kill -a || true

#### cline execution
echo "==> Starting instance"
cline -o "$FULL_MSG"

