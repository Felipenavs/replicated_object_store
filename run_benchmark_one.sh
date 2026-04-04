#!/usr/bin/env bash
set -euo pipefail

#############################################
# CONFIG
#############################################
ILAB_USER="${ILAB_USER:-*** INSERT NETID ***}"   # <========= INSERT YOUR NETID HERE AND REMOVE THE ASTERISKS
ILAB_PASS="${ILAB_PASS:-}"

SERVER1="${SERVER1:-pwd.cs.rutgers.edu}"
CLIENT="${CLIENT:-cp.cs.rutgers.edu}"

REMOTE_DIR="${REMOTE_DIR:-/common/home/${ILAB_USER}/project2}"
PORT="${PORT:-50055}"
DURATION="${DURATION:-30}"
GET_KEY_COUNT="${GET_KEY_COUNT:-50000}"

STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-30}"
BETWEEN_RUN_DELAY="${BETWEEN_RUN_DELAY:-5}"

TS="$(date +%Y%m%d_%H%M%S)"
LOCAL_PULL_DIR="${LOCAL_PULL_DIR:-./ilab_results_benchmark1_${TS}}"

#############################################
# SSH / RSYNC OPTIONS
#############################################
SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
  -o PreferredAuthentications=password
  -o PasswordAuthentication=yes
  -o PubkeyAuthentication=no
)

#############################################
# HELPERS
#############################################
prompt_pass() {
  if [[ -z "$ILAB_PASS" ]]; then
    read -r -s -p "Enter iLab password: " ILAB_PASS
    echo
  fi
  export SSHPASS="$ILAB_PASS"
}

ssh_cmd() {
  local host="$1"
  shift
  sshpass -e ssh "${SSH_OPTS[@]}" "${ILAB_USER}@${host}" "$@"
}

ssh_block() {
  local host="$1"
  local script="$2"
  sshpass -e ssh "${SSH_OPTS[@]}" "${ILAB_USER}@${host}" "bash -s" <<EOF
set -euo pipefail
$script
EOF
}

ensure_remote_dir() {
  local host="$1"
  ssh_block "$host" "mkdir -p '${REMOTE_DIR}'"
}

sync_all() {
  for host in "$SERVER1" "$CLIENT"; do
    echo "==> Syncing project to $host"
    ensure_remote_dir "$host"
    sshpass -e rsync -az --delete \
      --exclude 'venv' \
      --exclude '__pycache__' \
      --exclude '.pytest_cache' \
      --exclude '.DS_Store' \
      --exclude 'ilab_results_benchmark1_*' \
      --exclude 'benchmarks/results' \
      --exclude 'benchmarks/reports' \
      --exclude 'benchmarks/plots' \
      -e "ssh ${SSH_OPTS[*]}" \
      ./ "${ILAB_USER}@${host}:${REMOTE_DIR}/"
  done
}

setup_python_env() {
  for host in "$SERVER1" "$CLIENT"; do
    echo "==> Ensuring Python venv on $host"
    ssh_block "$host" "
cd '${REMOTE_DIR}'
if [[ ! -x venv/bin/python ]]; then
  echo '[setup] creating venv on ${host}'
  rm -rf venv
  python3 -m venv venv
  ./venv/bin/python -m pip install --upgrade pip
  ./venv/bin/pip install -r requirements.txt
else
  echo '[setup] refreshing Python deps on ${host}'
  ./venv/bin/python -m pip install --upgrade pip >/dev/null
  ./venv/bin/pip install -r requirements.txt >/dev/null
fi
"
  done
}

clean_remote_outputs() {
  local host="$1"
  echo "==> Cleaning old benchmark outputs on $host"
  ssh_block "$host" "
cd '${REMOTE_DIR}'
rm -rf benchmarks/results benchmarks/reports benchmarks/plots
mkdir -p benchmarks/results benchmarks/reports benchmarks/plots
rm -f server.out server.pid
"
}

stop_server_host() {
  local host="$1"
  ssh_block "$host" "
cd '${REMOTE_DIR}'
if [[ -f server.pid ]]; then
  kill \$(cat server.pid) >/dev/null 2>&1 || true
  rm -f server.pid
fi
pkill -f 'python.*server.py' >/dev/null 2>&1 || true
" || true
}

stop_all_servers() {
  echo "==> Stopping remote server(s)"
  stop_server_host "$SERVER1"
  sleep 1
}

wait_for_server() {
  local host="$1"
  echo "==> Waiting for server on $host:$PORT"
  ssh_block "$host" "
cd '${REMOTE_DIR}'
for _ in \$(seq 1 ${STARTUP_TIMEOUT}); do
  if ./venv/bin/python - <<'PY'
import grpc
import sys
sys.path.insert(0, '.')
import objectstore_pb2_grpc as pb_grpc
channel = grpc.insecure_channel('${host}:${PORT}')
grpc.channel_ready_future(channel).result(timeout=1)
pb_grpc.ObjectStoreStub(channel)
print('ready')
PY
  then
    exit 0
  fi
  sleep 1
done

echo '[error] server did not become ready in time' >&2
if [[ -f server.out ]]; then
  echo '--- server.out ---' >&2
  tail -n 50 server.out >&2 || true
fi
exit 1
"
}

start_single() {
  echo "==> Starting single-node cluster on $SERVER1"
  local cluster="${SERVER1}:${PORT}"
  ssh_block "$SERVER1" "
cd '${REMOTE_DIR}'
mkdir -p benchmarks/results benchmarks/reports benchmarks/plots
rm -f server.out server.pid
nohup ./venv/bin/python server.py \
  --listen '${SERVER1}:${PORT}' \
  --cluster '${cluster}' \
  > server.out 2>&1 </dev/null &
echo \$! > server.pid
"
  wait_for_server "$SERVER1"
}

run_benchmark1() {
  echo "==> Running Benchmark 1 from $CLIENT"
  ssh_block "$CLIENT" "
cd '${REMOTE_DIR}'
./venv/bin/python benchmarks/benchmark_one.py \
  --target '${SERVER1}:${PORT}' \
  --duration '${DURATION}' \
  --get-key-count '${GET_KEY_COUNT}'
"
}

pull_results() {
  mkdir -p "$LOCAL_PULL_DIR"
  echo "==> Pulling results into $LOCAL_PULL_DIR"

  sshpass -e rsync -az \
    -e "ssh ${SSH_OPTS[*]}" \
    "${ILAB_USER}@${CLIENT}:${REMOTE_DIR}/benchmarks/results/" \
    "$LOCAL_PULL_DIR/results/"

  sshpass -e rsync -az \
    -e "ssh ${SSH_OPTS[*]}" \
    "${ILAB_USER}@${CLIENT}:${REMOTE_DIR}/benchmarks/reports/" \
    "$LOCAL_PULL_DIR/reports/"

  sshpass -e rsync -az \
    -e "ssh ${SSH_OPTS[*]}" \
    "${ILAB_USER}@${CLIENT}:${REMOTE_DIR}/benchmarks/plots/" \
    "$LOCAL_PULL_DIR/plots/"

  sshpass -e rsync -az \
    -e "ssh ${SSH_OPTS[*]}" \
    "${ILAB_USER}@${SERVER1}:${REMOTE_DIR}/server.out" \
    "$LOCAL_PULL_DIR/server.out"

  echo "==> Pulled files:"
  find "$LOCAL_PULL_DIR" -maxdepth 2 -type f | sort
}

cleanup() {
  stop_all_servers || true
}
trap cleanup EXIT INT TERM

#############################################
# MAIN
#############################################
main() {
  prompt_pass

  echo "=== Syncing project ==="
  sync_all

  echo "=== Setting up remote Python environments ==="
  setup_python_env

  echo "=== Cleaning remote benchmark outputs ==="
  stop_all_servers
  clean_remote_outputs "$SERVER1"
  clean_remote_outputs "$CLIENT"

  echo "=== Benchmark 1 (single-node) ==="
  start_single
  run_benchmark1
  sleep "$BETWEEN_RUN_DELAY"

  echo "=== Pulling output back locally ==="
  pull_results

  echo "Benchmark 1 completed."
  echo "Local results directory: $LOCAL_PULL_DIR"
}

main "$@"
