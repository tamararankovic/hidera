#!/bin/bash

# Usage: ./run.sh <num_nodes> <num_peers_per_node>
N=${1:-4}
M=${2:-2}
PORT=8000
NETWORK="hidera_net"

echo "Starting $N nodes with $M peers each..."

# Build the image
docker build -t node:latest .

# Create network if not exists
if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  echo "Creating network $NETWORK..."
  docker network create "$NETWORK"
fi

# Clean up old containers
for i in $(seq 1 $N); do
  docker rm -f node_$i >/dev/null 2>&1 || true
done

# Build base data
for i in $(seq 1 $N); do
  HOSTS[$i]="node_$i"
done

# ---------------- Build peer relationships ----------------
declare -A PEERS

add_peer() {
  local a=$1 b=$2
  [[ $a -eq $b ]] && return
  [[ ,${PEERS[$a]}, == *",$b,"* ]] && return
  [[ ,${PEERS[$b]}, == *",$a,"* ]] && return
  [[ -n ${PEERS[$a]} ]] && PEERS[$a]+=","
  [[ -n ${PEERS[$b]} ]] && PEERS[$b]+=","
  PEERS[$a]+=$b
  PEERS[$b]+=$a
}

# Ensure each node has at least one peer (form a simple ring)
for i in $(seq 1 $N); do
  j=$(( (i % N) + 1 ))
  add_peer "$i" "$j"
done

# Compute target number of edges for avg degree ~M
TARGET_EDGES=$((M * N / 2))
CURRENT_EDGES=0
for v in "${PEERS[@]}"; do
  # count actual peers (not commas)
  [ -z "$v" ] && continue
  count=$(awk -F, '{print NF}' <<< "$v")
  CURRENT_EDGES=$((CURRENT_EDGES + count))
done
CURRENT_EDGES=$((CURRENT_EDGES / 2))

echo "Current edges: $CURRENT_EDGES / target: $TARGET_EDGES"

# Add random symmetric edges until reaching target
while [ "$CURRENT_EDGES" -lt "$TARGET_EDGES" ]; do
  a=$((RANDOM % N + 1))
  b=$((RANDOM % N + 1))
  [[ $a -eq $b ]] && continue
  [[ ,${PEERS[$a]}, == *",$b,"* ]] && continue
  add_peer "$a" "$b"
  CURRENT_EDGES=$((CURRENT_EDGES + 1))
done

# ---------- Run containers ----------
for i in $(seq 1 $N); do
  NAME="${HOSTS[$i]}"

  # Normalize peers list
  raw="${PEERS[$i]}"
  raw="${raw#,}"  # trim leading comma
  ids=(${raw//,/ })
  ids=($(printf "%s\n" "${ids[@]}" | sort -n | uniq)) # clean duplicates

  # Build env vars
  PEER_IDS=$(IFS=,; echo "${ids[*]}")
  PEER_HOSTS=$(IFS=,; for id in "${ids[@]}"; do printf "%s," "${HOSTS[$id]}"; done | sed 's/,$//')

  echo "Node $i ($NAME): peers -> ${PEER_IDS:-none}"

  docker run -d \
    --name "$NAME" \
    --hostname "$NAME" \
    --network "$NETWORK" \
    -e LISTEN_HOST="$NAME" \
    -e ID="$i" \
    -e LISTEN_PORT="$PORT" \
    -e PEER_IDS="$PEER_IDS" \
    -e PEER_HOSTS="$PEER_HOSTS" \
    node:latest >/dev/null
done

echo "Nodes started"
