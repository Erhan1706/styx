#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RELEASE_NAME=${RELEASE_NAME:-styx-cluster}
NAMESPACE=${NAMESPACE:-styx}
DEPLOY_MODE=${DEPLOY_MODE:-k8s-minikube}   # k8s-minikube | k8s-cluster

if [[ "$DEPLOY_MODE" == "k8s-minikube" ]]; then
  VALUES_FILE="$ROOT_DIR/charts/styx-cluster/dev_values.yaml"
  echo "Building current dev branch and loading the images to minikube..."
  "$ROOT_DIR/scripts/load_images_minikube.sh"
else
  VALUES_FILE="$ROOT_DIR/charts/styx-cluster/values.yaml"
fi

echo "Updating chart dependencies..."
helm dependency update "$ROOT_DIR/charts/styx-cluster"

# Optional experiment overrides (set by run_autoscale_experiment.sh or manually):
#   STYX_WORKER_REPLICAS, STYX_STANDBY_REPLICAS, STYX_WORKER_THREADS,
#   STYX_ENABLE_AUTOSCALE, STYX_FORECASTER_TYPE
HELM_SET_ARGS=()
if [[ -n "${STYX_WORKER_REPLICAS:-}" ]]; then
  HELM_SET_ARGS+=(--set "styx.worker.replicas=${STYX_WORKER_REPLICAS}")
fi
if [[ -n "${STYX_WORKER_THREADS:-}" ]]; then
  HELM_SET_ARGS+=(--set "styx.worker.env.WORKER_THREADS=${STYX_WORKER_THREADS}")
fi
if [[ -n "${STYX_SEQUENCE_MAX_SIZE:-}" ]]; then
  HELM_SET_ARGS+=(--set "styx.worker.env.SEQUENCE_MAX_SIZE=${STYX_SEQUENCE_MAX_SIZE}")
fi
if [[ -n "${STYX_STANDBY_REPLICAS:-}" ]]; then
  HELM_SET_ARGS+=(--set "styx.workerStandby.enabled=true")
  HELM_SET_ARGS+=(--set "styx.workerStandby.replicas=${STYX_STANDBY_REPLICAS}")
  if [[ -n "${STYX_WORKER_THREADS:-}" ]]; then
    HELM_SET_ARGS+=(--set "styx.workerStandby.env.WORKER_THREADS=${STYX_WORKER_THREADS}")
  fi
fi
if [[ -n "${STYX_ENABLE_AUTOSCALE:-}" ]]; then
  HELM_SET_ARGS+=(--set "styx.coordinator.env.ENABLE_AUTOSCALE=${STYX_ENABLE_AUTOSCALE}")
fi


echo "Installing/Upgrading Helm release '$RELEASE_NAME' in namespace '$NAMESPACE' using $(basename "$VALUES_FILE")..."
if ((${#HELM_SET_ARGS[@]} > 0)); then
  echo "Helm overrides: ${HELM_SET_ARGS[*]}"
fi
helm upgrade --install "$RELEASE_NAME" "$ROOT_DIR/charts/styx-cluster" \
  -n "$NAMESPACE" --create-namespace \
  -f "$VALUES_FILE" \
  "${HELM_SET_ARGS[@]}"

echo "Done."
