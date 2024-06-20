#!/bin/bash

SERVER_PORT=3232
function get_pid_on_port {
  PORT=$1
  if [[ $(uname) == "Linux" ]]; then
    sudo fuser -n tcp $PORT 2>/dev/null | awk '{print $1}'
  else
    sudo lsof -ti :$PORT
  fi
}

function print_usage_and_exit {
  echo "Usage: $0 [local|cloud] [-c]"
  exit 1
}


set -euo pipefail
pushd "$(dirname "$0")" > /dev/null


if [[ $# -lt 1 ]]; then
  print_usage_and_exit
fi
RUN_TYPE=$1
if [[ $RUN_TYPE != "local" && $RUN_TYPE != "cloud" ]]; then
  echo "Invalid run type: $RUN_TYPE"
  print_usage_and_exit
fi

shift
while getopts "c" opt; do
  case $opt in
    c) CENTRAL_MANUAL=true ;;
    \?) print_usage_and_exit ;;
  esac
done
CENTRAL_MANUAL="${CENTRAL_MANUAL:-false}"
# if central manual is true, ensure bearer token is set
if [[ $CENTRAL_MANUAL != "true" ]]; then
  if [[ -z ${BEARER_TOKEN+x} ]]; then
    echo 'BEARER_TOKEN must be set when running in "Central Manual Testing" mode'
fi

# Build and "publish" the library from source to local Maven repo
pushd .. > /dev/null
if [[ $CENTRAL_MANUAL == "false" ]]; then
  gradle clean :lib:publishToMavenLocal
fi
popd > /dev/null


# Start the test server (from integration_test/server)
pushd server > /dev/null
# SIGKILL any pre-existing server
PREV_SERVER_PID=$(get_pid_on_port $SERVER_PORT || true)
if [[ -n $PREV_SERVER_PID ]]; then
  echo "Killing previous server with PID: <$PREV_SERVER_PID>"
  kill -9 $PREV_SERVER_PID
fi

pnpm start --port $SERVER_PORT --file test_output.json >/dev/null &
popd > /dev/null


# Run the integration test client
pushd .. > /dev/null
RUN_COMMAND=
if [[ $CENTRAL_MANUAL == "true" ]]; then
    RUN_COMMAND="BEARER_TOKEN=$BEARER_TOKEN gradle clean :integration_test:client:runCentralManual"
else
    RUN_COMMAND="gradle clean :integration_test:client:run"
fi

if [[ $RUN_TYPE == "local" ]]; then
    AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id) \
    AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key) \
        $RUN_COMMAND
else
    $RUN_COMMAND
fi
popd > /dev/null


# SIGTERM the mock server, allowing it to write its logs
SERVER_PID=$(get_pid_on_port $SERVER_PORT)
echo "Killing server with PID: <$SERVER_PID>"
kill $SERVER_PID


# Test the logs to ensure the server received correct requests
pushd server > /dev/null
VERSION_CENTRAL_MANUAL=1.0.0
VERSION_STANDARD=1.0.0
if [[ $CENTRAL_MANUAL == "true" ]]; then
    VERSION=$VERSION_CENTRAL_MANUAL
else
    VERSION=$VERSION_STANDARD
fi
pnpm test -- --sdk-version=$VERSION --run-type=$RUN_TYPE
popd > /dev/null
