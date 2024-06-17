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


set -euo pipefail
pushd "$(dirname "$0")" > /dev/null


if [[ $# -ne 1 ]]; then
  echo "Usage: $0 [local|cloud]"
  exit 1
fi
RUN_TYPE=$1
if [[ $RUN_TYPE != "local" && $RUN_TYPE != "cloud" ]]; then
  echo "Invalid run type: $RUN_TYPE"
  exit 1
fi


# Build and "publish" the library from source to local Maven repo
pushd .. > /dev/null
gradle :lib:publishToMavenLocal
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
if [[ $RUN_TYPE == "local" ]]; then
    AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id) \
    AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key) \
        gradle clean :integration_test:client:run
else
    gradle clean :integration_test:client:run
fi
popd > /dev/null


# SIGTERM the mock server, allowing it to write its logs
SERVER_PID=$(get_pid_on_port $SERVER_PORT)
echo "Killing server with PID: <$SERVER_PID>"
kill $SERVER_PID


# Test the logs to ensure the server received correct requests
pushd server > /dev/null
pnpm test -- --sdk-version=1.0.0 --run-type=$RUN_TYPE
popd > /dev/null
