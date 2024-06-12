#!/bin/bash

set -euo pipefail
pushd "$(dirname "$0")" > /dev/null

RUN_TYPE=$1
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 [local|cloud]"
  exit 1
fi
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
# SIGTERM any pre-existing server
ps aux | grep -E 'pnpm start' | grep -v 'grep' | awk '{print $2}' | xargs -I{} kill {} || true
pnpm start --port 3232 --file server/test_output.json &
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
ps aux | grep -E 'pnpm start' | grep -v 'grep' | awk '{print $2}' | xargs -I{} kill {}
