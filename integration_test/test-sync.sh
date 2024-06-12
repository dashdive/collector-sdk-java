#!/bin/bash

set -euo pipefail
pushd "$(dirname "$0")" > /dev/null
source test-utils.sh

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <ec2_uri>"
  exit 1
fi
EC2_URI=$1
validate_ip $EC2_URI
add_ip_to_known_hosts $EC2_URI


echo ""
echo ""
echo "Transferring test client and server..."
sync_test_files $EC2_URI


popd > /dev/null
