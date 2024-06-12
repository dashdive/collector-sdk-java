#!/bin/bash

set -euo pipefail

function validate_ip {
  if [[ ! $1 =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
    echo "Invalid IP format: $1"
    exit 1
  fi
}

function add_ip_to_known_hosts {
  KNOWN_HOSTS="$HOME/.ssh/known_hosts"
  ssh-keyscan -H $1 2>/dev/null | while IFS= read -r line
  do
      key=$(echo "$line" | awk '{print $3}')
      if ! grep -Fqs -- "$key" "$KNOWN_HOSTS"; then
          echo "$line" >> "$KNOWN_HOSTS"
          echo "Added key to known_hosts: $key"
      else
          echo "Key already exists in known_hosts: $key"
      fi
  done
}

function reset_test_files {
  ssh -T -i sdk-test.pem admin@$1 << 'EOF' | sed -e '1,/applicable law/d' 
    rm -rf collector-sdk
    mkdir -p collector-sdk
EOF
}

function sync_test_files {
  pushd .. > /dev/null
  rsync -avz -e "ssh -i ./integration_test/sdk-test.pem" . "admin@$1:/home/admin/collector-sdk" \
      --exclude-from '.gitignore' --exclude-from './integration_test/server/.gitignore' --exclude '.git'
  popd > /dev/null

  ssh -T -i sdk-test.pem admin@$1 << 'EOF' | sed -e '1,/applicable law/d'
    bash -ic "source /home/admin/.bashrc && cd collector-sdk/integration_test/server && pnpm i"
EOF
}

export -f validate_ip
export -f add_ip_to_known_hosts
export -f reset_test_files
export -f sync_test_files
