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
echo "Initial instance setup..."
AWS_ACCESS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET_KEY=$(aws configure get aws_secret_access_key)
if [[ ! $AWS_ACCESS_KEY =~ ^[A-Z0-9]{20}$ ]]; then
  echo "Invalid AWS_ACCESS_KEY format: $AWS_ACCESS_KEY"
  exit 1
fi
if [[ ! $AWS_SECRET_KEY =~ ^[a-zA-Z0-9/+]{40}$ ]]; then
  echo "Invalid AWS_SECRET_KEY format: $AWS_SECRET_KEY"
  exit 1
fi
ssh -T -i sdk-test.pem admin@$EC2_URI << EOF | sed -e '1,/applicable law/d' 
  if ! grep -Fqs -- 'export AWS_ACCESS_KEY_ID=' ~/.bashrc; then
    echo "export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY" >> ~/.bashrc
  fi

  if ! grep -Fqs -- 'export AWS_SECRET_ACCESS_KEY=' ~/.bashrc; then
    echo "export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_KEY" >> ~/.bashrc
  fi
EOF

ssh -T -i sdk-test.pem admin@$EC2_URI << 'EOF' | sed -e '1,/applicable law/d' 
  sudo apt-get update
  sudo apt-get install -y rsync default-jdk zip nodejs

  sudo rm -rf /opt/gradle
  sudo mkdir /opt/gradle
  curl -L https://services.gradle.org/distributions/gradle-8.6-bin.zip --output gradle-8.6.bin.zip
  sudo unzip -d /opt/gradle gradle-8.6.bin.zip
  rm gradle-8.6.bin.zip
  if ! grep -Fqs -- 'export PATH=$PATH:"/opt/gradle/gradle-8.6/bin"' ~/.bashrc; then
    echo 'export PATH=$PATH:"/opt/gradle/gradle-8.6/bin"' >> ~/.bashrc
  fi

  curl -fsSL https://get.pnpm.io/install.sh | sh -
  pnpm install -g typescript ts-node
EOF


echo ""
echo ""
echo "Transferring test client and server..."
reset_test_files $EC2_URI
sync_test_files $EC2_URI


echo ""
echo "================================================"
echo "Setup complete!"
echo ""
echo "SSH into the instance using the following command:"
echo "ssh -i sdk-test.pem admin@$EC2_URI"
echo "================================================"


popd > /dev/null
