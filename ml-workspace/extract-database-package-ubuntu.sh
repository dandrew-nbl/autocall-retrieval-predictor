#!/bin/bash
# extract-database-package.sh
# Extracts the Factory Systems DB Client into ./shared-packages/app
# Designed for Ubuntu-based systems only

set -e

echo "🔍 Checking system requirements..."

# Check if the OS is Ubuntu
if ! grep -qi "ubuntu" /etc/os-release; then
  echo "❌ This script is designed for Ubuntu systems only."
  echo "Detected OS:"
  cat /etc/os-release
  exit 1
fi

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "🚧 Docker not found. Installing Docker..."
    sudo apt-get update
    sudo apt-get install -y docker.io
    sudo systemctl start docker
    sudo systemctl enable docker
    echo "✅ Docker installed."
else
    echo "✅ Docker is already installed."
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "🚧 Docker Compose not found. Installing Docker Compose..."
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" \
      -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    echo "✅ Docker Compose installed."
else
    echo "✅ Docker Compose is already installed."
fi

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "🚧 Python3 not found. Installing Python3..."
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip
    echo "✅ Python3 installed."
else
    echo "✅ Python3 is already installed."
fi

# Create shared-packages/app directory
mkdir -p ./shared-packages/app

echo "📦 Extracting factory-systems-db-client package from Docker image..."

docker run --rm -v $(pwd)/shared-packages/app:/output \
  adeji9/factory-systems-db-client:latest \
  bash -c "
    PACKAGE_PATH=\$(python -c 'import factory_systems_db_client; print(factory_systems_db_client.__file__)' | xargs dirname)
    echo 'Found package at: '\$PACKAGE_PATH
    cp -r \$PACKAGE_PATH/* /output/
    pip freeze | grep -i factory > /output/requirements-factory.txt 2>/dev/null || true
    echo '✅ Package extracted to ./shared-packages/app/'
  "

echo "🎉 Done! You can now run docker-compose up"