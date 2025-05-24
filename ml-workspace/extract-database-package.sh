#!/bin/bash
# extract-database-package.sh
# Run this script to extract the factory-systems-db-client package

echo "Extracting factory-systems-db-client package..."

# Create shared-packages directory if it doesn't exist
mkdir -p ./shared-packages

# Start factory-systems container temporarily and copy the package
docker run --rm -v $(pwd)/shared-packages:/output \
  adeji9/factory-systems-db-client:latest \
  bash -c "
    # Find where the package is installed
    PACKAGE_PATH=\$(python -c 'import factory_systems_db_client; print(factory_systems_db_client.__file__)' | xargs dirname)
    echo \"Found package at: \$PACKAGE_PATH\"
    
    # Copy the entire package to output directory
    cp -r \$PACKAGE_PATH /output/
    
    # Also copy any requirements or dependencies info
    pip freeze | grep -i factory > /output/requirements-factory.txt 2>/dev/null || true
    
    echo \"Package extracted to ./shared-packages/\"
  "

echo "Done! Package available in ./shared-packages/"
echo "You can now start your docker-compose services."