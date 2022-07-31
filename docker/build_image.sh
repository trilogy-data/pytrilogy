#!/bin/bash

# run this from the root of the repository
# e.g. /bin/bash ./docker/build_image.sh
set -e

echo "Downloading Adventureworks2019DW Data Warehouse backup..."
wget -O ./docker/AdventureWorksDW2019.bak -q https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksDW2019.bak
docker build --no-cache ./docker/ -t pyreql-test-sqlserver