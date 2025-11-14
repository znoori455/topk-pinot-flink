#!/bin/bash
set -e

# Directories containing schemas and tables
SCHEMA_DIR=./pinot/conf/schemas
TABLE_DIR=./pinot/conf/tables
#SCHEMA_DIR=pinot/conf/schemas
#TABLE_DIR=pinot/conf/tables

echo "Registering Pinot schemas and tables..."

# Wait a few seconds to ensure controller dependencies are up (if needed)
#sleep 10

# Register schemas
if [ -d "$SCHEMA_DIR" ]; then
  for schema_file in "$SCHEMA_DIR"/*.json; do
    if [ -f "$schema_file" ]; then
      echo "\n Registering schema: $schema_file"
      curl -X POST "pinot-controller:9000/schemas" \
        -H "Content-Type: application/json" \
        -d @"$schema_file"

#      /opt/pinot/bin/pinot-admin.sh AddSchema \
#        -schemaFile "$schema_file" \
#        -exec || echo "Schema registration failed for $schema_file, continuing..."
    fi
  done
fi

# Register tables
if [ -d "$TABLE_DIR" ]; then
  for table_file in "$TABLE_DIR"/*.json; do
    if [ -f "$table_file" ]; then
      echo "\n Registering table: $table_file"
      curl -X POST "pinot-controller:9000/tables" \
        -H "Content-Type: application/json" \
        -d @"$table_file"

#      /opt/pinot/bin/pinot-admin.sh AddTable \
#        -tableConfigFile "$table_file" \
#        -exec || echo "Table registration failed for $table_file, continuing..."
    fi
  done
fi

