#!/bin/bash
set -e

echo "Starting Hive Metastore and HiveServer2..."

# Initialize schema if needed
export HIVE_CONF_DIR=/opt/hive/conf
export HADOOP_CLIENT_OPTS="-Xmx1G -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=hive123"

# Import custom config if exists
if [ -d "$HIVE_CUSTOM_CONF_DIR" ]; then
    echo "Loading custom Hive configuration from $HIVE_CUSTOM_CONF_DIR"
    cp -f $HIVE_CUSTOM_CONF_DIR/*.xml $HIVE_CONF_DIR/ 2>/dev/null || true
fi

# Initialize or upgrade schema
echo "Initializing Hive Metastore schema..."
/opt/hive/bin/schematool -dbType postgres -initOrUpgradeSchema || {
    echo "Schema initialization failed or already initialized, continuing..."
}

# Start Metastore in background
echo "Starting Hive Metastore on port 9083..."
/opt/hive/bin/hive --service metastore &
METASTORE_PID=$!

# Wait for Metastore to be ready
sleep 10

# Start HiveServer2 in foreground
echo "Starting HiveServer2 on port 10000..."
exec /opt/hive/bin/hive --service hiveserver2
