#!/bin/bash
set -e

echo "Upgrading Hive libraries in Spark to version 4.0.0..."

# Backup directory
BACKUP_DIR="/opt/spark/jars/hive-backup-2.3.10"
mkdir -p "$BACKUP_DIR"

# Move old Hive 2.3.10 jars to backup (except hive-service-rpc-4.0.0.jar which is already correct)
echo "Backing up Hive 2.3.10 jars..."
mv /opt/spark/jars/hive-beeline-2.3.10.jar "$BACKUP_DIR/" 2>/dev/null || true
mv /opt/spark/jars/hive-cli-2.3.10.jar "$BACKUP_DIR/" 2>/dev/null || true
mv /opt/spark/jars/hive-common-2.3.10.jar "$BACKUP_DIR/" 2>/dev/null || true
mv /opt/spark/jars/hive-exec-2.3.10-core.jar "$BACKUP_DIR/" 2>/dev/null || true
mv /opt/spark/jars/hive-jdbc-2.3.10.jar "$BACKUP_DIR/" 2>/dev/null || true
mv /opt/spark/jars/hive-metastore-2.3.10.jar "$BACKUP_DIR/" 2>/dev/null || true
mv /opt/spark/jars/hive-serde-2.3.10.jar "$BACKUP_DIR/" 2>/dev/null || true
mv /opt/spark/jars/hive-shims*.jar "$BACKUP_DIR/" 2>/dev/null || true
mv /opt/spark/jars/hive-storage-api*.jar "$BACKUP_DIR/" 2>/dev/null || true

# Copy Hive 4.0.0 jars if they exist in the mounted directory
if [ -d "/tmp/hive-4.0.0-libs" ]; then
    echo "Copying Hive 4.0.0 jars to Spark..."
    cp /tmp/hive-4.0.0-libs/hive-*.jar /opt/spark/jars/ 2>/dev/null || true
    echo "Hive libraries upgraded successfully!"
else
    echo "Warning: Hive 4.0.0 libraries not found at /tmp/hive-4.0.0-libs"
fi

echo "Hive library upgrade complete!"
