#!/bin/sh
set -e

# JuiceFS metadata URL
META_URL="postgres://postgres@postgres:5432/juicefs?sslmode=disable"

# OSS configuration
OSS_BUCKET="${OSS_BUCKET:-https://feedjfs.oss-cn-shanghai.aliyuncs.com}"
OSS_ACCESS_KEY="${OSS_ACCESS_KEY}"
OSS_SECRET_KEY="${OSS_SECRET_KEY}"

# Format the filesystem with OSS storage (idempotent - won't fail if already formatted)
echo "Formatting JuiceFS filesystem with OSS storage..."
juicefs format \
    --storage oss \
    --bucket "$OSS_BUCKET" \
    --access-key "$OSS_ACCESS_KEY" \
    --secret-key "$OSS_SECRET_KEY" \
    "$META_URL" feedjfs || echo "Filesystem already formatted or format failed, continuing..."

# Mount the filesystem
echo "Mounting JuiceFS..."
exec juicefs mount "$META_URL" /mnt
