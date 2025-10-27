#!/bin/bash

# wait-for-rabbitmq.sh - Script to wait for RabbitMQ to be ready

set -e

host="${1:-localhost}"
port="${2:-5672}"
user="${3:-admin}"
password="${4:-password}"
vhost="${5:-test}"

echo "Waiting for RabbitMQ at $host:$port to be ready..."

# Maximum wait time in seconds
max_wait=60
elapsed=0

while [ $elapsed -lt $max_wait ]; do
    if rabbitmq-diagnostics ping >/dev/null 2>&1; then
        echo "RabbitMQ is ready!"
        
        # Test connection with credentials
        if timeout 5 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
            echo "RabbitMQ port $port is accessible"
            exit 0
        fi
    fi
    
    echo "RabbitMQ not ready yet... waiting ($elapsed/$max_wait seconds)"
    sleep 2
    elapsed=$((elapsed + 2))
done

echo "ERROR: RabbitMQ did not become ready within $max_wait seconds"
exit 1