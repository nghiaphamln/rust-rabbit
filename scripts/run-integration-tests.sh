#!/bin/bash

# run-integration-tests.sh - Script to run integration tests with RabbitMQ

set -e

echo "🐰 Starting RabbitMQ Integration Tests for RustRabbit"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose >/dev/null 2>&1; then
    print_error "docker-compose is not installed. Please install docker-compose first."
    exit 1
fi

print_status "Checking if RabbitMQ containers are already running..."

# Stop any existing containers
if docker-compose -f docker-compose.test.yml ps | grep -q "Up"; then
    print_warning "Stopping existing RabbitMQ containers..."
    docker-compose -f docker-compose.test.yml down
fi

# Start RabbitMQ containers
print_status "Starting RabbitMQ containers with Docker Compose..."
docker-compose -f docker-compose.test.yml up -d

# Wait for RabbitMQ to be ready
print_status "Waiting for RabbitMQ to be ready..."
max_wait=60
elapsed=0

while [ $elapsed -lt $max_wait ]; do
    if docker-compose -f docker-compose.test.yml exec -T rabbitmq rabbitmq-diagnostics ping >/dev/null 2>&1; then
        print_success "RabbitMQ is ready!"
        break
    fi
    
    echo -n "."
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $max_wait ]; then
    print_error "RabbitMQ did not become ready within $max_wait seconds"
    print_error "Showing container logs:"
    docker-compose -f docker-compose.test.yml logs
    exit 1
fi

# Test RabbitMQ connectivity
print_status "Testing RabbitMQ connectivity..."
if timeout 5 bash -c "</dev/tcp/localhost/5672" >/dev/null 2>&1; then
    print_success "RabbitMQ port 5672 is accessible"
else
    print_error "Cannot connect to RabbitMQ on port 5672"
    exit 1
fi

# Check if management interface is accessible
if timeout 5 bash -c "</dev/tcp/localhost/15672" >/dev/null 2>&1; then
    print_success "RabbitMQ management interface is accessible at http://localhost:15672"
    print_status "Login: admin/password"
else
    print_warning "RabbitMQ management interface is not accessible"
fi

# Run the integration tests
print_status "Running integration tests..."
echo "=================================================="

# Set environment variables for tests
export RUST_LOG=info
export RUST_BACKTRACE=1

# Run integration tests with proper test isolation
if cargo test --test integration_example -- --test-threads=1 --nocapture; then
    print_success "All integration tests passed! 🎉"
    test_result=0
else
    print_error "Some integration tests failed! ❌"
    test_result=1
fi

echo "=================================================="

# Show RabbitMQ status
print_status "RabbitMQ Container Status:"
docker-compose -f docker-compose.test.yml ps

# Option to keep containers running for debugging
if [ "$1" = "--keep-running" ]; then
    print_warning "Keeping RabbitMQ containers running for debugging..."
    print_status "Management UI: http://localhost:15672 (admin/password)"
    print_status "AMQP Port: localhost:5672"
    print_status "To stop containers: docker-compose -f docker-compose.test.yml down"
else
    print_status "Stopping RabbitMQ containers..."
    docker-compose -f docker-compose.test.yml down
    print_success "Cleanup completed"
fi

echo "=================================================="

if [ $test_result -eq 0 ]; then
    print_success "Integration test suite completed successfully! ✅"
else
    print_error "Integration test suite failed! ❌"
fi

exit $test_result