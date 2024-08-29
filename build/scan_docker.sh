#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to display usage information
usage() {
    echo "Usage: $0 [context_path] [-f <dockerfile_path>]"
    echo "  context_path: Path to the build context (default: current directory)"
    echo "  -f: Path to the Dockerfile (default: ./Dockerfile)"
    exit 1
}

# Function to clean up resources
cleanup() {
    local exit_code=$?
    echo "Cleaning up..."
    [ -n "$TAR_FILENAME" ] && rm -f "$TAR_FILENAME"
    [ -n "$IMAGE_SHA" ] && docker rmi "$IMAGE_SHA" > /dev/null 2>&1
    exit $exit_code
}

# Set trap to ensure cleanup happens even if the script exits unexpectedly
trap cleanup EXIT

# Default values
CONTEXT="."
DOCKERFILE="./Dockerfile"

# Parse command line arguments
if [ $# -gt 0 ] && [ "${1:0:1}" != "-" ]; then
    CONTEXT="$1"
    shift
fi

while getopts "f:h" opt; do
    case $opt in
        f) DOCKERFILE="$OPTARG";;
        h) usage;;
        *) usage;;
    esac
done

# Build the Docker image and capture the SHA
echo "Building Docker image..."
IMAGE_SHA=$(docker build -q -f "$DOCKERFILE" "$CONTEXT")

if [ $? -ne 0 ] || [ -z "$IMAGE_SHA" ]; then
    echo "Error: Failed to build Docker image or capture its SHA."
    exit 1
fi

echo "Image built with SHA: $IMAGE_SHA"

# Create a unique filename for the tar
TAR_FILENAME="/tmp/image-${IMAGE_SHA#sha256:}-$(date +%s).tar"

# Save the image to a tar file
echo "Saving image to a tar file..."
docker save "$IMAGE_SHA" > "$TAR_FILENAME"

trivy image --input "$TAR_FILENAME" --exit-code 1 --scanners secret

# Run TruffleHog scan
echo "Running TruffleHog scan..."
docker run --rm -v /tmp:/tmp trufflesecurity/trufflehog:latest \
    docker --image "file://$TAR_FILENAME" \
    --no-verification \
    --fail


echo "Build and scan completed. Image SHA: $IMAGE_SHA"

