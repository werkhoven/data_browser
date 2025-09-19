#!/bin/bash

# S3 Bucket Creation Script for LocalStack
# This script creates an S3 bucket in LocalStack using the awslocal CLI

set -e

# Default bucket name if not provided via environment variable
BUCKET_NAME=${S3_BUCKET:-data-browser-uploads}
REGION=${AWS_DEFAULT_REGION:-us-east-1}

echo "=========================================="
echo "🚀 Starting S3 bucket creation process"
echo "=========================================="
echo "📦 Bucket name: $BUCKET_NAME"
echo "🌍 Region: $REGION"
echo "=========================================="

# Wait for LocalStack to be ready
echo "⏳ Waiting for LocalStack to be ready..."
until curl -f http://localhost:4566/_localstack/health > /dev/null 2>&1; do
    echo "⏳ LocalStack is not ready yet. Waiting..."
    sleep 2
done

echo "✅ LocalStack is ready!"
echo "🔧 Creating S3 bucket..."

# Create the S3 bucket
echo "📋 Running: awslocal s3api create-bucket --bucket $BUCKET_NAME --region $REGION"
awslocal s3api create-bucket \
    --bucket "$BUCKET_NAME" \
    --region "$REGION"

echo "✅ S3 bucket '$BUCKET_NAME' created successfully in region '$REGION'"

# Verify the bucket was created
echo "🔍 Verifying bucket creation..."
awslocal s3api head-bucket --bucket "$BUCKET_NAME"
echo "✅ Bucket verification successful!"
echo "=========================================="
echo "🎉 S3 bucket setup completed successfully!"
echo "=========================================="
