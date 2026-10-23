#!/usr/bin/env bash
# Creates the DispatchFlow queue + dead-letter queue with the settings the code expects.
#
#   AWS_REGION=us-east-1 ./infra/aws/create_sqs.sh
#   AWS_ENDPOINT_URL=http://localhost:4566 ./infra/aws/create_sqs.sh     # LocalStack
#
# Idempotent: safe to run twice. Prints the env vars to paste into your task definition / .env.
set -euo pipefail

REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
QUEUE="${SQS_QUEUE_NAME:-dispatchflow-orders}"
DLQ="${SQS_DLQ_NAME:-dispatchflow-orders-dlq}"
MAX_RECEIVE="${SQS_MAX_RECEIVE_COUNT:-10}"   # must be > MAX_ASSIGN_ATTEMPTS (default 6) so normal retries never dead-letter
VISIBILITY="${SQS_VISIBILITY_TIMEOUT:-30}"   # must exceed your slowest handler

aws() { command aws --region "$REGION" "$@"; }

echo "Region: $REGION"

# 1. dead-letter queue first (14 days retention gives you time to investigate)
aws sqs create-queue --queue-name "$DLQ" \
  --attributes MessageRetentionPeriod=1209600,SqsManagedSseEnabled=true >/dev/null
DLQ_URL=$(aws sqs get-queue-url --queue-name "$DLQ" --query QueueUrl --output text)
DLQ_ARN=$(aws sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)

# 2. main queue: long polling, encryption at rest, and the redrive policy pointing at the DLQ
ATTRS=$(mktemp)
trap 'rm -f "$ATTRS"' EXIT
cat > "$ATTRS" <<JSON
{
  "VisibilityTimeout": "$VISIBILITY",
  "ReceiveMessageWaitTimeSeconds": "20",
  "MessageRetentionPeriod": "345600",
  "SqsManagedSseEnabled": "true",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"$DLQ_ARN\",\"maxReceiveCount\":\"$MAX_RECEIVE\"}"
}
JSON
aws sqs create-queue --queue-name "$QUEUE" --attributes "file://$ATTRS" >/dev/null
# create-queue is a no-op when the queue exists with different attributes on some regions: set them explicitly
QUEUE_URL=$(aws sqs get-queue-url --queue-name "$QUEUE" --query QueueUrl --output text)
aws sqs set-queue-attributes --queue-url "$QUEUE_URL" --attributes "file://$ATTRS"

cat <<OUT

Done. Use these settings (note: do NOT set AWS_ENDPOINT_URL on real AWS):

  AWS_REGION=$REGION
  SQS_QUEUE_URL=$QUEUE_URL
  SQS_DLQ_URL=$DLQ_URL
  SQS_AUTO_CREATE=false
  SQS_WAIT_SECONDS=20

Queue ARN for IAM:  $(aws sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)
DLQ ARN for IAM:    $DLQ_ARN
OUT
