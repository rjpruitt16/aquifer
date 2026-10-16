#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
STACK_NAME=${STACK_NAME:-aquifer-test}
REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region)}}

if [[ -z "$REGION" ]]; then
  echo "Set AWS_REGION, for example: export AWS_REGION=us-east-2" >&2
  exit 1
fi

VPC_ID=${VPC_ID:-$(aws ec2 describe-vpcs \
  --region "$REGION" \
  --filters Name=is-default,Values=true \
  --query 'Vpcs[0].VpcId' \
  --output text)}

if [[ -z "$VPC_ID" || "$VPC_ID" == "None" ]]; then
  echo "No default VPC found. Set VPC_ID and SUBNET_ID explicitly." >&2
  exit 1
fi

SUBNET_ID=${SUBNET_ID:-$(aws ec2 describe-subnets \
  --region "$REGION" \
  --filters Name=vpc-id,Values="$VPC_ID" Name=default-for-az,Values=true \
  --query 'sort_by(Subnets, &AvailabilityZone)[0].SubnetId' \
  --output text)}

if [[ -z "$SUBNET_ID" || "$SUBNET_ID" == "None" ]]; then
  echo "No default subnet found. Set SUBNET_ID to a public subnet." >&2
  exit 1
fi

if [[ -z "${ALLOWED_CIDR:-}" ]]; then
  PUBLIC_IP=$(curl --fail --silent --show-error https://checkip.amazonaws.com | tr -d '[:space:]')
  ALLOWED_CIDR="$PUBLIC_IP/32"
fi

PARAMETERS=(
  "VpcId=$VPC_ID"
  "SubnetId=$SUBNET_ID"
  "AllowedCidr=$ALLOWED_CIDR"
)

[[ -n "${AQUIFER_VERSION:-}" ]] && PARAMETERS+=("AquiferVersion=$AQUIFER_VERSION")
[[ -n "${INSTANCE_TYPE:-}" ]] && PARAMETERS+=("InstanceType=$INSTANCE_TYPE")
[[ -n "${ALLOWED_URL_DOMAINS:-}" ]] && PARAMETERS+=("AllowedUrlDomains=$ALLOWED_URL_DOMAINS")

aws cloudformation deploy \
  --region "$REGION" \
  --stack-name "$STACK_NAME" \
  --template-file "$SCRIPT_DIR/aquifer-quickstart.yaml" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides "${PARAMETERS[@]}"

aws cloudformation describe-stacks \
  --region "$REGION" \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[].{Name:OutputKey,Value:OutputValue}' \
  --output table
