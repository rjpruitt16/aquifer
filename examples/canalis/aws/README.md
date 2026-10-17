# Canalis + Aquifer on AWS

This evaluation stack creates two EC2 hosts in one public subnet:

1. **Canalis host:** Valkey, Canalis, and Aquifer A run under Docker Compose.
2. **Aquifer worker:** Aquifer B runs under Docker Compose with its own encrypted disk.

Only Canalis port `8080` is reachable from `ALLOWED_CIDR`. The worker has no public ingress; its Aquifer port accepts traffic only from the Canalis host security group. Both hosts use IMDSv2 and Session Manager, with no SSH port or key pair.

## Deploy

Prerequisites are an authenticated AWS CLI, `curl`, and either a default VPC with a public subnet or explicit `VPC_ID` and `SUBNET_ID` values.

```bash
export AWS_REGION=us-east-2
./examples/canalis/aws/deploy-quickstart.sh
```

The script restricts Canalis to your current public IP, deploys the stack, and waits until Canalis reports both Aquifer nodes. Useful overrides:

```bash
STACK_NAME=canalis-aquifer-demo \
INSTANCE_TYPE=t3.small \
AQUIFER_VERSION=v0.14.0 \
CANALIS_VERSION=0.2 \
ALLOWED_URL_DOMAINS=api.example.com \
./examples/canalis/aws/deploy-quickstart.sh
```

`QUICKSTART_REF` defaults to `main`. It exists so contributors can validate a quick-start branch before merge; ordinary users should leave it unchanged. Aquifer release downloads are checksum-verified during the image build. Docker Compose is also downloaded at a pinned version and checksum-verified before installation.

## Verify

Use the `CanalisUrl` output:

```bash
curl http://CANALIS_IP:8080/health
curl -N http://CANALIS_IP:8080/proxy \
  -H 'content-type: application/json' \
  -d '{
    "user_id":"aws-quickstart",
    "idempotent_key":"aws-quickstart-1",
    "url":"https://postman-echo.com/get",
    "method":"GET",
    "webhook_url":"https://postman-echo.com/post"
  }'
```

The health response should report `"instances":{"total":2`. Use the output Session Manager commands to inspect either host. The Compose project and install log are at `/opt/aquifer/examples/canalis` and `/var/log/canalis-aquifer-install.log`.

## Limits And Cleanup

- The public Canalis URL is plain HTTP and restricted only by source CIDR. Use it for a short test, not real credentials or production traffic.
- Canalis, Valkey, and Aquifer A share one host. Losing that host interrupts fleet routing even though Aquifer B remains alive.
- Both instances have public IPs only to download images and releases. The worker still has no inbound public security-group rule.
- Root EBS volumes are encrypted but deleted with the stack, including queued test work.
- EC2, EBS, public IPv4, and data transfer can incur charges.

Delete every resource created by the stack:

```bash
aws cloudformation delete-stack --stack-name canalis-aquifer-test --region "$AWS_REGION"
aws cloudformation wait stack-delete-complete --stack-name canalis-aquifer-test --region "$AWS_REGION"
```
