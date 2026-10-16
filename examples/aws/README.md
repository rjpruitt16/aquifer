# Aquifer AWS Quick Start

This stack launches one small Aquifer node for evaluation. It defaults to a one-vCPU `t2.micro`, installs the latest published Aquifer release, verifies the release checksum, and runs Aquifer under systemd with an encrypted 8 GiB EBS-backed Pebble store.

This is a test deployment, not a production topology. It has no TLS termination, load balancer, high availability, or automatic replacement. Port `8080` is reachable only from the IPv4 CIDR supplied during deployment, and SSH is not opened.

## Deploy

Prerequisites:

- AWS CLI authenticated to the target account
- A default VPC with a public subnet, or explicit `VPC_ID` and `SUBNET_ID`
- `curl`

From the Aquifer repository:

```bash
export AWS_REGION=us-east-2
./examples/aws/deploy-quickstart.sh
```

The script discovers the default VPC and subnet, restricts ingress to your current public IP as a `/32`, deploys the stack as `aquifer-test`, and prints its URLs and Session Manager command. It never opens SSH or port `8080` to the entire internet.

Useful overrides:

```bash
STACK_NAME=aquifer-demo \
AQUIFER_VERSION=v0.14.0 \
INSTANCE_TYPE=t3.small \
ALLOWED_URL_DOMAINS=hooks.example.com,api.example.com \
./examples/aws/deploy-quickstart.sh
```

`AQUIFER_VERSION` defaults to `latest`, resolved from GitHub when the instance first boots. Pin a release tag for repeatable deployments. The downloaded binary is always checked against that release's `checksums.txt` before installation.

## Verify

Wait roughly one minute for cloud-init, then use the `PublicAquiferUrl` output:

```bash
curl http://INSTANCE_IP:8080/health
```

Use the printed `SessionManagerCommand` for host access. On the instance:

```bash
sudo systemctl status aquifer
sudo journalctl -u aquifer -f
cat /etc/aquifer/release
```

Aquifer starts with a conservative default of 5 requests per second and 2 concurrent requests per upstream. Change those values through the template parameters when deploying manually, or edit `/etc/aquifer/aquifer.yml` for a temporary test and restart the service.

## Connect Inlet

Use `PrivateAquiferUrl` when Inlet's delivery Lambda has network access to the VPC, and set `ALLOWED_CIDR` to the appropriate Lambda subnet or VPC CIDR. If a VPC-connected Lambda exits through a NAT gateway with a fixed Elastic IP, that IP can instead be used as `ALLOWED_CIDR` for the public test URL. Do not solve Lambda connectivity by opening Aquifer to `0.0.0.0/0`; Aquifer expects authentication and destination policy to be enforced before traffic reaches it.

## Limits And Cleanup

- The public URL is plain HTTP. Use it only for narrow, short-lived tests where the source CIDR is controlled.
- A single node is neither highly available nor horizontally scalable.
- `latest` is resolved only during first boot. Delete and recreate the stack to install a newer release, or pin a new stack to an explicit version.
- The Pebble store survives process and instance reboots, but stack deletion removes its EBS volume and all queued data.
- EC2, EBS, public IPv4, and data transfer can incur charges even when the instance type qualifies for a free-tier offer.

Delete the test deployment when finished:

```bash
aws cloudformation delete-stack --stack-name aquifer-test --region "$AWS_REGION"
aws cloudformation wait stack-delete-complete --stack-name aquifer-test --region "$AWS_REGION"
```
