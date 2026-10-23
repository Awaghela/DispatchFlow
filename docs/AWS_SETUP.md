# Connecting DispatchFlow to AWS

**Quick start** (about 10 minutes, no AWS CLI needed) runs everything in Docker with the worker on real SQS. **Path A** does the same without Docker. **Path B** deploys the whole thing to AWS (RDS + ElastiCache + SQS + ECS Fargate behind a load balancer).

> **Honesty note.** The SQS code is exercised in this repo against an SQS emulator (moto in the tests and load test, LocalStack in `docker-compose.yml`), and the commands below follow AWS's documented CLI syntax. They have not been run against a live AWS account by the author of this repo. Run `scripts/aws_check.py` at each step: it tells you exactly what is wrong and what to fix.

Which AWS service does what:

| Need | AWS service | Used by | Local stand-in |
|---|---|---|---|
| Durable async work (order → courier matching) | **SQS** standard queue + dead-letter queue | worker (API never touches SQS) | LocalStack / moto |
| Orders, couriers, event log, outbox | **RDS for PostgreSQL** 16 | API + worker | postgres container |
| Cache, courier GEO index, WebSocket fan-out | **ElastiCache for Redis** | API + worker | redis container |
| Run the containers | **ECS on Fargate** | – | docker compose |
| Public entry point, WebSockets | **Application Load Balancer** | – | localhost:8000 |
| Logs, alarms | **CloudWatch** | – | – |

Why a *standard* queue (not FIFO)? Standard queues are at-least-once and unordered, so every consumer here is written to be idempotent: handlers lock the order row and check the state machine, which turns a duplicate or out-of-order message into a no-op. That gives you high throughput without FIFO's rate limits.

---

## Quick start: Docker on your laptop + real SQS (recommended for a personal project)

Only SQS runs on AWS. PostgreSQL, Redis, the API, the worker and the dashboard run in Docker on your laptop. Cost: $0 within SQS's 1 million free requests a month. Works the same on macOS, Windows and Linux, and you don't need the AWS CLI on your laptop.

**1. Protect yourself first (2 min).** In the AWS console: *Billing and Cost Management → Budgets → Create budget → Zero spend budget*. You get an email the moment anything costs money.

**2. Create the two queues (3 min).** Open **CloudShell** (the `>_` icon in the console's top bar, region **us-east-1**). Upload `infra/aws/create_sqs.sh` (*Actions → Upload file*), then run:

```bash
bash create_sqs.sh
```

Copy the `SQS_QUEUE_URL` and `SQS_DLQ_URL` lines it prints, and note your 12-digit account ID (top-right menu).

**3. Create an app user with SQS-only access (3 min).** *IAM → Policies → Create policy → JSON*, paste `infra/aws/worker-policy.json`, replace `REGION` with `us-east-1` and `ACCOUNT_ID` with your account ID, and name it `dispatchflow-worker`. Then *IAM → Users → Create user* `dispatchflow-app` and attach only that policy (no console access). Open the user, go to *Security credentials → Create access key → Application running outside AWS*, and copy both keys.

**4. Point the worker at SQS (1 min).** In the project folder:

```bash
cp .env.aws.example .env.aws      # Windows PowerShell: copy .env.aws.example .env.aws
```

Fill in the five values. `.env.aws` is git-ignored: never commit it.

**5. Run it.**

```bash
make aws-up        # or: docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.aws.yml up --build -d
make aws-check     # or: docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.aws.yml exec worker python scripts/aws_check.py
make aws-demo      # optional demo traffic (same command with --profile demo)
```

`aws-check` should print PASS for PostgreSQL, Redis and SQS (it skips the send/receive round trip, because the app user is correctly not allowed to create queues). Open http://localhost:8000: orders get couriers, which means messages went to SQS in the cloud and came back. In the console, *SQS → dispatchflow-orders → Monitoring* shows the messages.

**6. When you're done.** `make aws-down`, then in CloudShell:

```bash
aws sqs delete-queue --queue-url <SQS_QUEUE_URL>
aws sqs delete-queue --queue-url <SQS_DLQ_URL>
```

Then delete the access key (or the whole `dispatchflow-app` user) in IAM.

**If something fails:** `docker compose logs worker` shows the error. `InvalidClientTokenId` means the keys are wrong. `AccessDenied` means the policy's region or account ID is wrong or it isn't attached. `NonExistentQueue` means the region or queue URL is wrong.

---

## Path A: your laptop → real SQS (without Docker, using the AWS CLI) (≈10 min)

### 1. Install and sign in

```bash
# AWS CLI v2:  https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
aws configure sso            # recommended: short-lived credentials
# or, for a throwaway sandbox account only:  aws configure
export AWS_PROFILE=<your-profile>
export AWS_REGION=us-east-1
aws sts get-caller-identity  # must print your account id
```

Never commit access keys. If you must use an IAM user, give it only the policy in step 3.

### 2. Create the queues

```bash
./infra/aws/create_sqs.sh
```

This creates `dispatchflow-orders-dlq` (14-day retention) and `dispatchflow-orders` with:

- encryption at rest (SSE-SQS)
- 20-second long polling (fewer empty receives, lower cost and latency)
- a **redrive policy**: after 10 failed receives a message moves to the dead-letter queue
- a 30-second visibility timeout (must exceed your slowest handler; ours take milliseconds)

It prints the environment variables to use. The numbers are deliberate: `maxReceiveCount` (10) must exceed `MAX_ASSIGN_ATTEMPTS` (6), otherwise normal "no courier free yet" retries would dead-letter orders.

### 3. Least-privilege IAM

Only the **worker** talks to SQS. The API process needs no SQS permissions at all.

```bash
sed -e "s/REGION/$AWS_REGION/g" -e "s/ACCOUNT_ID/$(aws sts get-caller-identity --query Account --output text)/g" \
  infra/aws/worker-policy.json > /tmp/worker-policy.json
aws iam create-policy --policy-name dispatchflow-worker --policy-document file:///tmp/worker-policy.json
```

Attach it to the user or role you use locally. (For Path B you attach it to the worker's task role instead.) Note there is no `sqs:CreateQueue`: in production, queues are created by you, not by the app, so leave `SQS_AUTO_CREATE=false`.

### 4. Run the app against it

PostgreSQL and Redis can stay local for this path:

```bash
docker compose up -d postgres redis          # or: python scripts/stackctl.py up postgres redis

export DATABASE_URL=postgresql+asyncpg://dispatch:dispatch@localhost:5432/dispatchflow
export REDIS_URL=redis://localhost:6379/0
unset AWS_ENDPOINT_URL                        # IMPORTANT: this variable points the SDK at an emulator
export SQS_QUEUE_URL=<printed by create_sqs.sh>
export SQS_DLQ_URL=<printed by create_sqs.sh>
export SQS_AUTO_CREATE=false SQS_WAIT_SECONDS=20

python scripts/aws_check.py                   # every line should say PASS
python -m app.worker &                        # terminal 1
uvicorn app.main:app --port 8000              # terminal 2
```

### 5. Prove it works

```bash
python -m loadtest.run --mode demo            # couriers + orders; watch http://localhost:8000
aws sqs get-queue-attributes --queue-url "$SQS_QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

Messages appear and drain as the worker consumes them. Delete the demo traffic with `Ctrl-C`; tear down with `aws sqs delete-queue` for both queues.

**Troubleshooting**

| Symptom | Cause |
|---|---|
| `AccessDenied` on `ReceiveMessage` | The identity isn't the one the policy is attached to; check `aws sts get-caller-identity` |
| `NonExistentQueue` | Wrong region (`AWS_REGION`) or the queue wasn't created |
| Orders stay `CREATED` | The worker isn't running, or `outbox_backlog` is growing: the worker can't reach SQS |
| Everything lands in the DLQ | `maxReceiveCount` too low, or handlers are throwing: check the worker logs |
| `Could not connect to the endpoint URL` | `AWS_ENDPOINT_URL` is still set to LocalStack |

---

## Path B: deploy everything to AWS

Architecture: private subnets hold ECS tasks, RDS and ElastiCache; only the ALB is public. API tasks (2+) and worker tasks (2+) are the same image with a different command.

```
 Internet ─► ALB ─► ECS: api  (x2+) ──► RDS PostgreSQL
                         │  ▲    └────► ElastiCache Redis ◄── pub/sub fan-out to every API task
                         │  └──────────────────────────────────────┐
                    outbox rows                                      │
                         ▼                                           │
                 ECS: worker (x2+) ──► SQS orders ──► (self) ───────┘
                         └──────────► SQS dead-letter queue
```

Set shell variables once:

```bash
export AWS_REGION=us-east-1
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export VPC_ID=<vpc-id>                       # default VPC works for a first deployment
export PRIVATE_SUBNETS=<subnet-a>,<subnet-b>   # at least two AZs
export PUBLIC_SUBNETS=<subnet-c>,<subnet-d>
```

> For a quick experiment the default VPC's subnets will do, with `assignPublicIp=ENABLED` on the tasks so they can pull images without a NAT gateway. For anything real, use private subnets plus a NAT gateway or VPC endpoints (a NAT gateway is usually the surprise line on the bill).

### 1. Security groups (the allow-list is the firewall)

```bash
ALB_SG=$(aws ec2 create-security-group --group-name df-alb --description "DispatchFlow ALB" --vpc-id $VPC_ID --query GroupId --output text)
APP_SG=$(aws ec2 create-security-group --group-name df-app --description "DispatchFlow tasks" --vpc-id $VPC_ID --query GroupId --output text)
DB_SG=$(aws ec2 create-security-group --group-name df-db --description "DispatchFlow RDS" --vpc-id $VPC_ID --query GroupId --output text)
REDIS_SG=$(aws ec2 create-security-group --group-name df-redis --description "DispatchFlow Redis" --vpc-id $VPC_ID --query GroupId --output text)

aws ec2 authorize-security-group-ingress --group-id $ALB_SG   --protocol tcp --port 443  --cidr 0.0.0.0/0   # or 80 while testing
aws ec2 authorize-security-group-ingress --group-id $APP_SG   --protocol tcp --port 8000 --source-group $ALB_SG
aws ec2 authorize-security-group-ingress --group-id $DB_SG    --protocol tcp --port 5432 --source-group $APP_SG
aws ec2 authorize-security-group-ingress --group-id $REDIS_SG --protocol tcp --port 6379 --source-group $APP_SG
```

### 2. SQS

Same as Path A, step 2: `./infra/aws/create_sqs.sh`. Keep the printed `SQS_QUEUE_URL` and `SQS_DLQ_URL`.

### 3. PostgreSQL (RDS)

```bash
aws rds create-db-subnet-group --db-subnet-group-name df-db \
  --db-subnet-group-description "DispatchFlow" --subnet-ids ${PRIVATE_SUBNETS//,/ }

DB_PASSWORD=$(openssl rand -base64 24 | tr -d '/+=')
aws rds create-db-instance --db-instance-identifier dispatchflow-db \
  --engine postgres --engine-version 16 --db-instance-class db.t4g.micro \
  --allocated-storage 20 --storage-encrypted \
  --master-username dispatch --master-user-password "$DB_PASSWORD" --db-name dispatchflow \
  --db-subnet-group-name df-db --vpc-security-group-ids $DB_SG \
  --no-publicly-accessible --backup-retention-period 7
aws rds wait db-instance-available --db-instance-identifier dispatchflow-db
DB_HOST=$(aws rds describe-db-instances --db-instance-identifier dispatchflow-db --query 'DBInstances[0].Endpoint.Address' --output text)
```

For production add `--multi-az`, and size up from `t4g.micro`. RDS for PostgreSQL 15+ **requires TLS by default**, so the connection string must carry `ssl=require` (asyncpg's spelling):

```bash
aws secretsmanager create-secret --name dispatchflow/database-url \
  --secret-string "postgresql+asyncpg://dispatch:${DB_PASSWORD}@${DB_HOST}:5432/dispatchflow?ssl=require"
export DATABASE_URL_SECRET_ARN=$(aws secretsmanager describe-secret --secret-id dispatchflow/database-url --query ARN --output text)
```

The tables are created automatically on first start (guarded by a Postgres advisory lock so the API and worker don't race). For a team project you would switch this to Alembic migrations.

### 4. Redis (ElastiCache)

```bash
aws elasticache create-cache-subnet-group --cache-subnet-group-name df-redis \
  --cache-subnet-group-description "DispatchFlow" --subnet-ids ${PRIVATE_SUBNETS//,/ }
aws elasticache create-cache-cluster --cache-cluster-id dispatchflow-redis \
  --engine redis --cache-node-type cache.t4g.micro --num-cache-nodes 1 \
  --cache-subnet-group-name df-redis --security-group-ids $REDIS_SG
aws elasticache wait cache-cluster-available --cache-cluster-id dispatchflow-redis
REDIS_HOST=$(aws elasticache describe-cache-clusters --cache-cluster-id dispatchflow-redis --show-cache-node-info \
  --query 'CacheClusters[0].CacheNodes[0].Endpoint.Address' --output text)
export REDIS_URL=redis://$REDIS_HOST:6379/0
```

For production use a replication group with Multi-AZ failover and in-transit encryption (then `REDIS_URL=rediss://...`). The app is designed to survive a Redis outage: it falls back to PostgreSQL for reads and matching, and the load test proves it, so this is not on the critical path.

### 5. Container image

```bash
aws ecr create-repository --repository-name dispatchflow
aws ecr get-login-password | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
docker build -t $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/dispatchflow:v1 .
docker push $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/dispatchflow:v1
```

On an Apple Silicon Mac, either add `--platform linux/amd64` to `docker build` and use `CPU_ARCH=X86_64` below, or leave it as is and use `CPU_ARCH=ARM64` (Graviton Fargate is also cheaper).

### 6. IAM roles

```bash
# execution role: pulls the image, writes logs, reads the DB secret
aws iam create-role --role-name dispatchflow-execution --assume-role-policy-document file://infra/aws/trust-ecs-tasks.json
aws iam attach-role-policy --role-name dispatchflow-execution --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
aws iam put-role-policy --role-name dispatchflow-execution --policy-name read-db-secret --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"secretsmanager:GetSecretValue\",\"Resource\":\"$DATABASE_URL_SECRET_ARN\"}]}"

# task roles: api gets nothing (it never calls SQS); worker gets the SQS policy
aws iam create-role --role-name dispatchflow-api-task    --assume-role-policy-document file://infra/aws/trust-ecs-tasks.json
aws iam create-role --role-name dispatchflow-worker-task --assume-role-policy-document file://infra/aws/trust-ecs-tasks.json
sed -e "s/REGION/$AWS_REGION/g" -e "s/ACCOUNT_ID/$ACCOUNT_ID/g" infra/aws/worker-policy.json > /tmp/worker-policy.json
aws iam put-role-policy --role-name dispatchflow-worker-task --policy-name sqs --policy-document file:///tmp/worker-policy.json
```

### 7. Task definitions and services

```bash
aws logs create-log-group --log-group-name /dispatchflow/api
aws logs create-log-group --log-group-name /dispatchflow/worker
aws ecs create-cluster --cluster-name dispatchflow

export SQS_QUEUE_URL=... SQS_DLQ_URL=...      # from step 2
export IMAGE_TAG=v1 CPU_ARCH=ARM64            # or X86_64
for ROLE in api worker; do
  export ROLE
  if [ $ROLE = api ]; then export COMMAND='["uvicorn","app.main:app","--host","0.0.0.0","--port","8000","--no-access-log"]'
  else export COMMAND='["python","-m","app.worker"]'; fi
  envsubst < infra/aws/task-definition.template.json > /tmp/td-$ROLE.json
  aws ecs register-task-definition --cli-input-json file:///tmp/td-$ROLE.json >/dev/null
done
```

(`envsubst` is part of GNU gettext: `brew install gettext` on macOS.)

### 8. Load balancer (with WebSockets)

```bash
ALB_ARN=$(aws elbv2 create-load-balancer --name dispatchflow --subnets ${PUBLIC_SUBNETS//,/ } --security-groups $ALB_SG \
  --query 'LoadBalancers[0].LoadBalancerArn' --output text)
TG_ARN=$(aws elbv2 create-target-group --name dispatchflow-api --protocol HTTP --port 8000 --vpc-id $VPC_ID \
  --target-type ip --health-check-path /health --health-check-interval-seconds 15 \
  --query 'TargetGroups[0].TargetGroupArn' --output text)
aws elbv2 create-listener --load-balancer-arn $ALB_ARN --protocol HTTP --port 80 \
  --default-actions Type=forward,TargetGroupArn=$TG_ARN
```

Notes:

- ALBs support WebSockets natively (the HTTP upgrade just works). The server sends a ping every 20 s, which keeps idle sockets under the ALB's default 60 s idle timeout. To be safe: `aws elbv2 modify-load-balancer-attributes --load-balancer-arn $ALB_ARN --attributes Key=idle_timeout.timeout_seconds,Value=300`.
- No sticky sessions needed: any API task can serve any socket, because events fan out through Redis pub/sub (and fall back to a database tail if Redis is down).
- For HTTPS, request an ACM certificate and add a 443 listener; redirect 80 to 443.

### 9. Run it

```bash
SUBNETS=$PRIVATE_SUBNETS
NET="awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$APP_SG],assignPublicIp=DISABLED}"

aws ecs create-service --cluster dispatchflow --service-name api --task-definition dispatchflow-api \
  --desired-count 2 --launch-type FARGATE --network-configuration "$NET" \
  --load-balancers targetGroupArn=$TG_ARN,containerName=api,containerPort=8000 \
  --health-check-grace-period-seconds 40

aws ecs create-service --cluster dispatchflow --service-name worker --task-definition dispatchflow-worker \
  --desired-count 2 --launch-type FARGATE --network-configuration "$NET"
```

Both services can scale out freely: multiple workers are safe (row locks with `SKIP LOCKED`, idempotent handlers, a `SKIP LOCKED` outbox relay).

### 10. Smoke test

```bash
ALB_DNS=$(aws elbv2 describe-load-balancers --load-balancer-arns $ALB_ARN --query 'LoadBalancers[0].DNSName' --output text)
curl http://$ALB_DNS/ready
curl -X POST http://$ALB_DNS/api/couriers -H 'content-type: application/json' -d '{"name":"Ana","lat":37.77,"lng":-122.42,"online":true}'
curl -X POST http://$ALB_DNS/api/orders -H 'content-type: application/json' -H 'Idempotency-Key: first-order' \
  -d '{"merchant_id":"m1","consumer_id":"c1","pickup":{"lat":37.771,"lng":-122.419},"dropoff":{"lat":37.79,"lng":-122.41}}'
# open http://$ALB_DNS/ in a browser: the dashboard shows the order being assigned
```

Run the load test from your laptop against the ALB with `python -m loadtest.run --mode load --apis http://$ALB_DNS --orders 2000 --rate 40` (omit `--chaos`; that flag controls local processes). Check network paths from inside a running task with `aws ecs execute-command --cluster dispatchflow --task <id> --container api --interactive --command "python scripts/aws_check.py"` (requires ECS Exec to be enabled on the service).

### 11. Alarms worth having

```bash
SNS_ARN=<your SNS topic>
# 1. Anything in the dead-letter queue means a message failed repeatedly: look at it today
aws cloudwatch put-metric-alarm --alarm-name dispatchflow-dlq-not-empty --namespace AWS/SQS \
  --metric-name ApproximateNumberOfMessagesVisible --dimensions Name=QueueName,Value=dispatchflow-orders-dlq \
  --statistic Maximum --period 60 --evaluation-periods 1 --threshold 0 --comparison-operator GreaterThanThreshold \
  --alarm-actions $SNS_ARN
# 2. Work is waiting longer than 2 minutes: workers are down or too few
aws cloudwatch put-metric-alarm --alarm-name dispatchflow-queue-age --namespace AWS/SQS \
  --metric-name ApproximateAgeOfOldestMessage --dimensions Name=QueueName,Value=dispatchflow-orders \
  --statistic Maximum --period 60 --evaluation-periods 3 --threshold 120 --comparison-operator GreaterThanThreshold \
  --alarm-actions $SNS_ARN
```

Also alarm on ALB `HTTPCode_Target_5XX_Count`, RDS `CPUUtilization` / `FreeStorageSpace`, and the application metrics (`/metrics`, same names as the Grafana dashboard): `dispatchflow_outbox_backlog` rising means SQS is unreachable, `dispatchflow_redis_errors_total` rising means the cache tier is down (service continues, slower).

Scale the **worker** service on `ApproximateNumberOfMessagesVisible` (step scaling) and the **API** service on CPU (target tracking 60%).

### 12. Tear down (stop the meter)

```bash
aws ecs update-service --cluster dispatchflow --service api --desired-count 0
aws ecs update-service --cluster dispatchflow --service worker --desired-count 0
aws ecs delete-service --cluster dispatchflow --service api --force
aws ecs delete-service --cluster dispatchflow --service worker --force
aws elbv2 delete-load-balancer --load-balancer-arn $ALB_ARN
aws rds delete-db-instance --db-instance-identifier dispatchflow-db --skip-final-snapshot
aws elasticache delete-cache-cluster --cache-cluster-id dispatchflow-redis
aws sqs delete-queue --queue-url "$SQS_QUEUE_URL"; aws sqs delete-queue --queue-url "$SQS_DLQ_URL"
aws ecr delete-repository --repository-name dispatchflow --force
```

(Plus the target group, security groups, secret, log groups and IAM roles.)

---

## Environment variable reference

| Variable | Local | AWS |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://dispatch:dispatch@localhost:5432/dispatchflow` | from Secrets Manager, ends with `?ssl=require` |
| `REDIS_URL` | `redis://localhost:6379/0` | `redis://<elasticache-endpoint>:6379/0` (`rediss://` with TLS) |
| `AWS_ENDPOINT_URL` | `http://localhost:4566` (LocalStack) | **unset** |
| `AWS_REGION` | `us-east-1` | your region |
| `SQS_QUEUE_URL` / `SQS_DLQ_URL` | optional | from `create_sqs.sh` |
| `SQS_AUTO_CREATE` | `true` | `false` |
| `SQS_WAIT_SECONDS` | `2` | `20` |
| `MAX_ASSIGN_ATTEMPTS` | `6` | `6` (must stay below `maxReceiveCount`) |
| `ASSIGNMENT_TIMEOUT_SECONDS` | `120` | how long a courier has to start a job before it's reassigned |
| `REDRIVE_AFTER_SECONDS` | `60` | how long an unassigned order may sit idle before it's re-queued |
| `CHAOS_WORKER_FAIL_RATE` | `0` | **never set in production** |

Credentials are never environment variables on AWS: the ECS task role supplies them automatically.
