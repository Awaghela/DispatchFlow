# DispatchFlow

A real-time delivery dispatch platform. It matches orders with available couriers, tracks each delivery's status, and streams live updates to consumers and merchants.

**Python · FastAPI · PostgreSQL · Redis · AWS SQS · WebSockets · Docker**

![Control room](docs/screenshots/control-room.png)

## Features
- **Courier matching:** nearest free courier via a Redis GEO index, locked in PostgreSQL (`FOR UPDATE SKIP LOCKED`) so no courier is ever double-booked. Retries with backoff, reassigns on rejection, and reclaims jobs a courier never picks up.
- **Delivery tracking:** `CREATED → ASSIGNED → PICKED_UP → DELIVERED` (or `CANCELLED` / `FAILED`), with every change stored as an event.
- **Live updates:** WebSockets filtered per merchant and per customer. A client that reconnects with `last_event_id` gets everything it missed.
- **Idempotent APIs:** `Idempotency-Key` on order creation. Duplicate, late or out-of-order updates are safely ignored.
- **Async workflows on AWS SQS:** transactional outbox → SQS → worker, with a dead-letter queue for bad messages.
- **Redis caching:** order cache with version checks, and fallback to PostgreSQL if Redis goes down.
- **Dashboard and metrics:** live map, merchant and customer views, Prometheus metrics and a Grafana dashboard.

## Run it
```bash
docker compose up --build -d                    # app, PostgreSQL, Redis, local SQS emulator, Grafana
docker compose --profile demo up --build -d     # optional: simulated couriers and orders
```
- Dashboard: http://localhost:8000
- API docs: http://localhost:8000/docs
- Grafana: http://localhost:3000 (admin / admin)

### With real AWS SQS
1. Create the queues and an SQS-only IAM user (see the Quick start in [`docs/AWS_SETUP.md`](docs/AWS_SETUP.md)).
2. Copy `.env.aws.example` to `.env.aws` and fill it in. Never commit it.
3. Run:
   ```bash
   docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.aws.yml up --build -d
   ```

Only the worker talks to SQS, so everything else stays local.

## Load testing
A load generator simulates couriers and customers, including duplicate requests, courier rejections, delayed updates, retries and service outages (worker crash, Redis down, API and database restarts). After each run, 13 checks against the database confirm that nothing was lost, duplicated or double-booked.

| | Steady load | With 5 outages |
|---|---|---|
| Simulated events | 28,880 | 45,632 |
| Orders | 2,500 | 4,000 |
| Time to find a courier (p50 / p95) | 0.17 s / 0.30 s | 0.21 s / 1.70 s |
| Correctness checks passed | 13 / 13 | 13 / 13 |

These runs happened on a single 2-CPU machine at about 15 orders/s. They validate reliability and data consistency under failure, not production-scale capacity.

## Project layout
```
app/          API, worker, models, dispatch logic, Redis, SQS, WebSockets, dashboard
loadtest/     load generator, outage scenarios, correctness checks, results
tests/        51 automated tests
infra/aws/    SQS setup script and IAM policy
monitoring/   Prometheus and Grafana config
docs/         AWS setup guide, screenshots
```