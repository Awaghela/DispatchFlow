# DispatchFlow load test — 2026-10-09T20:48:56Z

**Mode:** load · **orders:** 2500 · **couriers:** 150 · **API instances:** 2 · **duration:** 170.2s

> single machine: PostgreSQL, Redis, SQS emulator (moto), 2 API processes, 1 worker and the load generator all share it (2 CPUs, Python 3.13.16).

## Volume

| metric | value |
|---|---|
| orders unique | 2,500 |
| duplicate create requests | 768 |
| client requests logical | 16,139 |
| client requests incl retries | 16,141 |
| gps pings | 4,970 |
| courier dup actions | 1,088 |
| late pickups after delivery | 200 |
| rejections | 139 |
| poison messages | 6 |
| order events persisted | 12,741 |
| live events over websocket | 23,127 |
| total simulated events | 28,880 |

## Throughput

| metric | value |
|---|---|
| orders per s submitted | 15.0 |
| requests per s | 94.8 |
| delivered per s | 14.5 |

## Outcomes

`{"CANCELLED": 26, "DELIVERED": 2474}` · failures: `{}`

Assignment latency (create → courier assigned): p50 0.17s · p95 0.30s · p99 0.47s

## API latency (client-side, ms)

| endpoint | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| GET /couriers/:id | 1,475 | 5.0 | 21.1 | 183.4 | 207.9 |
| GET /orders/:id?fresh | 1 | 6.4 | 6.4 | 6.4 | 6.4 |
| POST /couriers | 150 | 592.7 | 813.3 | 831.5 | 835.8 |
| POST /orders | 3,268 | 11.1 | 29.3 | 49.3 | 215.0 |
| POST /orders/:id/cancel | 78 | 8.0 | 27.2 | 35.6 | 35.6 |
| POST /orders/:id/deliver | 3,029 | 13.1 | 30.6 | 50.9 | 235.6 |
| POST /orders/:id/pickup | 3,027 | 9.4 | 25.0 | 41.8 | 249.4 |
| POST /orders/:id/reject | 139 | 19.0 | 30.6 | 34.2 | 35.0 |
| PUT /couriers/:id/location | 4,970 | 8.4 | 21.5 | 36.4 | 381.2 |
| ready | 1 | 24.4 | 24.4 | 24.4 | 24.4 |
| stats | 1 | 18.1 | 18.1 | 18.1 | 18.1 |
| ALL | 15,987 | 9.7 | 26.7 | 50.5 | 381.2 |

## Reliability signals

| signal | count |
|---|---|
| idempotent replays returned | 768 |
| duplicate transitions absorbed | 1083 |
| stale updates ignored | 200 |
| transitions applied | 4913 |
| orders with assignment retries | 0 |
| orders reassigned after rejection | 131 |
| client retries | 2 |
| client connection errors | 2 |
| client 5xx seen | 0 |
| client gave up | 0 |
| websocket reconnects | 0 |
| websocket lagged | 0 |
| courier pull recoveries | 1 |
| orders redriven by reconciler | 0 |
| assignments reclaimed after courier timeout | 0 |
| agent errors | 0 |

## Injected service interruptions

- none

## Invariants

| | check | detail |
|---|---|---|
| ✅ | one order per idempotency key | 2500 unique keys sent -> 2500 orders stored |
| ✅ | every order reached a terminal state | 0 stuck; by status: {'CANCELLED': 26, 'DELIVERED': 2474} |
| ✅ | no transition recorded twice (created/picked_up/delivered/cancelled/failed) | 0 duplicated |
| ✅ | assignment history is consistent (assigned - reassigned in {0,1}) | 0 orders inconsistent |
| ✅ | delivered orders have a courier and monotonic timestamps | 0 malformed |
| ✅ | no courier was ever double-booked (event-log replay) | 0 couriers with overlapping jobs |
| ✅ | courier and order rows agree (no leaked or orphaned assignments) | 0 bad courier rows, 0 orphaned orders |
| ✅ | event log starts with order.created for every order | 0 violations |
| ✅ | outbox fully relayed to SQS | 0 unpublished rows |
| ✅ | every persisted event was delivered over WebSocket (incl. replay after reconnect) | 12741/12741 delivered, 0 missing |
| ✅ | SQS main queue fully drained | depth={'visible': 0, 'in_flight': 0, 'delayed': 0} |
| ✅ | dead-letter queue empty after reaping | depth={'visible': 0, 'in_flight': 0, 'delayed': 0} |
| ✅ | poison messages were parked in the DLQ and reaped | 6 injected, dead_lettered counter=6 |
