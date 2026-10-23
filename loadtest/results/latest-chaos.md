# DispatchFlow load test — 2026-10-09T20:53:48Z

**Mode:** load+chaos · **orders:** 4000 · **couriers:** 150 · **API instances:** 2 · **duration:** 270.1s

> single machine: PostgreSQL, Redis, SQS emulator (moto), 2 API processes, 1 worker and the load generator all share it (2 CPUs, Python 3.13.16).

## Volume

| metric | value |
|---|---|
| orders unique | 4,000 |
| duplicate create requests | 1,205 |
| client requests logical | 25,171 |
| client requests incl retries | 27,356 |
| gps pings | 7,918 |
| courier dup actions | 1,794 |
| late pickups after delivery | 298 |
| rejections | 217 |
| poison messages | 6 |
| order events persisted | 20,461 |
| live events over websocket | 36,151 |
| total simulated events | 45,632 |

## Throughput

| metric | value |
|---|---|
| orders per s submitted | 15.0 |
| requests per s | 101.3 |
| delivered per s | 14.6 |

## Outcomes

`{"CANCELLED": 69, "DELIVERED": 3931}` · failures: `{}`

Assignment latency (create → courier assigned): p50 0.21s · p95 1.70s · p99 5.80s

## API latency (client-side, ms)

| endpoint | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| GET /couriers/:id | 1,487 | 7.4 | 1119.4 | 2704.3 | 5083.0 |
| GET /orders/:id?fresh | 85 | 589.4 | 3284.0 | 6807.2 | 6807.2 |
| POST /couriers | 150 | 468.9 | 786.0 | 818.1 | 820.3 |
| POST /couriers/:id/status | 274 | 122.7 | 1096.4 | 1828.6 | 1854.5 |
| POST /orders | 5,531 | 28.0 | 2740.4 | 4465.0 | 8181.1 |
| POST /orders/:id/cancel | 123 | 29.0 | 2694.7 | 4344.4 | 5998.8 |
| POST /orders/:id/deliver | 5,000 | 37.2 | 2630.2 | 4805.7 | 11595.2 |
| POST /orders/:id/pickup | 4,925 | 38.6 | 2651.5 | 4551.6 | 8255.6 |
| POST /orders/:id/reject | 219 | 52.4 | 2566.5 | 3295.5 | 4477.6 |
| PUT /couriers/:id/location | 8,152 | 29.7 | 2631.5 | 4586.0 | 11482.7 |
| ready | 1 | 29.0 | 29.0 | 29.0 | 29.0 |
| stats | 1 | 20.3 | 20.3 | 20.3 | 20.3 |
| ALL | 25,796 | 29.6 | 2592.1 | 4535.0 | 11595.2 |

## Reliability signals

| signal | count |
|---|---|
| idempotent replays returned | 1205 |
| duplicate transitions absorbed | 1789 |
| stale updates ignored | 300 |
| transitions applied | 7846 |
| orders with assignment retries | 80 |
| orders reassigned after rejection | 199 |
| client retries | 2185 |
| client connection errors | 1408 |
| client 5xx seen | 777 |
| client gave up | 0 |
| websocket reconnects | 2 |
| websocket lagged | 0 |
| courier pull recoveries | 85 |
| orders redriven by reconciler | 0 |
| assignments reclaimed after courier timeout | 0 |
| agent errors | 0 |

## Injected service interruptions

- t+32.0s — worker killed (SIGKILL) mid-flight (down ~6.9s)
- t+74.7s — Redis down (cache, GEO index, pub/sub) (down ~9.5s)
- t+112.0s — shift change: 90% of couriers go offline (down ~14.0s)
- t+165.3s — ALL API instances killed (down ~10.8s)
- t+208.1s — PostgreSQL crash (immediate shutdown + WAL recovery) (down ~5.9s)

## Invariants

| | check | detail |
|---|---|---|
| ✅ | one order per idempotency key | 4000 unique keys sent -> 4000 orders stored |
| ✅ | every order reached a terminal state | 0 stuck; by status: {'CANCELLED': 69, 'DELIVERED': 3931} |
| ✅ | no transition recorded twice (created/picked_up/delivered/cancelled/failed) | 0 duplicated |
| ✅ | assignment history is consistent (assigned - reassigned in {0,1}) | 0 orders inconsistent |
| ✅ | delivered orders have a courier and monotonic timestamps | 0 malformed |
| ✅ | no courier was ever double-booked (event-log replay) | 0 couriers with overlapping jobs |
| ✅ | courier and order rows agree (no leaked or orphaned assignments) | 0 bad courier rows, 0 orphaned orders |
| ✅ | event log starts with order.created for every order | 0 violations |
| ✅ | outbox fully relayed to SQS | 0 unpublished rows |
| ✅ | every persisted event was delivered over WebSocket (incl. replay after reconnect) | 20461/20461 delivered, 0 missing |
| ✅ | SQS main queue fully drained | depth={'visible': 0, 'in_flight': 0, 'delayed': 0} |
| ✅ | dead-letter queue empty after reaping | depth={'visible': 0, 'in_flight': 0, 'delayed': 0} |
| ✅ | poison messages were parked in the DLQ and reaped | 6 injected, dead_lettered counter=6 |
