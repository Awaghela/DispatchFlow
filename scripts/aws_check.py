#!/usr/bin/env python3
"""Connectivity check for every dependency, with a fix hint when something is wrong.

    python scripts/aws_check.py              # uses the same environment variables as the app
    python scripts/aws_check.py --skip-sqs   # only PostgreSQL + Redis

Run it on your laptop first, then again from inside the ECS task
(`aws ecs execute-command ... --command "python scripts/aws_check.py"`) to prove the network paths,
security groups and IAM role are right before you debug anything else.
"""
import argparse
import asyncio
import json
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import get_settings  # noqa: E402

cfg = get_settings()
failures = 0


def ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def bad(msg: str, hint: str) -> None:
    global failures
    failures += 1
    print(f"  FAIL  {msg}\n        -> {hint}")


def mask(url: str) -> str:
    if "@" in url:
        head, tail = url.split("@", 1)
        return head.split("://")[0] + "://***@" + tail
    return url


async def check_db() -> None:
    print(f"PostgreSQL  {mask(cfg.database_url)}")
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    eng = create_async_engine(cfg.database_url, pool_pre_ping=True)
    try:
        t0 = time.perf_counter()
        async with eng.connect() as c:
            version = (await c.execute(text("select version()"))).scalar_one()
            await c.execute(text("select 1"))
        ok(f"connected in {(time.perf_counter() - t0) * 1000:.0f} ms ({version.split(',')[0]})")
        async with eng.connect() as c:
            has = (await c.execute(text("select to_regclass('public.orders') is not null"))).scalar_one()
        ok("schema present") if has else print("  INFO  schema not created yet: the API / worker create it on first start")
    except Exception as exc:
        hint = "check the security group allows port 5432 from this task, and add ?ssl=require to DATABASE_URL (RDS enforces TLS)"
        bad(f"cannot connect: {type(exc).__name__}: {str(exc)[:140]}", hint)
    finally:
        await eng.dispose()


async def check_redis() -> None:
    print(f"Redis       {mask(cfg.redis_url)}")
    import redis.asyncio as aioredis

    r = aioredis.from_url(cfg.redis_url, socket_connect_timeout=3, socket_timeout=3)
    try:
        t0 = time.perf_counter()
        await r.ping()
        key = f"df:check:{uuid.uuid4().hex[:8]}"
        await r.set(key, "1", ex=10)
        assert await r.get(key) == b"1"
        ok(f"ping + set/get in {(time.perf_counter() - t0) * 1000:.0f} ms")
        info = await r.info("server")
        ok(f"redis {info.get('redis_version')}")
        await r.delete(key)
    except Exception as exc:
        bad(f"cannot use redis: {type(exc).__name__}: {str(exc)[:140]}",
            "check the ElastiCache security group allows 6379 from this task; use rediss:// if in-transit encryption is on")
    finally:
        await r.aclose()
    print("            (the app keeps running without Redis; it just loses caching and the GEO index)")


async def check_sqs() -> None:
    where = cfg.aws_endpoint_url or f"AWS ({cfg.aws_region})"
    print(f"SQS         {where}")
    import aioboto3
    from botocore.exceptions import ClientError, NoCredentialsError

    async with aioboto3.Session().client("sqs", region_name=cfg.aws_region, endpoint_url=cfg.aws_endpoint_url) as sqs:
        try:
            ident = "emulator" if cfg.aws_endpoint_url else None
            if not ident:
                import aiobotocore.session  # noqa: F401

                async with aioboto3.Session().client("sts", region_name=cfg.aws_region) as sts:
                    who = await sts.get_caller_identity()
                    ok(f"credentials valid: {who['Arn']}")
        except NoCredentialsError:
            bad("no AWS credentials found", "export AWS_PROFILE=<profile> (aws configure sso) locally, or attach a task role on ECS")
            return
        except Exception as exc:
            bad(f"credentials rejected: {str(exc)[:140]}", "run `aws sts get-caller-identity` to see what the CLI uses")
            return

        async def url(name, explicit):
            if explicit:
                return explicit
            return (await sqs.get_queue_url(QueueName=name))["QueueUrl"]

        try:
            main = await url(cfg.sqs_queue_name, cfg.sqs_queue_url)
            dlq = await url(cfg.sqs_dlq_name, cfg.sqs_dlq_url)
            ok(f"queues found: {cfg.sqs_queue_name}, {cfg.sqs_dlq_name}")
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            hint = ("create them: infra/aws/create_sqs.sh" if "NonExistent" in code or "QueueDoesNotExist" in code
                    else "the role needs sqs:GetQueueUrl on both queues: infra/aws/worker-policy.json")
            bad(f"{code} while looking up the queues", hint)
            return

        try:
            attrs = (await sqs.get_queue_attributes(QueueUrl=main, AttributeNames=["All"]))["Attributes"]
            rp = json.loads(attrs.get("RedrivePolicy", "{}"))
            dlq_arn = (await sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"]))["Attributes"]["QueueArn"]
            if rp.get("deadLetterTargetArn") == dlq_arn:
                ok(f"dead-letter queue wired up (maxReceiveCount={rp.get('maxReceiveCount')})")
                if int(rp.get("maxReceiveCount", 0)) <= cfg.max_assign_attempts:
                    bad("maxReceiveCount is not larger than MAX_ASSIGN_ATTEMPTS",
                        f"set maxReceiveCount above {cfg.max_assign_attempts + 3} so normal retries don't dead-letter")
            else:
                bad("main queue has no redrive policy pointing at the DLQ", "re-run infra/aws/create_sqs.sh or set the RedrivePolicy attribute")
            vt = int(attrs.get("VisibilityTimeout", 0))
            ok(f"visibility timeout {vt}s (must exceed the slowest handler)") if vt >= 10 else bad(f"visibility timeout {vt}s is very short", "use 30s")
        except ClientError as exc:
            bad(f"{exc.response['Error']['Code']} reading queue attributes", "grant sqs:GetQueueAttributes")

        # Round-trip on a throwaway queue so we never inject a message the real worker would consume.
        tmp = f"dispatchflow-check-{uuid.uuid4().hex[:8]}"
        try:
            q = (await sqs.create_queue(QueueName=tmp))["QueueUrl"]
        except ClientError:
            print("  INFO  skipped the send/receive round trip (no sqs:CreateQueue, which is the correct least-privilege setup)")
            return
        try:
            await sqs.send_message(QueueUrl=q, MessageBody="ping")
            got = (await sqs.receive_message(QueueUrl=q, WaitTimeSeconds=3)).get("Messages", [])
            assert got and got[0]["Body"] == "ping"
            await sqs.delete_message(QueueUrl=q, ReceiptHandle=got[0]["ReceiptHandle"])
            ok("send / receive / delete round trip")
        except Exception as exc:
            bad(f"round trip failed: {exc}", "check sqs:SendMessage / ReceiveMessage / DeleteMessage permissions")
        finally:
            await sqs.delete_queue(QueueUrl=q)


async def main(skip_sqs: bool) -> int:
    await check_db()
    print()
    await check_redis()
    print()
    if not skip_sqs:
        await check_sqs()
        print()
    print("ALL CHECKS PASSED" if not failures else f"{failures} CHECK(S) FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-sqs", action="store_true")
    sys.exit(asyncio.run(main(ap.parse_args().skip_sqs)))
