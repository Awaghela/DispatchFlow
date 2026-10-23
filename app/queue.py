"""Thin async wrapper around Amazon SQS (aioboto3).

Design notes
------------
* A *standard* queue + idempotent consumers, not FIFO. Standard queues are at-least-once and
  unordered, so every handler is written to be safe under duplicates and reordering (it locks the
  order row and checks the state machine). This scales without FIFO's throughput ceiling.
* Failed or delayed work is retried by changing the message's visibility timeout (exponential
  backoff). After ``sqs_max_receive_count`` receives SQS moves the message to the dead-letter queue.
* Works unchanged against AWS, LocalStack and moto: set AWS_ENDPOINT_URL for the emulators.
"""
import asyncio
import json
import logging
from dataclasses import dataclass

import aioboto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from .config import Settings, get_settings

log = logging.getLogger("dispatchflow.sqs")

_INVALID_RECEIPT = ("ReceiptHandleIsInvalid", "InvalidParameterValue", "MessageNotInflight")


@dataclass
class Message:
    message_id: str
    receipt: str
    body: dict
    receive_count: int


class SQSQueue:
    def __init__(self, settings: Settings | None = None) -> None:
        self.s = settings or get_settings()
        self.client = None
        self._cm = None
        self.url: str = ""
        self.dlq_url: str = ""

    async def start(self) -> None:
        cfg = Config(
            region_name=self.s.aws_region,
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=3,
            read_timeout=self.s.sqs_wait_seconds + 15,
            max_pool_connections=64,
        )
        self._cm = aioboto3.Session().client(
            "sqs", region_name=self.s.aws_region, endpoint_url=self.s.aws_endpoint_url, config=cfg
        )
        self.client = await self._cm.__aenter__()
        self.url, self.dlq_url = await self._resolve()
        log.info("sqs ready queue=%s dlq=%s", self.url, self.dlq_url)

    async def stop(self) -> None:
        if self._cm is not None:
            await self._cm.__aexit__(None, None, None)
            self._cm = self.client = None

    # ---- queue discovery / creation ---------------------------------------------------------
    async def _url_for(self, name: str, explicit: str | None, create: dict | None) -> str:
        if explicit:
            return explicit
        try:
            return (await self.client.get_queue_url(QueueName=name))["QueueUrl"]
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            missing = code in ("AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist")
            if not (missing and self.s.sqs_auto_create):
                raise
        resp = await self.client.create_queue(QueueName=name, Attributes=create or {})
        return resp["QueueUrl"]

    async def _resolve(self) -> tuple[str, str]:
        dlq = await self._url_for(self.s.sqs_dlq_name, self.s.sqs_dlq_url, {"MessageRetentionPeriod": "1209600"})
        arn = (await self.client.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"]))[
            "Attributes"
        ]["QueueArn"]
        main = await self._url_for(
            self.s.sqs_queue_name,
            self.s.sqs_queue_url,
            {
                "VisibilityTimeout": str(self.s.sqs_visibility_timeout),
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": arn, "maxReceiveCount": str(self.s.sqs_max_receive_count)}
                ),
            },
        )
        return main, dlq

    # ---- producing ---------------------------------------------------------------------------
    async def send_batch(self, items: list[tuple[str, dict]]) -> None:
        """items: [(entry_id, body)]. Raises if any entry fails so the caller can retry."""
        for i in range(0, len(items), 10):
            chunk = items[i : i + 10]
            resp = await self.client.send_message_batch(
                QueueUrl=self.url,
                Entries=[{"Id": str(eid), "MessageBody": json.dumps(body)} for eid, body in chunk],
            )
            if resp.get("Failed"):
                raise RuntimeError(f"SQS rejected {len(resp['Failed'])} messages: {resp['Failed'][0]}")

    async def send_dlq(self, body: dict) -> None:
        """Park a message that can never succeed (kept for inspection / redrive)."""
        await self.client.send_message(QueueUrl=self.dlq_url, MessageBody=json.dumps(body))

    # ---- consuming ---------------------------------------------------------------------------
    async def receive(self, max_messages: int = 10, wait: int | None = None, dlq: bool = False) -> list[Message]:
        resp = await self.client.receive_message(
            QueueUrl=self.dlq_url if dlq else self.url,
            MaxNumberOfMessages=max(1, min(10, max_messages)),
            WaitTimeSeconds=self.s.sqs_wait_seconds if wait is None else wait,
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
        )
        out = []
        for m in resp.get("Messages", []):
            try:
                body = json.loads(m["Body"])
            except json.JSONDecodeError:
                body = {"type": "invalid", "raw": m["Body"]}
            out.append(
                Message(
                    message_id=m["MessageId"],
                    receipt=m["ReceiptHandle"],
                    body=body,
                    receive_count=int(m.get("Attributes", {}).get("ApproximateReceiveCount", 1)),
                )
            )
        return out

    async def delete(self, msg: Message, dlq: bool = False) -> None:
        try:
            await self.client.delete_message(
                QueueUrl=self.dlq_url if dlq else self.url, ReceiptHandle=msg.receipt
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in _INVALID_RECEIPT:
                raise

    async def change_visibility(self, msg: Message, seconds: int) -> None:
        try:
            await self.client.change_message_visibility(
                QueueUrl=self.url, ReceiptHandle=msg.receipt, VisibilityTimeout=max(0, int(seconds))
            )
        except ClientError as exc:
            # The message was already redelivered to someone else: nothing to do.
            if exc.response["Error"]["Code"] not in _INVALID_RECEIPT:
                raise

    # ---- observability -----------------------------------------------------------------------
    async def depth(self, dlq: bool = False) -> dict[str, int]:
        attrs = (
            await self.client.get_queue_attributes(
                QueueUrl=self.dlq_url if dlq else self.url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            )
        )["Attributes"]
        return {
            "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
            "in_flight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
            "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
        }


TRANSIENT = (ClientError, BotoCoreError, OSError, asyncio.TimeoutError)
