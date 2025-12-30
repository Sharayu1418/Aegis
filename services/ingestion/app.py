import json
import uuid
import datetime
import logging
import hashlib
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB setup
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("INCIDENTS_TABLE_NAME", "aegis-incidents")
table = dynamodb.Table(TABLE_NAME)


def stable_hash(value: str) -> str:
    """
    Generates a deterministic hash used for idempotency.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def handler(event, context):
    """
    Ingests incident events from SQS and ensures idempotent processing
    using DynamoDB conditional writes.
    """

    # --- Parse SQS payload ---
    payload = event
    if isinstance(event, dict) and "Records" in event and event["Records"]:
        body = event["Records"][0].get("body", "")
        try:
            payload = json.loads(body)
        except Exception:
            payload = {"raw_body": body}

    alarm = payload.get("alarm", "unknown")
    severity = payload.get("severity", "UNKNOWN")

    # --- Idempotency key ---
    event_hash = stable_hash(f"{alarm}:{severity}")
    pk = f"INCIDENT#{event_hash}"
    sk = "METADATA"

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    incident_id = str(uuid.uuid4())

    item = {
        "pk": pk,
        "sk": sk,
        "incident_id": incident_id,
        "status": "OPEN",
        "source": "cloudwatch_alarm",
        "severity": severity,
        "created_at": now,
        "last_seen_at": now,
        "event_hash": event_hash,
    }

    try:
        # --- Create incident ONLY if it doesn't already exist ---
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)"
        )

        logger.info({
            "message": "New incident created",
            "incident_id": incident_id,
            "pk": pk,
            "severity": severity
        })

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # --- Duplicate event detected ---
            table.update_item(
                Key={"pk": pk, "sk": sk},
                UpdateExpression="SET last_seen_at = :ts",
                ExpressionAttributeValues={":ts": now}
            )

            logger.info({
                "message": "Duplicate incident detected; updated last_seen_at",
                "pk": pk,
                "severity": severity
            })
        else:
            logger.exception("Unexpected DynamoDB error")
            raise

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "processed"})
    }
