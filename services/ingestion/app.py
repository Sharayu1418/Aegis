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
    Generates a deterministic SHA-256 hash.
    Used for idempotency and error fingerprinting.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_error(error_type: str, message: str) -> str:
    """
    Normalizes error data so semantically identical errors
    always produce the same fingerprint.
    """
    base = f"{error_type}:{message}".lower()
    base = base.replace("\n", " ").strip()
    return base


def handler(event, context):
    """
    Aegis v3:
    - Ingests incident events from SQS
    - Generates error fingerprints
    - Correlates retries/noisy failures
    - Ensures idempotent processing using DynamoDB
    """

    # ----------------------------
    # Parse SQS payload
    # ----------------------------
    payload = event
    if isinstance(event, dict) and "Records" in event and event["Records"]:
        body = event["Records"][0].get("body", "")
        try:
            payload = json.loads(body)
        except Exception:
            payload = {"raw_body": body}

    alarm = payload.get("alarm", "unknown")
    severity = payload.get("severity", "UNKNOWN")

    error_type = payload.get("error_type", "UnknownError")
    error_message = payload.get("error_message", "unknown")

    # ----------------------------
    # Error fingerprinting (v3)
    # ----------------------------
    normalized_error = normalize_error(error_type, error_message)
    fingerprint = stable_hash(normalized_error)

    pk = f"INCIDENT#{fingerprint}"
    sk = "METADATA"

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    incident_id = str(uuid.uuid4())

    item = {
        "pk": pk,
        "sk": sk,
        "incident_id": incident_id,
        "status": "OPEN",
        "alarm": alarm,
        "severity": severity,
        "error_type": error_type,
        "error_message": error_message,
        "error_fingerprint": fingerprint,
        "created_at": now,
        "last_seen_at": now,
    }

    try:
        # ----------------------------
        # Create new incident (first occurrence)
        # ----------------------------
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)"
        )

        logger.info({
            "message": "New incident created",
            "incident_id": incident_id,
            "fingerprint": fingerprint,
            "severity": severity,
            "error_type": error_type
        })

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # ----------------------------
            # Duplicate / correlated error
            # ----------------------------
            table.update_item(
                Key={"pk": pk, "sk": sk},
                UpdateExpression="SET last_seen_at = :ts",
                ExpressionAttributeValues={":ts": now}
            )

            logger.info({
                "message": "Correlated incident detected; updated last_seen_at",
                "fingerprint": fingerprint,
                "severity": severity,
                "error_type": error_type
            })
        else:
            logger.exception("Unexpected DynamoDB error")
            raise

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "processed"})
    }
