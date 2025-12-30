import json
import uuid
import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    incident_id = str(uuid.uuid4())

    incident = {
        "incident_id": incident_id,
        "status": "OPEN",
        "source": "cloudwatch_alarm",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "event": event
    }

    logger.info({
        "message": "Incident created",
        "incident_id": incident_id,
        "source": incident["source"]
    })

    return {
        "statusCode": 200,
        "body": json.dumps(incident)
    }
