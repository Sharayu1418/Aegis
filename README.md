# Aegis

A serverless incident ingestion service: it reads error events off an SQS queue and collapses repeated occurrences of the same failure into a single DynamoDB incident record.

`Python 3.11` `AWS Lambda` `Amazon SQS` `DynamoDB` `AWS SAM` `boto3`

> Status: early. One Lambda (the ingestion path), one SAM template, no tests. The queue-to-table wiring described below is what the code does today; the rest of an incident platform — alerting, notification, status transitions — is not built yet.

---

## What it does

An SQS message arrives carrying an alarm name, a severity, and an error type and message. `services/ingestion/app.py` normalizes the error, hashes it into a fingerprint, and tries to write a new incident row keyed on that fingerprint. If a row already exists, the write is rejected and the function instead bumps `last_seen_at` on the existing incident. The result is one row per distinct failure, not one row per event, no matter how many times a broken service retries.

Every incident row carries `pk`, `sk`, `incident_id`, `status`, `alarm`, `severity`, `error_type`, `error_message`, `error_fingerprint`, `created_at`, and `last_seen_at`.

## Architecture

```
   producer                SQS                     Lambda                    DynamoDB
  ──────────           ───────────          ────────────────────        ──────────────────
  incident      ──▶   aegis-incident  ──▶   aegis-ingestion       ──▶   pk = INCIDENT#<fp>
  event JSON          -queue                app.handler                 sk = METADATA
                      (visibility 30s)      │
                                            ├─ normalize_error()   "type:message", lowered
                                            ├─ stable_hash()       SHA-256 → fingerprint
                                            │
                                            ├─ put_item + condition ─▶ new incident (OPEN)
                                            └─ on condition failure ─▶ update last_seen_at
```

| Component | Where | Role |
|---|---|---|
| `IncidentQueue` | `template.yaml` | SQS queue `aegis-incident-queue`, 30s visibility timeout |
| `IncidentIngestionFunction` | `template.yaml` | `aegis-ingestion`, python3.11, 128 MB, 10s timeout, SQS event source |
| `handler` | `services/ingestion/app.py` | Parses the record, fingerprints, writes or correlates |
| `normalize_error` / `stable_hash` | `services/ingestion/app.py` | Fingerprint derivation |
| Incidents table | **not in the template** — see Limitations | Keyed `pk` / `sk`, name from `INCIDENTS_TABLE_NAME`, default `aegis-incidents` |

## The interesting part: idempotency is the database's job, not the handler's

The obvious way to deduplicate is read-then-write: query for an existing incident, and insert if you don't find one. Under SQS that is wrong. SQS is at-least-once, Lambda scales the consumer out, and two invocations processing the same failure can both read "not found" and both insert. The gap between the read and the write is the bug.

Aegis has no read. It goes straight to:

```python
table.put_item(
    Item=item,
    ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)"
)
```

DynamoDB evaluates that condition inside the write, on a single partition, so exactly one concurrent caller can win. The loser gets `ConditionalCheckFailedException`, which the handler treats not as an error but as the signal for the second code path — this failure has been seen before, so update `last_seen_at` and move on. Duplicate suppression and first-write detection come out of the same call.

The key that makes it work is the fingerprint. `normalize_error` joins `error_type` and `error_message`, lowercases, flattens newlines, and strips; `stable_hash` takes SHA-256 of that. The same failure reported by ten retries — different message IDs, different timestamps in the SQS envelope — normalizes to the same string and therefore the same `pk`. Deduplication is on what broke, not on which message said so.

## Running it

Requires the AWS SAM CLI, Python 3.11, Docker, and AWS credentials.

```bash
sam build --use-container
sam deploy --guided        # or: sam deploy   (samconfig.toml → stack aegis-dev, us-east-1)
```

The stack does not create the incidents table (see below). Create it first, keyed on `pk` (string, partition) and `sk` (string, sort), then point the function at it.

Send a test event to the deployed queue:

```bash
aws sqs send-message \
  --queue-url "$(aws cloudformation describe-stacks --stack-name aegis-dev \
      --query 'Stacks[0].Outputs[?OutputKey==`IncidentQueueUrl`].OutputValue' --output text)" \
  --message-body '{"alarm":"checkout-5xx","severity":"HIGH","error_type":"TimeoutError","error_message":"upstream timed out"}'
```

Send it twice. The first invocation logs `New incident created`; the second logs `Correlated incident detected; updated last_seen_at`.

```bash
sam logs -n IncidentIngestionFunction --stack-name aegis-dev --tail
```

## Repository map

| Path | Purpose |
|---|---|
| `services/ingestion/app.py` | The ingestion handler — fingerprinting, conditional write, correlation |
| `services/ingestion/requirements.txt` | Empty; the handler needs only `boto3` from the Lambda runtime |
| `template.yaml` | SAM template — SQS queue, Lambda, event source mapping, queue URL output |
| `samconfig.toml` | Deploy defaults: stack `aegis-dev`, region `us-east-1` |
| `events/event.json` | Leftover SAM scaffold event (API Gateway shape, not SQS) |

## Limitations

These are real and current, not hypothetical.

- **The template is incomplete.** `template.yaml` provisions the queue and the function only. There is no `AWS::DynamoDB::Table`, no `INCIDENTS_TABLE_NAME` environment variable, and the function's only policy is `AWSLambdaBasicExecutionRole`. Deployed as-is, the first `put_item` fails on access. The table and its IAM grant have to be added before the stack is useful.
- **Only the first record in a batch is processed.** The handler reads `event["Records"][0]`. SQS delivers up to 10 records per invocation; the rest are dropped and then deleted, because the function still returns 200.
- **No failure handling on the queue.** No dead-letter queue, no redrive policy, and no partial-batch-failure response. A record that raises is retried by SQS until the visibility window and receive count run out, with nowhere to land.
- **Fingerprints never expire.** There is no time window on correlation. An error seen in January and again in June is the same incident. `status` is written as `OPEN` at creation and never transitioned by any code in the repo.
- **Variable text splits incidents.** `error_message` is hashed verbatim after lowering. A message containing a request ID, a timestamp, or a row count produces a distinct fingerprint every occurrence, which is exactly the case correlation is supposed to catch.
- **`events/event.json` is stale scaffold.** It is the SAM hello-world API Gateway event, not an SQS record, so `sam local invoke` with it exercises the "no Records key" fallback rather than the real path.
- **No tests.**
