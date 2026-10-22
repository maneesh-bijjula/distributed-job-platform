# HTTP API reference

Base URL: `http://localhost:8080` for development; `http://127.0.0.1:8088` for the standalone deployment. Use the configured HTTPS proxy for remote clients. All resource paths below begin with `/api/v1`.

## Authentication and wire format

Send one `Authorization: Bearer <key>` header. Job operations require `USER` or `OPERATOR`; administration and dead letters require `OPERATOR`. An operator's ordinary job requests still access only its own owner. `METRICS` permits Prometheus and `/auth/me` only. Keys, owner IDs and roles in request bodies cannot override the authenticated identity.

Send JSON with `Content-Type: application/json`. Mutation bodies have a **131072-byte** application limit, including whitespace and JSON escaping, enforced for both known-length and chunked requests after authorization and before parsing. The nginx example applies the same limit. Unknown properties, duplicate object properties, trailing JSON values, malformed JSON, null characters and unpaired Unicode surrogates in text/name fields are rejected. Valid surrogate pairs, including emoji, are supported. The text limit below counts UTF-16 code units; the body limit counts received bytes.

Response timestamps are UTC instants. IDs are UUID strings. Nullable fields are returned as JSON `null`. Unexpected server failures do not include stack traces or private database/handler messages. Clients should branch on HTTP status and documented fields, rather than matching error wording.

## Jobs

| Method and path | Success | Behavior |
| --- | --- | --- |
| `POST /jobs` | `202`, or `200` replay | Submit a job; optional `Idempotency-Key`; returns a Job and `Location` |
| `POST /jobs/batch` | `202`, `200`, or `207` | Submit 1–100 items with per-item keys and outcomes; details below |
| `GET /jobs/{id}` | `200` | Read an owned Job, including status and result |
| `GET /jobs` | `200` | Owned Job page; optional `status` filter |
| `POST /jobs/{id}/cancel` | `200` | Cancel queued/running work; repeated cancellation returns the cancelled Job |
| `POST /jobs/{id}/retry` | `202`, or `200` replay | Create a linked new Job from failed/cancelled work; required `Idempotency-Key` and `Location` |
| `GET /jobs/{id}/events` | `200` | Lifecycle-event page, oldest first |
| `GET /jobs/{id}/attempts` | `200` | Execution-attempt page, lowest attempt number first |

Cancel and retry accept **no body**. Another owner's job returns `404`, including when the caller is an operator. Successful/failed terminal jobs cannot be cancelled; running/queued/successful jobs cannot be manually retried (`409`). Cancellation is cooperative during computation, with database fencing preventing a cancelled attempt from publishing its result.

Submission body:

```json
{
  "type": "WORD_COUNT",
  "text": "hello distributed jobs",
  "priority": 5,
  "scheduledAt": "2030-01-01T12:00:00Z",
  "timeoutSeconds": 30,
  "retryPolicy": {
    "maxAttempts": 4,
    "initialBackoffSeconds": 1,
    "maxBackoffSeconds": 60
  }
}
```

| Field | Rules and default |
| --- | --- |
| `type` | Required: `WORD_COUNT`, `SHA256`, `UPPERCASE` |
| `text` | Required: nonblank, at most 16000 UTF-16 code units, well-formed Unicode, no NUL |
| `priority` | Integer `0..9`; default `0`; larger values dispatched first when available |
| `scheduledAt` | Optional offset timestamp in years `0001..9999`; normalized to microseconds; past timestamps are immediately eligible |
| `timeoutSeconds` | Integer `1..3600`; defaults to configured timeout (`30` initially) |
| `retryPolicy` | Optional; defaults to `4/1/60`; when supplied, all three integers are required |
| `retryPolicy.maxAttempts` | `1..25`, including abandoned attempts and the initial execution |
| `retryPolicy.initialBackoffSeconds` | `1..3600`, no larger than maximum backoff |
| `retryPolicy.maxBackoffSeconds` | `1..3600`; exponential retry delay is capped here |

The execution deadline starts at the first claim, covers subsequent retries/recovery, and does not reset when an attempt fails. Waiting for schedule/backoff occupies no worker thread. Automatic retries keep the same job ID; manual retry creates a new ID, inherits type/text/priority/timeout/policy/owner, links `retryOf`, and becomes immediately eligible.

Idempotency keys match `[A-Za-z0-9._:-]{1,128}` and are scoped to an owner across submission/manual-retry operations. A matching replay returns the original job's **current** representation with `200`; different input under the same key returns `409`. Compare normalized schedule, priority, timeout, policy and retry source as well as type/text. Legacy policy compatibility is retained for migrated jobs. A timeout/transport error after submission is ambiguous: repeat the same request with the same key. Poll `Location` with bounded exponential backoff. New jobs consume owner quota only on commit; replays are free even at the limit.

A Job contains `id`, `type`, `text`, `status`, `result`, `error`, `createdAt`, `completedAt`, `priority`, `scheduledAt`, `retryOf`, `timeoutSeconds`, `attemptCount`, `activeAttemptId`, `deadlineAt`, `retryPolicy`, `retryAt`, and `ownerId`. Status is `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, or `CANCELLED`. `result` becomes available on success:

| Type | Result and semantics |
| --- | --- |
| `WORD_COUNT` | `{"wordCount":3,"characterCount":22}`; Unicode whitespace separates words; characters count Unicode code points |
| `SHA256` | `{"sha256":"<64 lowercase hex characters>"}`; hashes the exact UTF-8 text, without normalization |
| `UPPERCASE` | `{"text":"HELLO DISTRIBUTED JOBS"}`; Java `Locale.ROOT` conversion |

The result shapes above are independent examples. Event records include `id`, `jobId`, `type`, `status`, `relatedJobId`, `occurredAt`, `attemptId`. Attempt records include `id`, `jobId`, `number`, `generation`, `workerId`, `status`, `startedAt`, `heartbeatAt`, `leaseUntil`, `deadlineAt`, `finishedAt`, `error`, `failureKind`. Claim tokens are never returned.

## Bulk submission

`POST /api/v1/jobs/batch` accepts the same authenticated job roles and owner scope:

```json
{
  "items": [
    {"idempotencyKey":"bulk-1","job":{"type":"WORD_COUNT","text":"hello jobs"}},
    {"idempotencyKey":"bulk-2","job":{"type":"SHA256","text":"hello jobs"}}
  ]
}
```

The list contains 1–100 items with distinct required keys. Each `job` follows ordinary submission validation. The entire JSON body, including all items, shares the 131072-byte cap. Structural/nested validation runs before any item is submitted; an invalid envelope returns `400` without admitting its items.

Each item uses an independent transaction, in input order. Successful items remain committed if another item fails. The response contains `items`, `created`, `replayed`, and `unsuccessful`; every item includes `idempotencyKey`, `status`, `job`, `error`, and `retryAfterSeconds` (unused fields are null). All-success responses use `202` if any item is new and `200` for all replays. Any unsuccessful item makes the envelope **`207`**; clients must inspect every item status. `unsuccessful` includes rejected, unattempted and unconfirmed items, and does not prove that a storage-ambiguous commit failed.

Per-item statuses follow ordinary submission: `202` created, `200` replay, `409` conflicting key/body, `429` owner quota with per-item `retryAfterSeconds`, and sanitized `503` storage/unconfirmed admission. Unexpected item failures use sanitized `500` and increment operational metrics. A cooperative five-second budget stops starting further item transactions; remaining items get `503`. An already-started transaction finishes normally and may exceed that budget. Owner limits are shared with single submissions and other keys/API instances; only committed new jobs consume quota.

Retry transient or unconfirmed items with the **same per-item key and body**, honoring quota delays and bounded backoff. A whole-envelope transport failure can be resolved by replaying the same items. Idempotency is per item, not a claim of whole-batch atomicity. A `207` response alone is insufficient to infer admission success. The process-local `job_platform_batch_items_total{status}` metric exposes item outcomes even when the HTTP envelope is `207`; labels are a fixed status set, never IDs or payloads. `scripts/batch-smoke.py` verifies real packaged results, replay IDs, mixed conflicts/quotas, validation and temporary-key cleanup.

## Pagination

All list endpoints accept zero-based `page` (`0..1000000`, default `0`) and `size` (`1..100`, default `20`). The response is `{"items":[],"page":0,"size":20,"hasNext":false}`. A full page alone does not imply another page exists; use `hasNext`. Job listing is newest first, ordered by creation time then ID. Dead letters and owner/key listings are oldest first, with ID tie-breaking. Access audit is ordered by audit ID. These are offset pages over changing data, so separate requests do not form a snapshot.

## Identity and administration

| Method and path | Success | Body / result |
| --- | --- | --- |
| `GET /auth/me` | `200` | `{identity:{ownerId,keyId,role,name},owner:{...}}`; `name` on identity is the owner UUID principal name |
| `POST /admin/owners` | `201` | `{name,submissionLimitPerMinute?}` → Owner with `Location` |
| `GET /admin/owners` | `200` | Owner page |
| `GET /admin/owners/{id}` | `200` | Owner |
| `POST /admin/owners/{id}/limit` | `200` | `{submissionLimitPerMinute}` → updated Owner |
| `POST /admin/owners/{id}/keys` | `201` | `{name,role?}` → `{key:{...},token:"..."}`; token returned once with `Cache-Control: no-store` |
| `GET /admin/owners/{id}/keys` | `200` | Key metadata page, including revoked keys |
| `POST /admin/keys/{id}/revoke` | `200` | No body; idempotently revoke and return metadata |
| `GET /admin/audit` | `200` | Access-audit page |

Names must be nonblank, well-formed Unicode without NUL, and at most 80 UTF-16 code units. Owner quota is `1..10000` new jobs per UTC database minute, default `120`, shared by every key/API instance for that owner. `role` defaults to `USER`; `USER`, `OPERATOR` and `METRICS` are supported. Operators are trusted account administrators and may explicitly provision any role/owner. Rotate keys by issuing a replacement, switching clients, then revoking the old key.

Owner fields: `id`, `name`, `submissionLimitPerMinute`, `createdAt`. Key metadata: `id`, `ownerId`, `name`, `role`, `prefix`, `createdAt`, `revokedAt`; full tokens and hashes are omitted. Access audit: `id`, `actorKeyId`, `action`, `ownerId`, `keyId`, `occurredAt`.

## Dead letters

| Method and path | Success | Behavior |
| --- | --- | --- |
| `GET /dead-letters` | `200` | Persisted inbox page; optional `state=OPEN`, `RECOVERED` or `DISCARDED` |
| `GET /dead-letters/{id}` | `200` | Sanitized record |
| `POST /dead-letters/collect?limit=20` | `200` | No body; move at most `1..100` broker messages into storage; `{collected,entryIds}` |
| `POST /dead-letters/{id}/recover` | `202`, or `200` replay | No body; required `Idempotency-Key`; resolved record plus recovered job `Location` |
| `POST /dead-letters/{id}/discard` | `200` | No body; required `Idempotency-Key`; resolved record |

GET requests never consume broker messages. Collection persists each record before acknowledging its message; repeated messages retain their original resolution. Records contain `id`, `source`, `reason`, `jobId`, `generation`, `payloadSha256`, `payloadBytes`, `capturedAt`, `state`, `resolvedAt`, `recoveryJobId`, `resolvedByKeyId`. Original broker bodies and private exceptions are omitted.

A current queued delivery can be redriven without resetting schedule/backoff/deadline/attempt budgets. A failed/cancelled job creates a linked child and charges the source owner's quota. Running/succeeded work, stale queued generations, expired deadlines, malformed messages and unknown job IDs cannot be recovered (`409`). Discard resolves the inbox record; it does not cancel its job. Resolution keys are **global across dead-letter actions**, separate from submission keys; replaying another action/record with the same resolution key returns `409`. Records retain the authenticated resolving key ID.

## Status handling

| Status | Client action |
| --- | --- |
| `400` | Correct malformed JSON, input, parameters, UUID or idempotency header; do not retry unchanged |
| `401` | Supply a valid active bearer key; `WWW-Authenticate: Bearer` is returned |
| `403` | Use a key with the required role |
| `404` | Resource absent or outside the caller's ownership |
| `409` | Resolve key/input or lifecycle/action conflict |
| `413` | Reduce the body to at most 131072 bytes |
| `415` | Send a supported JSON content type |
| `429` | Wait for `Retry-After` seconds, then retry with the same idempotency key |
| `503` | Storage/broker temporarily unavailable; back off and preserve the key/body when replaying admission |

Errors use `application/problem+json` with `status` and `detail` (and optional standard problem fields). Statuses rejected by the web container or an external proxy can have that layer's own error representation. Other unexpected `5xx` failures also warrant bounded retries and operator investigation; admission requires the same idempotency key to resolve uncertainty.

Public probes are `/actuator/health`, `/actuator/health/liveness` and `/actuator/health/readiness`. Readiness depends on PostgreSQL, and deliberately permits durable admission while RabbitMQ is down. `/actuator/prometheus` requires `METRICS` or `OPERATOR`; `/actuator/info` requires `OPERATOR`. See [observability](observability.md) for metrics scope and freshness.

Packaged compatibility checks: `scripts/smoke.py`, `scripts/auth-smoke.py`, `scripts/dead-letter-smoke.py`, and `scripts/api-boundary-smoke.py`. The boundary check exercises real HTTP chunked transfer and exact-limit UTF-8 input, and revokes its temporary diagnostic key on exit.
