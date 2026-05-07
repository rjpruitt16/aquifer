POST http://localhost:8080/jobs
Content-Type: application/json
{
  "user_id": "load-user",
  "idempotent_key": "load-{{newUuid}}",
  "url": "http://localhost:9000/target",
  "method": "POST",
  "body": "{\"load\":\"test\"}",
  "headers": {"Content-Type": "application/json"},
  "webhook_url": "http://localhost:9000/webhook"
}

HTTP 201
[Asserts]
jsonpath "$.status" == "queued"
jsonpath "$.duplicate" not exists
