# POST a new job
POST http://localhost:8080/jobs
Content-Type: application/json
{
  "user_id": "user-smoke",
  "idempotent_key": "smoke-{{newUuid}}",
  "url": "https://httpbin.org/post",
  "method": "POST",
  "body": "{\"hello\":\"world\"}",
  "headers": {"Content-Type": "application/json"},
  "webhook_url": "https://httpbin.org/post"
}

HTTP 201
[Captures]
job_id: jsonpath "$.job_id"
[Asserts]
jsonpath "$.status" == "queued"
jsonpath "$.duplicate" not exists

# GET job status
GET http://localhost:8080/jobs/{{job_id}}

HTTP 200
[Asserts]
jsonpath "$.job_id" == {{job_id}}
jsonpath "$.status" isString
jsonpath "$.url" == "https://httpbin.org/post"
jsonpath "$.method" == "POST"

# POST duplicate — same key, expect duplicate:true
POST http://localhost:8080/jobs
Content-Type: application/json
{
  "user_id": "user-smoke",
  "idempotent_key": "smoke-fixed-key",
  "url": "https://httpbin.org/post",
  "method": "POST",
  "webhook_url": "https://httpbin.org/post"
}

HTTP *

POST http://localhost:8080/jobs
Content-Type: application/json
{
  "user_id": "user-smoke",
  "idempotent_key": "smoke-fixed-key",
  "url": "https://httpbin.org/post",
  "method": "POST",
  "webhook_url": "https://httpbin.org/post"
}

HTTP 200
[Asserts]
jsonpath "$.duplicate" == true
