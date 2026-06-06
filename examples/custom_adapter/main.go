package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/rjpruitt16/aquifer"
)

type JSONLineAdapter struct {
	in  *os.File
	out *os.File
}

func (a *JSONLineAdapter) Name() string {
	return "jsonl-example"
}

func (a *JSONLineAdapter) Start(ctx context.Context, app *aquifer.Aquifer) error {
	scanner := bufio.NewScanner(a.in)
	encoder := json.NewEncoder(a.out)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		var req Request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			writeError(encoder, err)
			continue
		}

		switch req.Method {
		case "enqueue":
			var jobReq aquifer.JobRequest
			if err := json.Unmarshal(req.Params, &jobReq); err != nil {
				writeError(encoder, err)
				continue
			}
			result, err := app.Enqueue(jobReq)
			writeResult(encoder, result, err)

		case "get_job":
			var params struct {
				JobID string `json:"job_id"`
			}
			if err := json.Unmarshal(req.Params, &params); err != nil {
				writeError(encoder, err)
				continue
			}
			if params.JobID == "" {
				writeError(encoder, errors.New("job_id is required"))
				continue
			}
			job, err := app.GetJob(params.JobID)
			writeResult(encoder, job, err)

		default:
			writeError(encoder, errors.New("unknown method"))
		}
	}

	return scanner.Err()
}

type Request struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type Response struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func writeResult(encoder *json.Encoder, result any, err error) {
	if err != nil {
		writeError(encoder, err)
		return
	}
	encoder.Encode(Response{Result: result})
}

func writeError(encoder *json.Encoder, err error) {
	encoder.Encode(Response{Error: err.Error()})
}

func main() {
	adapter := &JSONLineAdapter{in: os.Stdin, out: os.Stdout}
	if err := aquifer.RunAdapter(context.Background(), adapter, aquifer.RuntimeOptions{
		DBPath: os.Getenv("DB_PATH"),
	}); err != nil {
		log.Fatal(err)
	}
}
