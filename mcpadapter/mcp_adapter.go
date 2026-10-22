// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// Package mcpadapter exposes Aquifer as an MCP server over stdio
// (AQUIFER_ADAPTER=mcp-stdio): tools to submit and read jobs, and job
// resources at aquifer://jobs/{job_id}.
package mcpadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/rjpruitt16/aquifer"
)

const mcpProtocolVersion = "2025-06-18"

type Adapter struct {
	in  io.Reader
	out io.Writer
}

func NewAdapter(in io.Reader, out io.Writer) *Adapter {
	return &Adapter{in: in, out: out}
}

func (a *Adapter) Name() string {
	return "mcp-stdio"
}

func (a *Adapter) Start(ctx context.Context, app *aquifer.Aquifer) error {
	scanner := bufio.NewScanner(a.in)
	scanner.Buffer(make([]byte, 1024), 1024*1024*16)
	encoder := json.NewEncoder(a.out)

	lines := make(chan []byte)
	scanDone := make(chan error, 1)
	go func() {
		defer close(lines)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case lines <- line:
			case <-ctx.Done():
				scanDone <- nil
				return
			}
		}
		scanDone <- scanner.Err()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case line, ok := <-lines:
			if !ok {
				return <-scanDone
			}

			var req mcpRequest
			if err := json.Unmarshal(line, &req); err != nil {
				writeMCPError(encoder, nil, -32700, "parse error")
				continue
			}

			if req.JSONRPC != "2.0" {
				writeMCPError(encoder, req.ID, -32600, "invalid request")
				continue
			}

			if req.ID == nil {
				continue
			}

			result, err := handleMCPRequest(app, req)
			if err != nil {
				var rpcErr *mcpRPCError
				if errors.As(err, &rpcErr) {
					writeMCPError(encoder, req.ID, rpcErr.Code, rpcErr.Message)
				} else {
					writeMCPError(encoder, req.ID, -32603, err.Error())
				}
				continue
			}

			encoder.Encode(mcpResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result:  result,
			})
		}
	}
}

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *mcpErrorObject `json:"error,omitempty"`
}

type mcpErrorObject struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpRPCError struct {
	Code    int
	Message string
}

func (e *mcpRPCError) Error() string {
	return e.Message
}

func writeMCPError(encoder *json.Encoder, id json.RawMessage, code int, message string) {
	encoder.Encode(mcpResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &mcpErrorObject{
			Code:    code,
			Message: message,
		},
	})
}

func handleMCPRequest(app *aquifer.Aquifer, req mcpRequest) (any, error) {
	switch req.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": mcpProtocolVersion,
			"capabilities": map[string]any{
				"tools":     map[string]any{},
				"resources": map[string]any{},
			},
			"serverInfo": map[string]any{
				"name":    "aquifer",
				"version": aquifer.Version,
			},
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return map[string]any{"tools": mcpTools()}, nil
	case "resources/list":
		return map[string]any{"resources": []any{}}, nil
	case "resources/templates/list":
		return map[string]any{"resourceTemplates": mcpResourceTemplates()}, nil
	case "resources/read":
		return readMCPResource(app, req.Params)
	case "tools/call":
		return callMCPTool(app, req.Params)
	default:
		return nil, &mcpRPCError{Code: -32601, Message: "method not found"}
	}
}

type mcpResourceRead struct {
	URI string `json:"uri"`
}

func readMCPResource(app *aquifer.Aquifer, params json.RawMessage) (any, error) {
	var req mcpResourceRead
	if err := json.Unmarshal(params, &req); err != nil || req.URI == "" {
		return nil, &mcpRPCError{Code: -32602, Message: "uri is required"}
	}

	const prefix = "aquifer://jobs/"
	if !strings.HasPrefix(req.URI, prefix) {
		return nil, &mcpRPCError{Code: -32602, Message: "unsupported resource uri"}
	}

	jobID := strings.TrimPrefix(req.URI, prefix)
	job, err := app.GetJob(jobID)
	if err != nil {
		return nil, &mcpRPCError{Code: -32000, Message: err.Error()}
	}

	text, _ := json.Marshal(job)
	return map[string]any{
		"contents": []map[string]string{
			{
				"uri":      req.URI,
				"mimeType": "application/json",
				"text":     string(text),
			},
		},
	}, nil
}

type mcpToolCall struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

func callMCPTool(app *aquifer.Aquifer, params json.RawMessage) (any, error) {
	var call mcpToolCall
	if err := json.Unmarshal(params, &call); err != nil {
		return nil, &mcpRPCError{Code: -32602, Message: "invalid tool call params"}
	}

	var result any
	var err error
	isError := false

	switch call.Name {
	case "aquifer_enqueue_job":
		var req aquifer.JobRequest
		if err := json.Unmarshal(call.Arguments, &req); err != nil {
			return nil, &mcpRPCError{Code: -32602, Message: "invalid aquifer_enqueue_job arguments"}
		}
		result, err = app.Enqueue(req)
	case "aquifer_get_job":
		var args struct {
			JobID string `json:"job_id"`
		}
		if err := json.Unmarshal(call.Arguments, &args); err != nil || args.JobID == "" {
			return nil, &mcpRPCError{Code: -32602, Message: "job_id is required"}
		}
		result, err = app.GetJob(args.JobID)
	case "aquifer_health":
		result = app.Health()
	case "aquifer_l8_metadata":
		var args struct {
			Host string `json:"host"`
		}
		json.Unmarshal(call.Arguments, &args)
		result = app.L8Metadata(args.Host)
	case "aquifer_l8_challenge":
		var challenge aquifer.L8ChallengeReq
		if err := json.Unmarshal(call.Arguments, &challenge); err != nil {
			return nil, &mcpRPCError{Code: -32602, Message: "invalid aquifer_l8_challenge arguments"}
		}
		result, err = app.HandleL8Challenge(challenge)
	default:
		return nil, &mcpRPCError{Code: -32602, Message: fmt.Sprintf("unknown tool: %s", call.Name)}
	}

	if err != nil {
		isError = true
		result = map[string]any{"error": err.Error()}
	}

	text, _ := json.Marshal(result)
	return map[string]any{
		"content": []map[string]string{
			{"type": "text", "text": string(text)},
		},
		"structuredContent": result,
		"isError":           isError,
	}, nil
}

func mcpTools() []map[string]any {
	return []map[string]any{
		{
			"name":        "aquifer_enqueue_job",
			"description": "Queue an HTTP request through Aquifer for durable, rate-controlled dispatch.",
			"inputSchema": objectSchema(map[string]any{
				"user_id":        stringSchema("Stable user, tenant, or agent identifier."),
				"idempotent_key": stringSchema("Caller-provided idempotency key scoped to user_id."),
				"url":            stringSchema("Target URL Aquifer should dispatch to. Mutually exclusive with pool_id — set exactly one."),
				"pool_id":        stringSchema("Dispatch to a registered pool by id instead of a fixed URL. Mutually exclusive with url — set exactly one."),
				"method":         stringSchema("HTTP method to use when dispatching the request."),
				"headers":        map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
				"body":           stringSchema("Optional raw request body."),
				"webhook_url":    stringSchema("Optional URL that receives the eventual job result."),
			}, []string{"user_id", "idempotent_key", "method"}),
		},
		{
			"name":        "aquifer_get_job",
			"description": "Fetch the current status and metadata for an Aquifer job.",
			"inputSchema": objectSchema(map[string]any{
				"job_id": stringSchema("Aquifer job id."),
			}, []string{"job_id"}),
		},
		{
			"name":        "aquifer_health",
			"description": "Return Aquifer health and protocol metadata.",
			"inputSchema": objectSchema(map[string]any{}, []string{}),
		},
		{
			"name":        "aquifer_l8_metadata",
			"description": "Return Aquifer L8 public key metadata for trustless webhook delivery.",
			"inputSchema": objectSchema(map[string]any{
				"host": stringSchema("Host to use when building L8 endpoint metadata. Defaults to localhost."),
			}, []string{}),
		},
		{
			"name":        "aquifer_l8_challenge",
			"description": "Answer an L8 challenge for services verifying Aquifer's webhook identity.",
			"inputSchema": objectSchema(map[string]any{
				"challenge_id":      stringSchema("Challenge id from the receiver."),
				"nonce":             stringSchema("Challenge nonce."),
				"timestamp":         map[string]any{"type": "integer"},
				"sender_public_key": stringSchema("Receiver public key."),
				"signature":         stringSchema("Receiver signature."),
			}, []string{"challenge_id", "nonce", "timestamp", "sender_public_key", "signature"}),
		},
	}
}

func mcpResourceTemplates() []map[string]any {
	return []map[string]any{
		{
			"uriTemplate": "aquifer://jobs/{job_id}",
			"name":        "Aquifer job",
			"description": "Current Aquifer job status and metadata.",
			"mimeType":    "application/json",
		},
	}
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}
}

func stringSchema(description string) map[string]any {
	return map[string]any{
		"type":        "string",
		"description": description,
	}
}
