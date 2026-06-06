package aquifer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestMCPStdioAdapterListsToolsAndResourceTemplates(t *testing.T) {
	aquifer := testAquifer(t)
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
		`{"jsonrpc":"2.0","id":3,"method":"resources/templates/list","params":{}}`,
	}, "\n") + "\n"

	var output bytes.Buffer
	adapter := NewMCPStdioAdapter(strings.NewReader(input), &output)
	if err := adapter.Start(context.Background(), aquifer); err != nil {
		t.Fatalf("adapter.Start: %v", err)
	}

	responses := decodeMCPResponses(t, output.String())
	if len(responses) != 3 {
		t.Fatalf("expected 3 responses, got %d", len(responses))
	}

	if responses[0]["error"] != nil {
		t.Fatalf("initialize returned error: %v", responses[0]["error"])
	}

	toolsResult := responses[1]["result"].(map[string]any)
	tools := toolsResult["tools"].([]any)
	if len(tools) != 5 {
		t.Fatalf("expected 5 tools, got %d", len(tools))
	}

	templatesResult := responses[2]["result"].(map[string]any)
	templates := templatesResult["resourceTemplates"].([]any)
	if len(templates) != 1 {
		t.Fatalf("expected 1 resource template, got %d", len(templates))
	}
	template := templates[0].(map[string]any)
	if template["uriTemplate"] != "aquifer://jobs/{job_id}" {
		t.Fatalf("unexpected uri template: %v", template["uriTemplate"])
	}
}

func TestMCPReadJobResource(t *testing.T) {
	aquifer := testAquifer(t)
	req := JobRequest{
		UserID:        "agent-1",
		IdempotentKey: "job-resource-test",
		URL:           "http://127.0.0.1:1/test",
		Method:        "GET",
		WebhookURL:    "http://127.0.0.1:1/webhook",
	}
	result, err := aquifer.Enqueue(req)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	input := `{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"aquifer://jobs/` + result.JobID + `"}}` + "\n"
	var output bytes.Buffer
	adapter := NewMCPStdioAdapter(strings.NewReader(input), &output)
	if err := adapter.Start(context.Background(), aquifer); err != nil {
		t.Fatalf("adapter.Start: %v", err)
	}

	responses := decodeMCPResponses(t, output.String())
	resourceResult := responses[0]["result"].(map[string]any)
	contents := resourceResult["contents"].([]any)
	if len(contents) != 1 {
		t.Fatalf("expected 1 resource content item, got %d", len(contents))
	}
	content := contents[0].(map[string]any)
	if content["uri"] != "aquifer://jobs/"+result.JobID {
		t.Fatalf("unexpected resource uri: %v", content["uri"])
	}

	var job Job
	if err := json.Unmarshal([]byte(content["text"].(string)), &job); err != nil {
		t.Fatalf("resource content is not a job: %v", err)
	}
	if job.ID != result.JobID {
		t.Fatalf("expected job %s, got %s", result.JobID, job.ID)
	}
}

func testAquifer(t *testing.T) *Aquifer {
	t.Helper()

	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{})
	return NewAquifer(store, registry, broker, l8)
}

func decodeMCPResponses(t *testing.T, output string) []map[string]any {
	t.Helper()

	var responses []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		var response map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &response); err != nil {
			t.Fatalf("invalid json response %q: %v", scanner.Text(), err)
		}
		responses = append(responses, response)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan responses: %v", err)
	}
	return responses
}
