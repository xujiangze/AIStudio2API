package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// withErrorMask 在测试期间设置对外错误遮蔽设置，结束后还原
func withErrorMask(t *testing.T, enabled bool, serverMessage string) {
	t.Helper()
	previous := errorMask
	errorMask = func() (bool, string) { return enabled, serverMessage }
	t.Cleanup(func() { errorMask = previous })
}

// TestMaskResponseServerError asserts 5xx errors are replaced by the configured message
func TestMaskResponseServerError(t *testing.T) {
	withErrorMask(t, true, "")
	recorder := httptest.NewRecorder()
	writeOpenAIError(recorder, http.StatusBadGateway, "upstream_error", "AI Studio returned HTTP 403 for aistudio.google.com")
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Message != defaultErrorMaskMessage {
		t.Fatalf("expected masked message %q, got %q", defaultErrorMaskMessage, body.Error.Message)
	}
}

// TestMaskResponseServerCustomMessage asserts a custom 5xx message from settings is used
func TestMaskResponseServerCustomMessage(t *testing.T) {
	withErrorMask(t, true, "  后端维护中，请稍后重试  ")
	recorder := httptest.NewRecorder()
	writeAnthropicError(recorder, http.StatusInternalServerError, "api_error", "AI Studio stream closed before finish")
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Message != "后端维护中，请稍后重试" {
		t.Fatalf("expected custom message, got %q", body.Error.Message)
	}
}

// TestMaskResponseClientRedaction asserts 4xx messages pass through with source names redacted
func TestMaskResponseClientRedaction(t *testing.T) {
	withErrorMask(t, true, "")
	cases := []struct{ input, want string }{
		{"AI Studio 拒绝当前输入: bad words", "Upstream 拒绝当前输入: bad words"},
		{"AIStudio2API runtime lease held by another process", "Service runtime lease held by another process"},
		{"request to aistudio.google.com failed", "request to upstream service failed"},
		{"ai-studio protocol error 7", "upstream protocol error 7"},
		{"ai_studio quota exceeded", "upstream quota exceeded"},
		{"MakerSuite returned an empty catalog", "Upstream returned an empty catalog"},
		{"aistudio visit id missing", "upstream visit id missing"},
		{"x-aistudio-visit-id header required", "x-upstream-visit-id header required"},
	}
	for _, item := range cases {
		if got := maskResponseError(http.StatusBadRequest, item.input); got != item.want {
			t.Fatalf("maskResponseError(%q) = %q, want %q", item.input, got, item.want)
		}
	}
}

// TestMaskResponseClientUnrelated asserts 4xx messages without source names pass through unchanged
func TestMaskResponseClientUnrelated(t *testing.T) {
	withErrorMask(t, true, "")
	message := "model gemini-3.8-flash does not exist"
	if got := maskResponseError(http.StatusNotFound, message); got != message {
		t.Fatalf("expected unchanged message, got %q", got)
	}
}

// TestMaskResponseDisabled asserts messages pass through untouched when masking is off
func TestMaskResponseDisabled(t *testing.T) {
	withErrorMask(t, false, "")
	recorder := httptest.NewRecorder()
	writeOpenAIError(recorder, http.StatusBadGateway, "upstream_error", "AI Studio returned HTTP 403")
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body.Error.Message, "AI Studio") {
		t.Fatalf("expected original message when disabled, got %q", body.Error.Message)
	}
}

// TestMaskResponseNilSource asserts a missing settings callback keeps messages as-is
func TestMaskResponseNilSource(t *testing.T) {
	previous := errorMask
	errorMask = nil
	t.Cleanup(func() { errorMask = previous })
	message := "AI Studio rejected current input"
	if got := maskResponseError(http.StatusBadGateway, message); got != message {
		t.Fatalf("expected unchanged message, got %q", got)
	}
}

// TestMaskResponseRealCases asserts the two error shapes observed in production are handled
func TestMaskResponseRealCases(t *testing.T) {
	withErrorMask(t, true, "")
	// 4xx：透传但替换来源关键词
	if got := maskResponseError(http.StatusBadRequest, "status_code=400, AI Studio GenerateContent 返回 HTTP 400"); got != "status_code=400, Upstream GenerateContent 返回 HTTP 400" {
		t.Fatalf("4xx case = %q", got)
	}
	// 5xx：整体替换为统一文案
	if got := maskResponseError(http.StatusBadGateway, "Internal server error: AI Studio stream closed before finish"); got != defaultErrorMaskMessage {
		t.Fatalf("5xx case = %q", got)
	}
}

// TestRedactUpstreamNameSentenceStart asserts redacted words are capitalized at sentence start
func TestRedactUpstreamNameSentenceStart(t *testing.T) {
	if got := redactUpstreamName("AI Studio returned HTTP 500"); got != "Upstream returned HTTP 500" {
		t.Fatalf("got %q", got)
	}
	if got := redactUpstreamName("failed: AI Studio returned HTTP 500"); got != "failed: Upstream returned HTTP 500" {
		t.Fatalf("got %q", got)
	}
}

// TestMaskResponseSSEErrorFrame asserts in-stream OpenAI error frames are masked too
func TestMaskResponseSSEErrorFrame(t *testing.T) {
	withErrorMask(t, true, "")
	recorder := httptest.NewRecorder()
	writeOpenAISSEError(recorder, errors.New("AI Studio stream closed before finish"))
	payload := recorder.Body.String()
	if !strings.Contains(payload, defaultErrorMaskMessage) {
		t.Fatalf("expected masked message in SSE frame, got %q", payload)
	}
	if strings.Contains(strings.ToLower(payload), "aistudio") || strings.Contains(payload, "AI Studio") {
		t.Fatalf("SSE frame still leaks the source name: %q", payload)
	}
}
