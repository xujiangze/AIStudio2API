package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/api"
	"github.com/Mag1cFall/AIStudio2API/internal/config"
)

// TestFirstEventArrivalDisarmsBeforeArbitration 首事件接收点立即撤销计时，不依赖仲裁线程调度
func TestFirstEventArrivalDisarmsBeforeArbitration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		guard, cancel := newFirstEventContext(context.Background(), time.Second)
		defer cancel()
		guard.arm()
		ctx := aistudio.ContextWithAccountLease(guard, nil)
		events := make(chan aistudio.Event, 1)
		events <- aistudio.Event{Kind: aistudio.EventReasoning, Text: "thinking"}
		first, err := firstGenerateEvent(ctx, events, func(time.Duration) {})
		if err != nil || first.Kind != aistudio.EventReasoning {
			t.Fatalf("first=%+v err=%v", first, err)
		}
		time.Sleep(3 * time.Second)
		if guard.Err() != nil {
			t.Fatalf("arrived event lost to timer: %v", guard.Err())
		}
	})
}

// TestGenerateFirstEventAcrossAccountSwitch 真实换号仍使用第一次尝试的首事件总期限
func TestGenerateFirstEventAcrossAccountSwitch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			events := make(chan aistudio.Event, 1)
			if request.AccountID == "a" {
				go func() {
					time.Sleep(2 * time.Second)
					events <- aistudio.Event{Kind: aistudio.EventError, Err: &aistudio.RPCError{StatusCode: 502, Code: 13, Message: "temporary"}}
					close(events)
				}()
			} else {
				go func() { <-ctx.Done(); close(events) }()
			}
			return events, nil
		})
		cfg := config.Default()
		cfg.FirstEventHedge = "off"
		cfg.FirstEventTimeoutSeconds = 3
		service.SetFirstEventPolicy(cfg)
		started := time.Now()
		events, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "switch-deadline", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		var failure error
		for event := range events {
			if event.Err != nil {
				failure = event.Err
			}
		}
		if !errors.Is(failure, context.DeadlineExceeded) || time.Since(started) != 3*time.Second {
			t.Fatalf("wait=%v err=%v", time.Since(started), failure)
		}
	})
}

// TestFirstEventTimeoutHTTP 通过真实公开路由验证超时状态和已有错误遮蔽不冲突
func TestFirstEventTimeoutHTTP(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			events := make(chan aistudio.Event)
			go func() { <-ctx.Done(); close(events) }()
			return events, nil
		})
		cfg := config.Default()
		cfg.FirstEventHedge = "off"
		cfg.FirstEventTimeoutSeconds = 1
		service.SetFirstEventPolicy(cfg)
		handler := api.NewHandler(service, api.Config{ErrorMask: func() (bool, string) { return true, "Upstream service error" }})
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gemini","messages":[{"role":"user","content":"hi"}],"stream":false}`))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusGatewayTimeout {
			t.Fatalf("status=%d body=%s", response.Code, response.Body)
		}
		var body struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Error.Message != "Upstream service error" {
			t.Fatalf("unmasked error=%q", body.Error.Message)
		}
	})
}
