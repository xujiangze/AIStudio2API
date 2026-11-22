package app

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
)

// TestGenerateWorkerReplayAfterHedge 原账户可恢复失败不因另一账户仍等待而丢失重放机会
func TestGenerateWorkerReplayAfterHedge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var aCalls atomic.Int64
		var bCalls atomic.Int64
		service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			events := make(chan aistudio.Event, 2)
			go func() {
				defer close(events)
				if request.AccountID == "a" && aCalls.Add(1) == 1 {
					time.Sleep(2 * time.Second)
					events <- aistudio.Event{Kind: aistudio.EventError, Err: aistudio.ErrProtectedTransport}
				} else if request.AccountID == "b" {
					bCalls.Add(1)
					time.Sleep(3 * time.Second)
					events <- aistudio.Event{Kind: aistudio.EventError, Err: &aistudio.RPCError{StatusCode: 429, Code: 8, Message: "quota limit"}}
				} else {
					events <- aistudio.Event{Kind: aistudio.EventText, Text: "recovered", ProviderModel: "gemini"}
					events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
				}
			}()
			return events, nil
		})
		service.pool.SetAPIKeyAccounts(map[string][]string{"fixture": {"a", "b"}})
		service.SetRequestPolicy(nil, 0)
		ctx := aistudio.ContextWithAPIKey(context.Background(), "fixture")
		events, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "recover-race", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		var text string
		for event := range events {
			if event.Kind == aistudio.EventError {
				t.Fatal(event.Err)
			}
			text += event.Text
		}
		if text != "recovered" || aCalls.Load() != 2 || bCalls.Load() != 1 {
			t.Fatalf("text=%q a=%d b=%d", text, aCalls.Load(), bCalls.Load())
		}
	})
}
