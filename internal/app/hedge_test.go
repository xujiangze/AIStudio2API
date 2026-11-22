package app

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/config"
)

type racingService struct {
	models   []aistudio.Model
	generate func(context.Context, aistudio.GenerateRequest) (<-chan aistudio.Event, error)
}

func (service *racingService) Models(context.Context) ([]aistudio.Model, error) {
	return service.models, nil
}
func (*racingService) CountTokens(context.Context, aistudio.TokenCountRequest) (aistudio.TokenCount, error) {
	return aistudio.TokenCount{}, nil
}
func (service *racingService) Generate(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
	return service.generate(ctx, request)
}
func (service *racingService) RefreshAccountModels(context.Context, string, bool) ([]aistudio.Model, error) {
	return service.models, nil
}
func (*racingService) RefreshAccountState(context.Context, string, bool) (aistudio.AccountRefresh, error) {
	return aistudio.AccountRefresh{}, nil
}
func (*racingService) ReclaimExpiredResources(context.Context) error { return nil }
func (service *racingService) CachedModels() []aistudio.Model        { return service.models }

func newRacingService(t *testing.T, generate func(context.Context, aistudio.GenerateRequest) (<-chan aistudio.Event, error)) *trackedService {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	model := aistudio.Model{ID: "gemini", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}}
	accounts := make([]*aistudio.Account, 0, 4)
	for _, id := range []string{"a", "b", "c", "d"} {
		accounts = append(accounts, &aistudio.Account{ID: id, Config: aistudio.AccountConfig{Enabled: true, Label: id}, State: aistudio.AccountReady, Models: []aistudio.Model{model}})
	}
	pool := aistudio.NewAccountPool(accounts, 2)
	workers := &accountWorkerManager{
		pool: pool, accounts: make(map[string]*accountWorker), dispatch: newDispatchQueue(),
		readyIDs: func() []string { return []string{"a", "b", "c", "d"} },
	}
	workers.maxActive.Store(4)
	service := newTrackedService(ctx, &racingService{models: []aistudio.Model{model}, generate: generate}, pool, newRequestRegistry(ctx), workers, time.Hour)
	service.models = []aistudio.Model{model}
	service.dataContext = ctx
	service.state.Store(serviceRunning)
	cfg := config.Default()
	cfg.FirstEventHedge = "1"
	service.SetFirstEventPolicy(cfg)
	return service
}

// TestGenerateHedgeWinner 通过生成事件和账户池观察竞速胜出、取消及独立冷却
func TestGenerateHedgeWinner(t *testing.T) {
	var mu sync.Mutex
	var ids []string
	canceled := make(chan struct{})
	service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
		mu.Lock()
		ids = append(ids, request.AccountID)
		index := len(ids)
		mu.Unlock()
		events := make(chan aistudio.Event, 2)
		if index == 1 {
			go func() { <-ctx.Done(); close(events); close(canceled) }()
		} else {
			events <- aistudio.Event{Kind: aistudio.EventText, Text: "fast", ProviderModel: "gemini"}
			events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
			close(events)
		}
		return events, nil
	})
	events, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "race", Model: "gemini"})
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
	if text != "fast" {
		t.Fatalf("text=%q", text)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("losing request was not canceled")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("attempts=%v", ids)
	}
	for _, status := range service.pool.Status() {
		if status.ID != ids[0] {
			continue
		}
		cooldowns := status.ShortCooldowns[aistudio.ShortCooldownScope(aistudio.ChannelPlayground, "gemini")]
		if _, ok := cooldowns[aistudio.ShortCooldownRace]; !ok {
			t.Fatal("original account did not enter race cooldown")
		}
	}
}

// TestGenerateHedgeRefill 备用失败后补发其他账户，共用预算且不增加第三份
func TestGenerateHedgeRefill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			events := make(chan aistudio.Event, 2)
			switch calls.Add(1) {
			case 1:
				go func() { <-ctx.Done(); close(events) }()
			case 2:
				events <- aistudio.Event{Kind: aistudio.EventError, Err: &aistudio.RPCError{StatusCode: 502, Code: 13, Message: "temporary"}}
				close(events)
			default:
				events <- aistudio.Event{Kind: aistudio.EventText, Text: request.AccountID, ProviderModel: "gemini"}
				events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
				close(events)
			}
			return events, nil
		})
		events, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "refill", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		for event := range events {
			if event.Kind == aistudio.EventError {
				t.Fatal(event.Err)
			}
		}
		synctest.Wait()
		if calls.Load() != 3 {
			t.Fatalf("attempts=%d, want 3", calls.Load())
		}
		if service.hedges.Load() != 0 {
			t.Fatalf("hedge slots leaked: %d", service.hedges.Load())
		}
		for _, status := range service.pool.Status() {
			if status.ActiveRequests != 0 {
				t.Fatalf("account %s still active", status.ID)
			}
		}
	})
}

// TestGenerateHedgeBudget 非额度换号预算用尽后不再补发备用
func TestGenerateHedgeBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			events := make(chan aistudio.Event, 1)
			if calls.Add(1) == 1 {
				go func() { <-ctx.Done(); close(events) }()
			} else {
				events <- aistudio.Event{Kind: aistudio.EventError, Err: &aistudio.RPCError{StatusCode: 502, Code: 13, Message: "temporary"}}
				close(events)
			}
			return events, nil
		})
		service.SetRequestPolicy(nil, 0)
		cfg := config.Default()
		cfg.FirstEventHedge = "1"
		cfg.FirstEventTimeoutSeconds = 3
		service.SetFirstEventPolicy(cfg)
		events, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "budget", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		var failure error
		for event := range events {
			if event.Kind == aistudio.EventError {
				failure = event.Err
			}
		}
		synctest.Wait()
		if !errors.Is(failure, context.DeadlineExceeded) {
			t.Fatalf("error=%v", failure)
		}
		if calls.Load() != 2 {
			t.Fatalf("budget exceeded: attempts=%d", calls.Load())
		}
	})
}

// TestGenerateFirstEventTimeout 首事件期限同时停止原请求和备用，关闭竞速也生效
func TestGenerateFirstEventTimeout(t *testing.T) {
	for _, hedge := range []string{"off", "1"} {
		t.Run(hedge, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int64
				var canceled atomic.Int64
				service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
					calls.Add(1)
					events := make(chan aistudio.Event)
					go func() { <-ctx.Done(); canceled.Add(1); close(events) }()
					return events, nil
				})
				cfg := config.Default()
				cfg.FirstEventHedge = hedge
				cfg.FirstEventTimeoutSeconds = 3
				service.SetFirstEventPolicy(cfg)
				started := time.Now()
				events, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "timeout", Model: "gemini"})
				if err != nil {
					t.Fatal(err)
				}
				var failure error
				for event := range events {
					failure = event.Err
				}
				synctest.Wait()
				if !errors.Is(failure, context.DeadlineExceeded) {
					t.Fatalf("error=%v", failure)
				}
				if time.Since(started) != 3*time.Second {
					t.Fatalf("wait=%v", time.Since(started))
				}
				if canceled.Load() != calls.Load() {
					t.Fatalf("canceled %d of %d", canceled.Load(), calls.Load())
				}
				if service.hedges.Load() != 0 {
					t.Fatalf("leaked slots=%d", service.hedges.Load())
				}
			})
		})
	}
}

// TestGenerateBusyCooldown 关闭竞速仍写模型繁忙冷却，其他 5xx 不冒充繁忙
func TestGenerateBusyCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			events := make(chan aistudio.Event, 2)
			if calls.Add(1) == 1 {
				events <- aistudio.Event{Kind: aistudio.EventError, Err: &aistudio.RPCError{StatusCode: http.StatusServiceUnavailable, Code: 14, Message: "This model is currently experiencing high demand. Please try again later."}}
			} else {
				events <- aistudio.Event{Kind: aistudio.EventText, Text: "ok", ProviderModel: "gemini"}
				events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
			}
			close(events)
			return events, nil
		})
		cfg := config.Default()
		cfg.FirstEventHedge = "off"
		service.SetFirstEventPolicy(cfg)
		events, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "busy", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		for event := range events {
			if event.Kind == aistudio.EventError {
				t.Fatal(event.Err)
			}
		}
		synctest.Wait()
		busy := 0
		for _, status := range service.pool.Status() {
			if cooldown, ok := status.ShortCooldowns["playground:gemini"][aistudio.ShortCooldownBusy]; ok {
				busy++
				if cooldown.Until.Sub(cooldown.MarkedAt) != 10*time.Second {
					t.Fatalf("cooldown=%+v", cooldown)
				}
			}
		}
		if busy != 1 {
			t.Fatalf("busy accounts=%d", busy)
		}
	})
}

// TestGenerateBudgetAfterConcurrentQuota 最后结算的额度失败不掩盖先前已用尽的换号预算
func TestGenerateBudgetAfterConcurrentQuota(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			events := make(chan aistudio.Event, 1)
			index := calls.Add(1)
			go func() {
				defer close(events)
				if index == 1 {
					time.Sleep(2 * time.Second)
					events <- aistudio.Event{Kind: aistudio.EventError, Err: &aistudio.RPCError{StatusCode: 502, Code: 13, Message: "temporary"}}
				} else {
					time.Sleep(3 * time.Second)
					events <- aistudio.Event{Kind: aistudio.EventError, Err: &aistudio.RPCError{StatusCode: 429, Code: 8, Message: "quota limit"}}
				}
			}()
			return events, nil
		})
		service.SetRequestPolicy(nil, 0)
		events, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "budget-quota", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		for range events {
		}
		synctest.Wait()
		if calls.Load() != 2 {
			t.Fatalf("quota revived exhausted budget: %d attempts", calls.Load())
		}
	})
}

// TestGenerateHedgeSlotHeldUntilCleanup 落败上游停止前不提前归还全局备用槽
func TestGenerateHedgeSlotHeldUntilCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		releaseCleanup := make(chan struct{})
		service := newRacingService(t, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			events := make(chan aistudio.Event, 2)
			if calls.Add(1) == 1 {
				go func() { <-ctx.Done(); <-releaseCleanup; close(events) }()
			} else {
				events <- aistudio.Event{Kind: aistudio.EventText, Text: "ok", ProviderModel: "gemini"}
				events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
				close(events)
			}
			return events, nil
		})
		events, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "cleanup-slot", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		for range events {
		}
		synctest.Wait()
		if service.hedges.Load() != 1 {
			t.Fatalf("slot returned before upstream cleanup: %d", service.hedges.Load())
		}
		close(releaseCleanup)
		synctest.Wait()
		if service.hedges.Load() != 0 {
			t.Fatalf("slot not returned after cleanup: %d", service.hedges.Load())
		}
	})
}
