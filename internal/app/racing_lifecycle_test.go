package app

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/config"
)

// TestRacingLifecyclePrimaryWinsWithoutCooldown 备用已发出时原请求先出首事件，只取消备用，不冷却任何账户。
func TestRacingLifecyclePrimaryWinsWithoutCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan racingLifecycleAttempt, 8)
		service := newRacingService(t, racingLifecycleGenerate(started))
		service.pool.SetRoutingStrategy("fill-first")
		service.pool.SetAccountOrder([]string{"a", "b", "c", "d"})
		cfg := config.Default()
		cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds = "1", 4
		service.SetFirstEventPolicy(cfg)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "primary-wins", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		primary := racingLifecycleStarted(t, started)
		if primary.accountID != "a" {
			t.Fatalf("primary=%s, want a", primary.accountID)
		}
		time.Sleep(time.Second)
		backup := racingLifecycleStarted(t, started)
		if backup.accountID != "b" {
			t.Fatalf("backup=%s, want b", backup.accountID)
		}
		primary.input <- aistudio.Event{Kind: aistudio.EventText, Text: "primary", ProviderModel: "gemini"}
		synctest.Wait()
		if first := <-events; first.Kind != aistudio.EventText || first.Text != "primary" {
			t.Fatalf("first event=%+v", first)
		}
		if backup.ctx.Err() != context.Canceled {
			t.Fatalf("backup context=%v, want canceled", backup.ctx.Err())
		}
		primary.input <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
		racingLifecycleFinished(t, events)
		synctest.Wait()
		for _, status := range service.pool.Status() {
			if _, ok := status.ShortCooldowns["playground:gemini"][aistudio.ShortCooldownRace]; ok {
				t.Errorf("primary victory cooled account %s", status.ID)
			}
			if status.ActiveRequests != 0 {
				t.Errorf("account %s leaked %d slots", status.ID, status.ActiveRequests)
			}
		}
	})
}

// TestRacingLifecycleStreamingSurvivesFirstEventDeadline 首事件解除总期限，后续长输出仍受客户端取消约束。
func TestRacingLifecycleStreamingSurvivesFirstEventDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan racingLifecycleAttempt, 8)
		service := newRacingService(t, racingLifecycleGenerate(started))
		cfg := config.Default()
		cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds = "off", 2
		service.SetFirstEventPolicy(cfg)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "long-output", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		primary := racingLifecycleStarted(t, started)
		primary.input <- aistudio.Event{Kind: aistudio.EventReasoning, Text: "thinking", ProviderModel: "gemini"}
		synctest.Wait()
		if first := <-events; first.Kind != aistudio.EventReasoning {
			t.Fatalf("first event=%+v", first)
		}
		time.Sleep(6 * time.Second)
		synctest.Wait()
		if primary.ctx.Err() != nil {
			t.Fatalf("first-event deadline canceled a stream after its first event: %v", primary.ctx.Err())
		}
		primary.input <- aistudio.Event{Kind: aistudio.EventText, Text: "still running", ProviderModel: "gemini"}
		synctest.Wait()
		if event := <-events; event.Kind != aistudio.EventText || event.Text != "still running" {
			t.Fatalf("event after total deadline=%+v", event)
		}
		cancel()
		synctest.Wait()
		if primary.ctx.Err() != context.Canceled {
			t.Fatalf("client cancellation did not stop upstream: %v", primary.ctx.Err())
		}
		for event := range events {
			if event.Kind == aistudio.EventError {
				t.Errorf("client cancellation emitted error: %v", event.Err)
			}
		}
		for _, status := range service.pool.Status() {
			if status.ActiveRequests != 0 {
				t.Errorf("client cancellation leaked account %s slot", status.ID)
			}
		}
	})
}

// TestRacingLifecyclePolicySnapshotKeepsHedgeAndCooldown 旧请求保留竞速阈值和冷却，新请求立即采用新设置。
func TestRacingLifecyclePolicySnapshotKeepsHedgeAndCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan racingLifecycleAttempt, 8)
		service := newRacingService(t, racingLifecycleGenerate(started))
		service.pool.SetRoutingStrategy("fill-first")
		service.pool.SetAccountOrder([]string{"a", "b", "c", "d"})
		cfg := config.Default()
		cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds, cfg.FirstEventRaceCooldownSeconds = "4", 30, 7
		service.SetFirstEventPolicy(cfg)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		oldEvents, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "old-policy", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		oldPrimary := racingLifecycleStarted(t, started)
		time.Sleep(time.Second)
		cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds, cfg.FirstEventRaceCooldownSeconds = "1", 3, 2
		service.SetFirstEventPolicy(cfg)
		newEvents, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "new-policy", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		newPrimary := racingLifecycleStarted(t, started)
		time.Sleep(time.Second)
		newBackup := racingLifecycleStarted(t, started)
		if newBackup.requestID != "new-policy" || newBackup.accountID == newPrimary.accountID {
			t.Fatalf("first backup=%+v, want new-policy on another account", newBackup)
		}
		newBackup.input <- aistudio.Event{Kind: aistudio.EventText, Text: "new", ProviderModel: "gemini"}
		newBackup.input <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
		racingLifecycleFinished(t, newEvents)
		synctest.Wait()
		if newPrimary.ctx.Err() != context.Canceled {
			t.Fatal("new request did not replace its waiting primary")
		}
		if cooldown := racingLifecycleCooldown(t, service.pool, newPrimary.accountID, aistudio.ShortCooldownRace); cooldown.Until.Sub(cooldown.MarkedAt) != 2*time.Second {
			t.Fatalf("new cooldown=%+v, want 2s", cooldown)
		}
		select {
		case attempt := <-started:
			t.Fatalf("old request changed to new hedge threshold: %+v", attempt)
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if oldPrimary.ctx.Err() != nil {
			t.Fatalf("old request canceled before its snapshotted threshold: %v", oldPrimary.ctx.Err())
		}
		select {
		case attempt := <-started:
			t.Fatalf("old request hedged before 4s: %+v", attempt)
		default:
		}
		time.Sleep(time.Second)
		oldBackup := racingLifecycleStarted(t, started)
		if oldBackup.requestID != "old-policy" || oldBackup.accountID == oldPrimary.accountID {
			t.Fatalf("old backup=%+v", oldBackup)
		}
		oldBackup.input <- aistudio.Event{Kind: aistudio.EventText, Text: "old", ProviderModel: "gemini"}
		oldBackup.input <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
		racingLifecycleFinished(t, oldEvents)
		synctest.Wait()
		if cooldown := racingLifecycleCooldown(t, service.pool, oldPrimary.accountID, aistudio.ShortCooldownRace); cooldown.Until.Sub(cooldown.MarkedAt) != 7*time.Second {
			t.Fatalf("old cooldown=%+v, want snapshotted 7s", cooldown)
		}
	})
}

// TestRacingLifecyclePolicySnapshotKeepsTotalDeadline 保存新期限不会重新计时或终止旧请求。
func TestRacingLifecyclePolicySnapshotKeepsTotalDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan racingLifecycleAttempt, 8)
		service := newRacingService(t, racingLifecycleGenerate(started))
		cfg := config.Default()
		cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds = "off", 8
		service.SetFirstEventPolicy(cfg)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		began := time.Now()
		oldEvents, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "old-deadline", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		oldPrimary := racingLifecycleStarted(t, started)
		time.Sleep(time.Second)
		cfg.FirstEventTimeoutSeconds = 2
		service.SetFirstEventPolicy(cfg)
		newEvents, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "new-deadline", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		newPrimary := racingLifecycleStarted(t, started)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if err := racingLifecycleFailure(t, newEvents); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("new request error=%v", err)
		}
		if newPrimary.ctx.Err() == nil || oldPrimary.ctx.Err() != nil {
			t.Fatalf("new=%v old=%v, only new should time out at 3s", newPrimary.ctx.Err(), oldPrimary.ctx.Err())
		}
		time.Sleep(4 * time.Second)
		synctest.Wait()
		if oldPrimary.ctx.Err() != nil {
			t.Fatalf("old request expired before its original deadline: %v", oldPrimary.ctx.Err())
		}
		select {
		case event := <-oldEvents:
			t.Fatalf("old request returned before 8s: %+v", event)
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if err := racingLifecycleFailure(t, oldEvents); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("old request error=%v", err)
		}
		if time.Since(began) != 8*time.Second {
			t.Fatalf("old deadline=%v, want 8s from first attempt", time.Since(began))
		}
	})
}

// TestRacingLifecycleLoweredGlobalLimitKeepsRunningBackups 下调只限制后续放行；全局满时总期限仍停止等待请求。
func TestRacingLifecycleLoweredGlobalLimitKeepsRunningBackups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan racingLifecycleAttempt, 16)
		service := newRacingService(t, racingLifecycleGenerate(started))
		service.pool.SetRoutingStrategy("fill-first")
		service.pool.SetAccountOrder([]string{"a", "b", "c", "d"})
		cfg := config.Default()
		cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds, cfg.FirstEventHedgeMaxConcurrency = "1", 20, 2
		service.SetFirstEventPolicy(cfg)
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		defer cancelFirst()
		firstEvents, err := service.Generate(firstCtx, aistudio.GenerateRequest{ID: "occupies-slot-one", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		firstPrimary := racingLifecycleStarted(t, started)
		time.Sleep(time.Second)
		firstBackup := racingLifecycleStarted(t, started)
		secondCtx, cancelSecond := context.WithCancel(context.Background())
		defer cancelSecond()
		secondEvents, err := service.Generate(secondCtx, aistudio.GenerateRequest{ID: "occupies-slot-two", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		secondPrimary := racingLifecycleStarted(t, started)
		time.Sleep(time.Second)
		secondBackup := racingLifecycleStarted(t, started)
		if firstBackup.requestID != "occupies-slot-one" || secondBackup.requestID != "occupies-slot-two" {
			t.Fatalf("backups=%+v / %+v", firstBackup, secondBackup)
		}
		cfg.FirstEventHedgeMaxConcurrency, cfg.FirstEventTimeoutSeconds = 1, 3
		service.SetFirstEventPolicy(cfg)
		blockedCtx, cancelBlocked := context.WithCancel(context.Background())
		defer cancelBlocked()
		blockedAt := time.Now()
		blockedEvents, err := service.Generate(blockedCtx, aistudio.GenerateRequest{ID: "global-full", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		blockedPrimary := racingLifecycleStarted(t, started)
		time.Sleep(time.Second)
		synctest.Wait()
		for _, attempt := range []racingLifecycleAttempt{firstPrimary, firstBackup, secondPrimary, secondBackup} {
			if attempt.ctx.Err() != nil {
				t.Fatalf("lowered limit canceled running %s/%s: %v", attempt.requestID, attempt.accountID, attempt.ctx.Err())
			}
		}
		cancelFirst()
		synctest.Wait()
		for range firstEvents {
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if secondBackup.ctx.Err() != nil {
			t.Fatalf("remaining backup canceled after lowering: %v", secondBackup.ctx.Err())
		}
		select {
		case attempt := <-started:
			t.Fatalf("lowered global limit admitted another backup while one still runs: %+v", attempt)
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if err := racingLifecycleFailure(t, blockedEvents); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("global-full request error=%v", err)
		}
		if time.Since(blockedAt) != 3*time.Second || blockedPrimary.ctx.Err() == nil {
			t.Fatalf("global-full deadline elapsed=%v upstream=%v", time.Since(blockedAt), blockedPrimary.ctx.Err())
		}
		if secondPrimary.ctx.Err() != nil || secondBackup.ctx.Err() != nil {
			t.Fatal("a different request's deadline canceled the running race")
		}
		cancelSecond()
		synctest.Wait()
		for range secondEvents {
		}
		for _, status := range service.pool.Status() {
			if status.ActiveRequests != 0 {
				t.Errorf("global admission cleanup leaked account %s slots", status.ID)
			}
		}
	})
}

// TestRacingLifecycleBackupUsesPartiallyActiveAccount B 已有一次请求但仍有空槽时，可为 A 提供备用。
func TestRacingLifecycleBackupUsesPartiallyActiveAccount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan racingLifecycleAttempt, 8)
		service := newRacingService(t, racingLifecycleGenerate(started))
		service.pool.SetRoutingStrategy("fill-first")
		service.pool.SetAccountOrder([]string{"a", "b", "c", "d"})
		holder, _, err := service.pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{
			AccountID: "b", ModelID: "gemini", Method: "generateContent", Channel: aistudio.ChannelPlayground,
		})
		if err != nil || holder == nil {
			t.Fatalf("existing B request lease=%v error=%v", holder, err)
		}
		defer holder.Release()
		cfg := config.Default()
		cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds = "1", 5
		service.SetFirstEventPolicy(cfg)
		service.pool.SetAPIKeyAccounts(map[string][]string{"partially-active-scope": {"a", "b"}})
		ctx, cancel := context.WithCancel(aistudio.ContextWithAPIKey(context.Background(), "partially-active-scope"))
		defer cancel()
		events, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "partially-active", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		primary := racingLifecycleStarted(t, started)
		if primary.accountID != "a" {
			t.Fatalf("primary=%s, want a", primary.accountID)
		}
		time.Sleep(time.Second)
		backup := racingLifecycleStarted(t, started)
		if backup.accountID != "b" {
			t.Fatalf("backup=%s, want partially active b instead of idle c/d", backup.accountID)
		}
		for _, status := range service.pool.Status() {
			if status.ID == "b" && status.ActiveRequests != 2 {
				t.Fatalf("B active=%d, want existing request plus backup", status.ActiveRequests)
			}
		}
		backup.input <- aistudio.Event{Kind: aistudio.EventText, Text: "b", ProviderModel: "gemini"}
		backup.input <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
		racingLifecycleFinished(t, events)
		synctest.Wait()
		if primary.ctx.Err() != context.Canceled {
			t.Fatal("backup did not replace primary")
		}
		for _, status := range service.pool.Status() {
			if status.ID == "b" && status.ActiveRequests != 1 {
				t.Fatalf("B active=%d after backup, existing request should remain", status.ActiveRequests)
			}
		}
	})
}

// TestRacingLifecycleBoundAccountDoesNotCrossAccounts 指定账户有空槽也不在同账户或其他账户发备用。
func TestRacingLifecycleBoundAccountDoesNotCrossAccounts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan racingLifecycleAttempt, 8)
		service := newRacingService(t, racingLifecycleGenerate(started))
		cfg := config.Default()
		cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds = "1", 3
		service.SetFirstEventPolicy(cfg)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events, err := service.Generate(ctx, aistudio.GenerateRequest{ID: "bound-account", Model: "gemini", AccountID: "b"})
		if err != nil {
			t.Fatal(err)
		}
		primary := racingLifecycleStarted(t, started)
		if primary.accountID != "b" {
			t.Fatalf("bound primary=%s, want b", primary.accountID)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case attempt := <-started:
			t.Fatalf("bound request started forbidden backup: %+v", attempt)
		default:
		}
		if primary.ctx.Err() != nil {
			t.Fatalf("bound primary ended before total deadline: %v", primary.ctx.Err())
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if err := racingLifecycleFailure(t, events); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("bound request error=%v", err)
		}
		select {
		case attempt := <-started:
			t.Fatalf("bound timeout caused cross-account retry: %+v", attempt)
		default:
		}
		for _, status := range service.pool.Status() {
			if status.ActiveRequests != 0 {
				t.Errorf("bound request leaked account %s slots", status.ID)
			}
			if len(status.ShortCooldowns) > 0 {
				t.Errorf("deadline-only failure cooled account %s: %+v", status.ID, status.ShortCooldowns)
			}
		}
	})
}

func racingLifecycleFailure(t *testing.T, events <-chan aistudio.Event) error {
	t.Helper()
	var failure error
	for event := range events {
		if event.Kind == aistudio.EventError {
			failure = event.Err
		} else {
			t.Errorf("unexpected event while waiting for timeout: %+v", event)
		}
	}
	if failure == nil {
		t.Fatal("generation closed without expected timeout error")
	}
	return failure
}

func racingLifecycleCooldown(t *testing.T, pool *aistudio.AccountPool, accountID string, kind aistudio.ShortCooldownKind) aistudio.ShortCooldownState {
	t.Helper()
	for _, status := range pool.Status() {
		if status.ID == accountID {
			if cooldown, ok := status.ShortCooldowns["playground:gemini"][kind]; ok {
				return cooldown
			}
		}
	}
	t.Fatalf("account %s has no %s cooldown", accountID, kind)
	return aistudio.ShortCooldownState{}
}

type racingLifecycleAttempt struct {
	requestID string
	accountID string
	ctx       context.Context
	input     chan aistudio.Event
}

// racingLifecycleGenerate 是离线上游事件缝；不创建 Worker、账户会话或网络服务。
func racingLifecycleGenerate(started chan<- racingLifecycleAttempt) func(context.Context, aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
	return func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
		attempt := racingLifecycleAttempt{requestID: request.ID, accountID: request.AccountID, ctx: ctx, input: make(chan aistudio.Event, 2)}
		source := make(chan aistudio.Event)
		started <- attempt
		go func() {
			defer close(source)
			for {
				select {
				case event := <-attempt.input:
					select {
					case source <- event:
					case <-ctx.Done():
						return
					}
					if event.Kind == aistudio.EventFinish || event.Kind == aistudio.EventError {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
		return source, nil
	}
}

func racingLifecycleStarted(t *testing.T, started <-chan racingLifecycleAttempt) racingLifecycleAttempt {
	t.Helper()
	synctest.Wait()
	select {
	case attempt := <-started:
		return attempt
	default:
		t.Fatal("expected an upstream attempt at the current fake time")
		return racingLifecycleAttempt{}
	}
}

func racingLifecycleFinished(t *testing.T, events <-chan aistudio.Event) {
	t.Helper()
	finished := false
	for event := range events {
		if event.Kind == aistudio.EventError {
			t.Fatalf("generation failed: %v", event.Err)
		}
		finished = finished || event.Kind == aistudio.EventFinish
	}
	if !finished {
		t.Fatal("generation closed without finish")
	}
}
