package app

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
	"github.com/Mag1cFall/AIStudio2API/internal/config"
)

// TestRacingBuildUnknownStreamHedgesAndTimesOut 真实协议解析确认 Build-only 流式后，启动备用并遵守首事件总期限。
func TestRacingBuildUnknownStreamHedgesAndTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, pooled, transport := newRacingBuildService(t, false)
		request := aistudio.GenerateRequest{
			ID: "build-unknown-stream", Model: "gemini-build-only",
			Contents: []aistudio.Content{{Role: aistudio.RoleUser, Parts: []aistudio.Part{{Text: "hi"}}}},
		}
		for _, id := range []string{"a", "b"} {
			if streaming, known := pooled.FirstEventStreamingMode(context.Background(), request, id, aistudio.ChannelBuild); streaming || known {
				t.Fatalf("account %s mode=(%v,%v), want unknown", id, streaming, known)
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		began := time.Now()
		events, err := service.Generate(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		primary := racingBuildEntered(t, transport)
		if primary.rpc.AccountID != "a" || primary.rpc.Method != "ProxyStreamedCall" || !primary.rpc.Streaming {
			t.Fatalf("primary=%+v, want streaming Build on a", primary.rpc)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		backup := racingBuildEntered(t, transport)
		if backup.rpc.AccountID != "b" || backup.rpc.Method != "ProxyStreamedCall" || !backup.rpc.Streaming {
			t.Fatalf("backup=%+v, want streaming Build on b", backup.rpc)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		var failure error
		for event := range events {
			if event.Kind != aistudio.EventError {
				t.Fatalf("unexpected event=%+v", event)
			}
			failure = event.Err
		}
		if !errors.Is(failure, context.DeadlineExceeded) || time.Since(began) != 3*time.Second {
			t.Fatalf("error=%v elapsed=%v, want first-event timeout at 3s", failure, time.Since(began))
		}
		synctest.Wait()
		if primary.ctx.Err() == nil || backup.ctx.Err() == nil {
			t.Fatalf("deadline did not cancel both attempts: a=%v b=%v", primary.ctx.Err(), backup.ctx.Err())
		}
		select {
		case extra := <-transport.entered:
			t.Fatalf("unexpected additional RPC=%+v", extra.rpc)
		default:
		}
		for _, status := range service.pool.Status() {
			if status.ActiveRequests != 0 || len(status.ShortCooldowns) != 0 {
				t.Errorf("timeout leaked slots or added cooldown: %+v", status)
			}
		}
	})
}

// TestRacingBuildUnknownReloadsNativeWithoutFirstEventDeadline 重载后实际为原生单次的主调用不竞速、不受首事件期限截断。
func TestRacingBuildUnknownReloadsNativeWithoutFirstEventDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, pooled, transport := newRacingBuildService(t, true)
		request := aistudio.GenerateRequest{
			ID: "build-unknown-native", Model: "gemini-build-only",
			Contents: []aistudio.Content{{Role: aistudio.RoleUser, Parts: []aistudio.Part{{Text: "hi"}}}},
		}
		if streaming, known := pooled.FirstEventStreamingMode(context.Background(), request, "a", aistudio.ChannelBuild); streaming || known {
			t.Fatalf("initial mode=(%v,%v), want unknown", streaming, known)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		began := time.Now()
		events, err := service.Generate(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		primary := racingBuildEntered(t, transport)
		if primary.rpc.AccountID != "a" || primary.rpc.Method != "ProxyUnaryCall" || primary.rpc.Streaming {
			t.Fatalf("primary=%+v, want native Build on a", primary.rpc)
		}
		if streaming, known := pooled.FirstEventStreamingMode(ctx, request, "a", aistudio.ChannelBuild); streaming || !known {
			t.Fatalf("reloaded mode=(%v,%v), want known native", streaming, known)
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if primary.ctx.Err() != nil || time.Since(began) != 5*time.Second {
			t.Fatalf("native ended at first-event deadline: err=%v elapsed=%v", primary.ctx.Err(), time.Since(began))
		}
		select {
		case extra := <-transport.entered:
			t.Fatalf("native primary launched forbidden backup RPC: %+v", extra.rpc)
		default:
		}
		select {
		case event := <-events:
			t.Fatalf("native returned before parent cancel: %+v", event)
		default:
		}
		cancel()
		synctest.Wait()
		for event := range events {
			if event.Kind == aistudio.EventError && errors.Is(event.Err, context.DeadlineExceeded) {
				t.Fatalf("native wrongly classified as first-event deadline: %v", event.Err)
			}
		}
		if primary.ctx.Err() != context.Canceled {
			t.Fatalf("parent cancellation did not stop native: %v", primary.ctx.Err())
		}
		select {
		case extra := <-transport.entered:
			t.Fatalf("native parent cancellation caused extra RPC: %+v", extra.rpc)
		default:
		}
		for _, status := range service.pool.Status() {
			if status.ActiveRequests != 0 || len(status.ShortCooldowns) != 0 {
				t.Errorf("native cancellation leaked slots or added cooldown: %+v", status)
			}
		}
	})
}

type racingBuildCall struct {
	rpc aistudio.RPCRequest
	ctx context.Context
}

type racingBuildTransport struct {
	entered chan racingBuildCall
}

func (*racingBuildTransport) Do(context.Context, aistudio.RPCRequest) (*aistudio.RPCResponse, error) {
	return nil, errors.New("unexpected unprotected RPC")
}

func (transport *racingBuildTransport) DoProtected(ctx context.Context, _ aistudio.GenerateRequest, rpc aistudio.RPCRequest) (*aistudio.RPCResponse, error) {
	transport.entered <- racingBuildCall{rpc: rpc, ctx: ctx}
	<-ctx.Done()
	return nil, ctx.Err()
}

type racingBuildPages struct {
	page camoufoxnative.PageCatalog
}

func (pages racingBuildPages) PageCatalog(context.Context, string, bool) (camoufoxnative.PageCatalog, error) {
	return pages.page, nil
}

func newRacingBuildService(t *testing.T, reloadNative bool) (*trackedService, *aistudio.PooledService, *racingBuildTransport) {
	t.Helper()
	page := func(modelID string, modes []int64) camoufoxnative.PageCatalog {
		row := make([]any, 83)
		row[0], row[3], row[5], row[6] = "models/"+modelID, modelID, 8192, 1024
		row[7], row[64], row[82] = []string{"generateContent"}, []int{1}, modes
		raw, err := json.Marshal([]any{[]any{row}})
		if err != nil {
			t.Fatal(err)
		}
		return camoufoxnative.PageCatalog{Logging: []byte(`[]`), Tier: []byte(`[2]`), Models: raw}
	}
	initial := page("unrelated", nil)
	reloaded := initial
	if reloadNative {
		reloaded = page("gemini-build-only", []int64{4})
	}
	transport := &racingBuildTransport{entered: make(chan racingBuildCall, 8)}
	client := aistudio.NewClient(aistudio.ClientOptions{Transport: transport, Protected: transport, Pages: racingBuildPages{page: reloaded}})
	accounts := make([]*aistudio.Account, 0, 2)
	for _, id := range []string{"a", "b"} {
		_, models, err := client.ApplyPageCatalog(context.Background(), id, initial)
		if err != nil {
			t.Fatal(err)
		}
		accounts = append(accounts, &aistudio.Account{
			ID: id, Config: aistudio.AccountConfig{Enabled: true, Label: id}, State: aistudio.AccountReady,
			BenefitTier: aistudio.BenefitTierUltra, Models: models,
		})
	}
	pool := aistudio.NewAccountPool(accounts, 2)
	pool.SetUpstreamChannels([]aistudio.Channel{aistudio.ChannelBuild})
	pool.SetRoutingStrategy("fill-first")
	pool.SetAccountOrder([]string{"a", "b"})
	model := aistudio.Model{ID: "gemini-build-only", Methods: []string{"generateContent"}, OutputTokenLimit: 1024, Capabilities: map[string]bool{"chat_model": true}}
	for _, id := range []string{"a", "b"} {
		if err := pool.SetBuildCatalog(id, []aistudio.Model{model}); err != nil {
			t.Fatal(err)
		}
	}
	pooled := aistudio.NewPooledService(pool, client)
	workers := &accountWorkerManager{
		pool: pool, accounts: make(map[string]*accountWorker), dispatch: newDispatchQueue(),
		readyIDs: func() []string { return []string{"a", "b"} },
	}
	workers.maxActive.Store(2)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service := newTrackedService(ctx, pooled, pool, newRequestRegistry(ctx), workers, time.Hour)
	service.models, service.dataContext = []aistudio.Model{model}, ctx
	service.state.Store(serviceRunning)
	cfg := config.Default()
	cfg.FirstEventHedge, cfg.FirstEventTimeoutSeconds = "1", 3
	service.SetFirstEventPolicy(cfg)
	return service, pooled, transport
}

func racingBuildEntered(t *testing.T, transport *racingBuildTransport) racingBuildCall {
	t.Helper()
	select {
	case call := <-transport.entered:
		return call
	default:
		t.Fatal("expected an actual protected RPC at current fake time")
		return racingBuildCall{}
	}
}
