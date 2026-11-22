package app

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/api"
	"github.com/Mag1cFall/AIStudio2API/internal/config"
)

type recencyTestService struct {
	*racingService
}

func (*recencyTestService) CopyFileReferencesToLease(_ context.Context, _ *aistudio.AccountLease, contents []aistudio.Content) ([]aistudio.Content, *aistudio.TemporaryFileCopies, error) {
	return contents, &aistudio.TemporaryFileCopies{}, nil
}

func newRoutingRecencyService(t *testing.T, warmIDs []string, generate func(context.Context, aistudio.GenerateRequest) (<-chan aistudio.Event, error)) *trackedService {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	models := []aistudio.Model{
		{ID: "gemini", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}},
		{ID: "other", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}},
	}
	accounts := make([]*aistudio.Account, 0, 4)
	for _, id := range []string{"a", "b", "c", "d"} {
		accounts = append(accounts, &aistudio.Account{
			ID:     id,
			Config: aistudio.AccountConfig{Enabled: true, Label: id},
			State:  aistudio.AccountReady,
			Models: models,
		})
	}
	pool := aistudio.NewAccountPool(accounts, 2)
	pool.SetRoutingStrategy(aistudio.RoutingStrategyLeastRecent)
	pool.SetRoutingRecencyWindow(5 * time.Minute)
	workers := &accountWorkerManager{
		pool:     pool,
		accounts: make(map[string]*accountWorker),
		dispatch: newDispatchQueue(),
		readyIDs: func() []string { return slices.Clone(warmIDs) },
	}
	workers.maxActive.Store(4)
	service := newTrackedService(ctx, &recencyTestService{&racingService{models: models, generate: generate}}, pool, newRequestRegistry(ctx), workers, time.Hour)
	service.models = models
	service.dataContext = ctx
	service.state.Store(serviceRunning)
	cfg := config.Default()
	cfg.FirstEventHedge = "off"
	service.SetFirstEventPolicy(cfg)
	return service
}

func runSimpleGenerate(t *testing.T, service *trackedService, ctx context.Context, request aistudio.GenerateRequest) {
	t.Helper()
	events, err := service.Generate(ctx, request)
	if err != nil {
		t.Fatalf("Generate(%s): %v", request.ID, err)
	}
	for event := range events {
		if event.Err != nil {
			t.Fatalf("Generate(%s) event err: %v", request.ID, event.Err)
		}
	}
}

// TestGenerateLeastRecentWarmOnlyNoStandbyColdStart 验证近期均衡只在已就绪常驻池内均分，不因降权冷启动待命账户。
func TestGenerateLeastRecentWarmOnlyNoStandbyColdStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		picked := make([]string, 0, 6)
		service := newRoutingRecencyService(t, []string{"a", "b"}, func(_ context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			mu.Lock()
			picked = append(picked, request.AccountID)
			mu.Unlock()
			events := make(chan aistudio.Event, 2)
			events <- aistudio.Event{Kind: aistudio.EventText, Text: "ok"}
			events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
			close(events)
			return events, nil
		})

		models := []string{"gemini", "other", "gemini", "other", "gemini", "other"}
		for i, modelID := range models {
			runSimpleGenerate(t, service, context.Background(), aistudio.GenerateRequest{ID: "warm-" + string(rune('a'+i)), Model: modelID})
			time.Sleep(time.Second)
		}
		want := []string{"a", "b", "a", "b", "a", "b"}
		if !slices.Equal(picked, want) {
			t.Fatalf("常驻池近期均衡分配=%v，期望 %v（不应拉起待命 c/d）", picked, want)
		}
	})
}

// TestGenerateLeastRecentStickyUnaffectedAndCounted 验证显式账号绑定与内联附件亲和在 Generate 链路下不受降权影响，且后续普通新请求避开该账号。
func TestGenerateLeastRecentStickyUnaffectedAndCounted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		picked := make([]string, 0, 8)
		service := newRoutingRecencyService(t, []string{"a", "b", "c"}, func(_ context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			mu.Lock()
			picked = append(picked, request.AccountID)
			mu.Unlock()
			events := make(chan aistudio.Event, 2)
			events <- aistudio.Event{Kind: aistudio.EventText, Text: "ok"}
			events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
			close(events)
			return events, nil
		})

		// 为 a 绑定一个 Drive 文件，验证资源绑定请求不受降权影响
		if err := service.pool.BindResourceKind("drive-file-a", "a", "drive-file"); err != nil {
			t.Fatal(err)
		}

		// 1-2: 显式指定账户 a
		for range 2 {
			ctx := aistudio.ContextWithAccount(context.Background(), "a")
			runSimpleGenerate(t, service, ctx, aistudio.GenerateRequest{ID: "pin-a", Model: "gemini"})
			time.Sleep(time.Second)
		}
		// 3: 引用已绑定到 a 的文件（此时 a 已有 2 次记录，仍应命中 a）
		runSimpleGenerate(t, service, context.Background(), aistudio.GenerateRequest{
			ID:    "file-a",
			Model: "gemini",
			Contents: []aistudio.Content{{
				Role:  "user",
				Parts: []aistudio.Part{{File: &aistudio.FileRef{ID: "drive-file-a", MIME: "text/plain"}}},
			}},
		})
		time.Sleep(time.Second)

		// 4-7: 4 笔普通新请求，应避开已累计 3 次的 a，在 b/c 间均分
		for i := range 4 {
			runSimpleGenerate(t, service, context.Background(), aistudio.GenerateRequest{ID: "new-" + string(rune('0'+i)), Model: "gemini"})
			time.Sleep(time.Second)
		}
		want := []string{"a", "a", "a", "b", "c", "b", "c"}
		if !slices.Equal(picked, want) {
			t.Fatalf("绑定与新请求分配=%v，期望 %v", picked, want)
		}
	})
}

// TestGenerateLeastRecentPrefersIdleWarmAccountOverInFlight 验证 Generate 链路下空闲常驻账号优先于正在流式输出的账号。
func TestGenerateLeastRecentPrefersIdleWarmAccountOverInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		releaseFirst := make(chan struct{})
		firstStarted := make(chan string, 1)
		secondPicked := make(chan string, 1)
		var callCount int
		var mu sync.Mutex
		service := newRoutingRecencyService(t, []string{"a", "b"}, func(ctx context.Context, request aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
			mu.Lock()
			callCount++
			n := callCount
			mu.Unlock()
			events := make(chan aistudio.Event, 2)
			if n <= 2 {
				events <- aistudio.Event{Kind: aistudio.EventText, Text: "warmup"}
				events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
				close(events)
				return events, nil
			}
			if n == 3 {
				if lease, ok := aistudio.AccountLeaseFromContext(ctx); ok && lease != nil {
					lease.ProofDone()
				}
				firstStarted <- request.AccountID
				go func() {
					events <- aistudio.Event{Kind: aistudio.EventText, Text: "first"}
					<-releaseFirst
					events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
					close(events)
				}()
				return events, nil
			}
			secondPicked <- request.AccountID
			events <- aistudio.Event{Kind: aistudio.EventText, Text: "second"}
			events <- aistudio.Event{Kind: aistudio.EventFinish, FinishReason: "STOP"}
			close(events)
			return events, nil
		})

		// 先让 b 跑完 2 次请求（b 计数=2、active=0）
		for range 2 {
			runSimpleGenerate(t, service, aistudio.ContextWithAccount(context.Background(), "b"), aistudio.GenerateRequest{ID: "warm-b", Model: "gemini"})
			time.Sleep(time.Second)
		}

		// 第 3 笔未绑定请求会选中 a（active=1 且已完成 proof），并保持流打开
		firstEvents, err := service.Generate(context.Background(), aistudio.GenerateRequest{ID: "inflight-a", Model: "gemini"})
		if err != nil {
			t.Fatal(err)
		}
		if got := <-firstStarted; got != "a" {
			t.Fatalf("在途请求应命中 a，实际=%s", got)
		}
		<-firstEvents

		// 第 4 笔未绑定请求在 a 仍有在途流时应选中完全空闲的 b
		runSimpleGenerate(t, service, context.Background(), aistudio.GenerateRequest{ID: "concurrent-new", Model: "gemini"})
		if got := <-secondPicked; got != "b" {
			t.Fatalf("a 在途时新请求应选空闲的 b，实际=%s", got)
		}
		close(releaseFirst)
		for range firstEvents {
		}
	})
}

// TestRoutingRecencyAdminConfigHotReload 验证管理接口保存 least-recent 与时间窗即时作用于选号池、不触发重启且拒绝非法值与尊重环境变量锁。
func TestRoutingRecencyAdminConfigHotReload(t *testing.T) {
	accounts := make([]*aistudio.Account, 0, 2)
	for _, id := range []string{"a", "b"} {
		accounts = append(accounts, &aistudio.Account{
			ID:     id,
			Config: aistudio.AccountConfig{Enabled: true, Label: id},
			State:  aistudio.AccountReady,
			Models: []aistudio.Model{{ID: "gemini", Methods: []string{"generateContent"}}},
		})
	}
	path := t.TempDir() + "/.env"
	cfg := config.Default()
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pool := aistudio.NewAccountPool(accounts, 2)
	pool.SetRoutingStrategy(cfg.RoutingStrategy)
	pool.SetRoutingRecencyWindow(time.Duration(cfg.RoutingRecencyWindowSeconds) * time.Second)
	requests := newRequestRegistry(ctx)
	admin := &runtimeAdmin{configPath: path, pool: pool, store: &aistudio.AccountStore{}, requests: requests, service: &trackedService{pool: pool}}
	manager := &runtimeManager{
		lifecycle: ctx, configPath: path, activeManagement: cfg, requests: requests, logs: &logOutput{},
		current: &runtimeGeneration{service: &firstEventPolicyRecorder{}, admin: admin, config: cfg},
	}
	handler := api.NewHandler(manager, api.Config{Admin: manager})

	// 默认 GET /api/config 返回 round-robin 与 300 秒
	getResp := firstEventConfigExchange(handler, http.MethodGet, "")
	var current api.RuntimeConfig
	if getResp.Code != http.StatusOK || json.Unmarshal(getResp.Body.Bytes(), &current) != nil {
		t.Fatalf("GET /api/config=%d %s", getResp.Code, getResp.Body)
	}
	if current.RoutingStrategy != "round-robin" || current.RoutingRecencyWindowSeconds != 300 {
		t.Fatalf("默认路由配置=%s", getResp.Body)
	}

	// PUT 切换到 least-recent + 120 秒：立即生效、不要求重启
	current.RoutingStrategy = "least-recent"
	current.RoutingRecencyWindowSeconds = 120
	raw, _ := json.Marshal(current)
	putResp := firstEventConfigExchange(handler, http.MethodPut, string(raw))
	if putResp.Code != http.StatusOK {
		t.Fatalf("PUT /api/config=%d %s", putResp.Code, putResp.Body)
	}
	var updated api.RuntimeConfig
	if err := json.Unmarshal(putResp.Body.Bytes(), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.RoutingStrategy != "least-recent" || updated.RoutingRecencyWindowSeconds != 120 ||
		updated.ServiceRestartRequired || updated.ManagementRestartRequired {
		t.Fatalf("热更新结果异常: %+v", updated)
	}

	// 立即在池上验证已切到 least-recent：先让 a 显式跑 2 次，再发起 2 次新请求应全落到 b
	for range 2 {
		lease, _, err := pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{AccountID: "a", ModelID: "gemini"})
		if err != nil || lease == nil {
			t.Fatalf("显式租约 a 失败: %v", err)
		}
		_ = lease.Release()
	}
	for i := range 2 {
		lease, _, err := pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{ModelID: "gemini"})
		if err != nil || lease == nil {
			t.Fatalf("热更新后第 %d 次选号失败: %v", i+1, err)
		}
		if lease.Account().ID != "b" {
			t.Fatalf("热更新为 least-recent 后第 %d 次应选 b，实际=%s", i+1, lease.Account().ID)
		}
		_ = lease.Release()
	}

	// 非法策略与负时间窗被 400 invalid_config 拒绝且不改写 .env
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*api.RuntimeConfig){
		func(c *api.RuntimeConfig) { c.RoutingStrategy = "bogus" },
		func(c *api.RuntimeConfig) { c.RoutingRecencyWindowSeconds = -5 },
	} {
		bad := updated
		mutate(&bad)
		payload, _ := json.Marshal(bad)
		resp := firstEventConfigExchange(handler, http.MethodPut, string(payload))
		if resp.Code != http.StatusBadRequest || !strings.Contains(resp.Body.String(), `"code":"invalid_config"`) {
			t.Fatalf("非法配置响应=%d %s", resp.Code, resp.Body)
		}
		after, err := os.ReadFile(path)
		if err != nil || string(after) != string(before) {
			t.Fatalf("非法保存修改了 .env")
		}
	}

	// 环境变量锁定时 PUT 不能覆盖锁定项
	t.Setenv("ROUTING_STRATEGY", "least-recent")
	t.Setenv("ROUTING_RECENCY_WINDOW_SECONDS", "45")
	attempt := updated
	attempt.RoutingStrategy = "fill-first"
	attempt.RoutingRecencyWindowSeconds = 900
	payload, _ := json.Marshal(attempt)
	resp := firstEventConfigExchange(handler, http.MethodPut, string(payload))
	if resp.Code != http.StatusOK {
		t.Fatalf("环境锁保存响应=%d %s", resp.Code, resp.Body)
	}
	var locked api.RuntimeConfig
	if err := json.Unmarshal(resp.Body.Bytes(), &locked); err != nil {
		t.Fatal(err)
	}
	if locked.RoutingStrategy != "least-recent" || locked.RoutingRecencyWindowSeconds != 45 ||
		!slices.Contains(locked.Environment, "ROUTING_STRATEGY") || !slices.Contains(locked.Environment, "ROUTING_RECENCY_WINDOW_SECONDS") {
		t.Fatalf("环境锁定未保持: %+v", locked)
	}
}
