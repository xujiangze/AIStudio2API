package aistudio

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func newRecencyTestPool(concurrency int, channels []Channel, accountIDs ...string) *AccountPool {
	models := []Model{
		{ID: "gemini", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}},
		{ID: "other", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}},
	}
	accounts := make([]*Account, 0, len(accountIDs))
	for _, id := range accountIDs {
		accounts = append(accounts, &Account{
			ID:          id,
			Config:      AccountConfig{Enabled: true, Label: id},
			State:       AccountReady,
			Models:      models,
			buildModels: models,
			BenefitTier: BenefitTierPro,
		})
	}
	pool := NewAccountPool(accounts, concurrency)
	if len(channels) > 0 {
		pool.SetUpstreamChannels(channels)
	}
	return pool
}

func acquireAndRelease(t *testing.T, pool *AccountPool, ctx context.Context, selection AccountSelection) (string, Channel) {
	t.Helper()
	lease, _, err := pool.TryAcquireFor(ctx, selection)
	if err != nil || lease == nil {
		t.Fatalf("TryAcquireFor(%+v) = (%v, %v)", selection, lease, err)
	}
	accountID, channel := lease.Account().ID, lease.Channel()
	if err := lease.Release(); err != nil {
		t.Fatalf("Release(%s): %v", accountID, err)
	}
	return accountID, channel
}

// TestLeastRecentSpreadsNewRequestsAcrossModelsAndChannels 验证跨模型、多通道的新请求在时间窗内按账号维度均分，不连续命中同一账号。
func TestLeastRecentSpreadsNewRequestsAcrossModelsAndChannels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newRecencyTestPool(2, []Channel{ChannelPlayground, ChannelBuild}, "a", "b", "c")
		pool.SetRoutingStrategy(RoutingStrategyLeastRecent)
		pool.SetRoutingRecencyWindow(5 * time.Minute)

		models := []string{"gemini", "other", "gemini", "other", "gemini", "other"}
		got := make([]string, 0, len(models))
		for _, modelID := range models {
			id, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: modelID})
			got = append(got, id)
			time.Sleep(time.Second)
		}
		want := []string{"a", "b", "c", "a", "b", "c"}
		if !slices.Equal(got, want) {
			t.Fatalf("跨模型多通道分配=%v，期望 %v", got, want)
		}
	})
}

// TestLeastRecentPrefersIdleOverBusyActiveAccounts 验证当前空闲账户优先于已有在途请求的账户，均忙碌时再按窗口计数均衡。
func TestLeastRecentPrefersIdleOverBusyActiveAccounts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newRecencyTestPool(2, nil, "a", "b")
		pool.SetRoutingStrategy(RoutingStrategyLeastRecent)
		pool.SetRoutingRecencyWindow(5 * time.Minute)

		// 让 b 先累计 3 次历史使用并释放回空闲
		for range 3 {
			acquireAndRelease(t, pool, context.Background(), AccountSelection{AccountID: "b", ModelID: "gemini"})
			time.Sleep(time.Second)
		}
		// 让 a 持有一个未释放的在途租约（active=1，窗口计数仅 1）
		inFlightA, _, err := pool.TryAcquireFor(context.Background(), AccountSelection{ModelID: "gemini"})
		if err != nil || inFlightA == nil || inFlightA.Account().ID != "a" {
			t.Fatalf("首笔在途租约应选中 a，实际=%v err=%v", inFlightA, err)
		}
		defer func() { _ = inFlightA.Release() }()
		time.Sleep(time.Second)

		// 下一笔新请求应优先选择当前空闲的 b（active=0 < a.active=1）
		inFlightB, _, err := pool.TryAcquireFor(context.Background(), AccountSelection{ModelID: "gemini"})
		if err != nil || inFlightB == nil {
			t.Fatalf("次笔租约失败: %v", err)
		}
		defer func() { _ = inFlightB.Release() }()
		if inFlightB.Account().ID != "b" {
			t.Fatalf("空闲账户 b 未优先于在途账户 a，实际选中=%s", inFlightB.Account().ID)
		}
		time.Sleep(time.Second)

		// 此时 a(active=1, count=1) 与 b(active=1, count=4) 同为 active=1，应选窗口次数更少的 a
		third, _, err := pool.TryAcquireFor(context.Background(), AccountSelection{ModelID: "gemini"})
		if err != nil || third == nil {
			t.Fatalf("第三笔租约失败: %v", err)
		}
		defer func() { _ = third.Release() }()
		if third.Account().ID != "a" {
			t.Fatalf("同并发下应选窗内次数更少的 a，实际选中=%s", third.Account().ID)
		}
	})
}

// TestLeastRecentWindowExpiryAndZeroWindow 验证滑窗过期后计数自然衰减，以及窗口为 0 时按空闲与最久未使用均衡且同刻度不粘死。
func TestLeastRecentWindowExpiryAndZeroWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newRecencyTestPool(2, nil, "a", "b", "c")
		pool.SetRoutingStrategy(RoutingStrategyLeastRecent)
		pool.SetRoutingRecencyWindow(10 * time.Second)

		// t=0: a 累计 3 次
		for range 3 {
			acquireAndRelease(t, pool, context.Background(), AccountSelection{AccountID: "a", ModelID: "gemini"})
		}
		// t=5s: b 使用 1 次
		time.Sleep(5 * time.Second)
		acquireAndRelease(t, pool, context.Background(), AccountSelection{AccountID: "b", ModelID: "gemini"})
		// t=6s: 未绑定请求应选中尚未使用过的 c
		time.Sleep(time.Second)
		id, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"})
		if id != "c" {
			t.Fatalf("t=6s 应选未使用的 c，实际=%s", id)
		}
		// t=11s: a 在 t=0 的 3 次使用已过期（count=0），b(t=5s, count=1) 与 c(t=6s, count=1) 仍在窗内，应重新选中 a
		time.Sleep(5 * time.Second)
		id, _ = acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"})
		if id != "a" {
			t.Fatalf("t=11s a 的旧计数过期后应选中 a，实际=%s", id)
		}

		// 切到 window=0：忽略历史次数，仅按最久未使用与单调序号轮转
		pool.SetRoutingRecencyWindow(0)
		time.Sleep(time.Second)
		for range 3 {
			acquireAndRelease(t, pool, context.Background(), AccountSelection{AccountID: "a", ModelID: "gemini"})
		}
		time.Sleep(time.Second)
		acquireAndRelease(t, pool, context.Background(), AccountSelection{AccountID: "b", ModelID: "gemini"})
		time.Sleep(time.Second)
		acquireAndRelease(t, pool, context.Background(), AccountSelection{AccountID: "c", ModelID: "gemini"})

		// 同一时钟刻度连续 6 次请求，无 sleep 也应按最久未使用严格轮转 a -> b -> c -> a -> b -> c
		got := make([]string, 0, 6)
		for range 6 {
			picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"})
			got = append(got, picked)
		}
		want := []string{"a", "b", "c", "a", "b", "c"}
		if !slices.Equal(got, want) {
			t.Fatalf("window=0 同刻度连续分配=%v，期望 %v", got, want)
		}
	})
}

// TestLeastRecentStickyAndBoundRequestsUnaffectedYetCounted 验证绑定与亲和请求不受降权干扰，但计入窗内次数促使新请求避让；内部维护租约不计入。
func TestLeastRecentStickyAndBoundRequestsUnaffectedYetCounted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newRecencyTestPool(2, nil, "a", "b", "c")
		pool.SetRoutingStrategy(RoutingStrategyLeastRecent)
		pool.SetRoutingRecencyWindow(5 * time.Minute)
		if err := pool.BindResourceKind("file-a", "a", "drive-file"); err != nil {
			t.Fatal(err)
		}
		// 也验证 InlineMediaAccount 软亲和解析能选中 a 并被 PreferredAccountID 保护
		blob := &Blob{MIME: "image/png", Data: []byte("sticky-image")}
		part := Part{InlineData: blob, DisplayName: "img.png"}
		key := inlineMediaKeyOf(part)
		pool.mu.Lock()
		pool.byID["a"].runtime.Resources["inline-a"] = ResourceBinding{
			Kind: "inline-media", MIME: key.mime, SHA256: key.sha256, DisplayName: key.name,
			CreatedAt: time.Now().UTC(),
		}
		pool.mu.Unlock()
		if preferred := pool.InlineMediaAccount([]Content{{Role: "user", Parts: []Part{part}}}); preferred != "a" {
			t.Fatalf("InlineMediaAccount=%q，期望 a", preferred)
		}

		// 内部 anyState 维护租约不应让 a 产生流量降权
		maint, _, err := pool.TryAcquireFor(context.Background(), AccountSelection{AccountID: "a", anyState: true})
		if err != nil || maint == nil {
			t.Fatalf("维护租约失败: %v", err)
		}
		if err := maint.Release(); err != nil {
			t.Fatal(err)
		}
		if picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"}); picked != "a" {
			t.Fatalf("anyState 维护租约不应导致 a 降权，实际首笔选中=%s", picked)
		}
		time.Sleep(time.Second)

		// 1. 显式指定账户 a
		if picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{AccountID: "a", ModelID: "gemini"}); picked != "a" {
			t.Fatalf("显式指定账户应命中 a，实际=%s", picked)
		}
		// 2. 文件资源绑定 file-a
		if picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ResourceID: "file-a", ModelID: "gemini"}); picked != "a" {
			t.Fatalf("资源绑定请求应命中 a，实际=%s", picked)
		}
		// 3. 软亲和 PreferredAccountID=a（即使 a 已累计 3 次、b/c 为 0）
		if picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{PreferredAccountID: "a", ModelID: "gemini"}); picked != "a" {
			t.Fatalf("软亲和请求应命中 a，实际=%s", picked)
		}
		time.Sleep(time.Second)

		// 此时 a 在窗内计数为 4，b 和 c 为 0；后续 8 次普通新请求应先填满 b/c 各 4 次，绝不提前打到 a
		got := make([]string, 0, 8)
		for range 8 {
			picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"})
			got = append(got, picked)
			time.Sleep(time.Second)
		}
		want := []string{"b", "c", "b", "c", "b", "c", "b", "c"}
		if !slices.Equal(got, want) {
			t.Fatalf("绑定/亲和计入后新请求应先均分 b/c，实际=%v，期望 %v", got, want)
		}
		// 第 9 次全部达到 count=4，a 最久未使用重新进入轮转
		if picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"}); picked != "a" {
			t.Fatalf("b/c 追平后应重新轮到最久未使用的 a，实际=%s", picked)
		}
	})
}

// TestLeastRecentRespectsCooldownsScopesAndExistingStrategiesUnchanged 验证冷却、作用域隔离、预热排序、旧策略不变及运行态文件不新增字段。
func TestLeastRecentRespectsCooldownsScopesAndExistingStrategiesUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		store := NewAccountStore(dir)
		savedAccounts := make([]*Account, 0, 3)
		ids := make([]string, 0, 3)
		for _, label := range []string{"a@example.com", "b@example.com", "c@example.com"} {
			cfg := DefaultAccountConfig(label)
			cfg.Locale, cfg.Timezone = "en-US", "UTC"
			account, publish, err := store.Create(cfg, StorageState{})
			if err != nil {
				t.Fatal(err)
			}
			if err := publish.Release(); err != nil {
				t.Fatal(err)
			}
			account.State = AccountReady
			account.Models = []Model{{ID: "gemini", Methods: []string{"generateContent"}}}
			account.BenefitTier = BenefitTierFree
			savedAccounts = append(savedAccounts, account)
			ids = append(ids, account.ID)
		}
		idA, idB, idC := ids[0], ids[1], ids[2]
		pool := NewAccountPool(savedAccounts, 2)
		pool.SetRoutingStrategy(RoutingStrategyLeastRecent)
		pool.SetRoutingRecencyWindow(5 * time.Minute)
		pool.SetAccountOrder([]string{idC, idB, idA})

		// PreferWarmPool 在 least-recent 下不套用 fill-first 的 ACCOUNT_ORDER
		warmLeast := pool.PreferWarmPool([]string{idA, idB, idC})
		if !slices.Equal(warmLeast, []string{idA, idB, idC}) {
			t.Fatalf("least-recent 预热顺序不应受 ACCOUNT_ORDER 改写: %v", warmLeast)
		}
		pool.SetRoutingStrategy(RoutingStrategyFillFirst)
		warmFill := pool.PreferWarmPool([]string{idA, idB, idC})
		if !slices.Equal(warmFill, []string{idC, idB, idA}) {
			t.Fatalf("fill-first 预热顺序应继续尊重 ACCOUNT_ORDER: %v", warmFill)
		}
		// fill-first 仍持续选中排序第一的 c
		if picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"}); picked != idC {
			t.Fatalf("fill-first 首选=%s，期望 %s", picked, idC)
		}
		if picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"}); picked != idC {
			t.Fatalf("fill-first 次选=%s，期望 %s", picked, idC)
		}

		// 切回 least-recent 并对 b 写入短冷却，b 不应被选中
		pool.SetRoutingStrategy(RoutingStrategyLeastRecent)
		if _, err := pool.MarkShortCooldown(idB, ChannelPlayground, "gemini", ShortCooldownBusy, time.Now(), 30*time.Second, "busy"); err != nil {
			t.Fatal(err)
		}
		if picked, _ := acquireAndRelease(t, pool, context.Background(), AccountSelection{ModelID: "gemini"}); picked != idA {
			t.Fatalf("b 在短冷却中应选 %s，实际=%s", idA, picked)
		}

		// API key 作用域仅允许 c 时必须尊重限制
		pool.SetAPIKeyAccounts(map[string][]string{"k1": {idC}})
		ctx := ContextWithAPIKey(context.Background(), "k1")
		if picked, _ := acquireAndRelease(t, pool, ctx, AccountSelection{ModelID: "gemini"}); picked != idC {
			t.Fatalf("API_KEY_ACCOUNTS 限制下应选 %s，实际=%s", idC, picked)
		}

		// 验证 runtime-state.json 未写入任何 recency 字段，且严格反序列化正常通过
		for _, account := range savedAccounts {
			raw, err := os.ReadFile(account.RuntimePath)
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if strings.Contains(strings.ToLower(string(raw)), "recency") || strings.Contains(strings.ToLower(string(raw)), "recent") {
				t.Fatalf("账户 %s 的 runtime-state.json 意外包含 recency 字段: %s", account.ID, raw)
			}
		}
		loaded, err := store.Load()
		if err != nil || len(loaded) != 3 || len(store.Failures()) != 0 {
			t.Fatalf("严格加载账户运行态失败: loaded=%d failures=%v err=%v", len(loaded), store.Failures(), err)
		}
		_ = filepath.Clean(dir)
	})
}

// TestLeastRecentWindowPruningAndBounds 验证高频调用下内存滑窗样本数受上限约束且过期后可回收。
func TestLeastRecentWindowPruningAndBounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newRecencyTestPool(2, nil, "a", "b")
		pool.SetRoutingStrategy(RoutingStrategyLeastRecent)
		pool.SetRoutingRecencyWindow(2 * time.Minute)

		for range maxRecencySamples + 200 {
			acquireAndRelease(t, pool, context.Background(), AccountSelection{AccountID: "a", ModelID: "gemini"})
		}
		pool.mu.Lock()
		gotLen := len(pool.byID["a"].recentUses)
		pool.mu.Unlock()
		if gotLen != maxRecencySamples {
			t.Fatalf("滑窗样本上限=%d，期望 %d", gotLen, maxRecencySamples)
		}
		// 超过保留期（DefaultRoutingRecencyWindow=5m）后触发查询应清空旧记录
		time.Sleep(DefaultRoutingRecencyWindow + time.Second)
		pool.mu.Lock()
		count := pool.recentUseCountLocked(pool.byID["a"], time.Now())
		afterLen := len(pool.byID["a"].recentUses)
		pool.mu.Unlock()
		if count != 0 || afterLen != 0 {
			t.Fatalf("过期回收后 count=%d len=%d，期望均为 0", count, afterLen)
		}
	})
}
