package aistudio

import (
	"context"
	"testing"
	"time"
)

// TestShortCooldownStatusKeepsIndependentKinds 验证短冷却按通道、模型与原因独立保留，状态快照不共享可变映射
func TestShortCooldownStatusKeepsIndependentKinds(t *testing.T) {
	pool := newShortCooldownPool("a@example.com")
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	for _, item := range []struct {
		channel  Channel
		kind     ShortCooldownKind
		duration time.Duration
		reason   string
	}{
		{ChannelPlayground, ShortCooldownRace, 30 * time.Second, "首字竞速被取代"},
		{ChannelPlayground, ShortCooldownBusy, 10 * time.Second, "模型繁忙"},
		{ChannelBuild, ShortCooldownBusy, 20 * time.Second, "Build 模型繁忙"},
	} {
		until, err := pool.MarkShortCooldown("a@example.com", item.channel, " models/gemini ", item.kind, now, item.duration, item.reason)
		if err != nil || !until.Equal(now.Add(item.duration)) {
			t.Fatalf("mark channel=%s kind=%s until=%s err=%v", item.channel, item.kind, until, err)
		}
	}
	status := pool.Status()[0]
	if len(status.ShortCooldowns) != 2 || len(status.ShortCooldowns["playground:gemini"]) != 2 || len(status.ShortCooldowns["build:gemini"]) != 1 {
		t.Fatalf("short cooldowns=%+v", status.ShortCooldowns)
	}
	want := ShortCooldownState{MarkedAt: now, Until: now.Add(30 * time.Second), Reason: "首字竞速被取代"}
	if got := status.ShortCooldowns["playground:gemini"][ShortCooldownRace]; got != want {
		t.Fatalf("race=%+v want=%+v", got, want)
	}
	if len(status.Cooldowns) != 0 {
		t.Fatalf("short cooldowns leaked into quota cooldowns=%+v", status.Cooldowns)
	}
	delete(status.ShortCooldowns["playground:gemini"], ShortCooldownRace)
	delete(status.ShortCooldowns, "build:gemini")
	fresh := pool.Status()[0].ShortCooldowns
	if fresh["playground:gemini"][ShortCooldownRace] != want || len(fresh["build:gemini"]) != 1 {
		t.Fatalf("status mutation changed pool=%+v", fresh)
	}
}

// TestShortCooldownRepeatedReasonKeepsFirstMark 验证连续有效的同原因短冷却保留起点、不缩短期限，并且不受模型成功检查时间拦截
func TestShortCooldownRepeatedReasonKeepsFirstMark(t *testing.T) {
	pool := newShortCooldownPool("a@example.com")
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	if _, err := pool.MarkModelAccessVerifiedIfGeneration("a@example.com", "gemini", pool.ModelAccessGeneration("a@example.com"), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		at       time.Time
		duration time.Duration
		markedAt time.Time
		until    time.Time
	}{
		{now, 30 * time.Second, now, now.Add(30 * time.Second)},
		{now.Add(5 * time.Second), 2 * time.Second, now, now.Add(30 * time.Second)},
		{now.Add(10 * time.Second), 40 * time.Second, now, now.Add(50 * time.Second)},
		{now.Add(51 * time.Second), 10 * time.Second, now.Add(51 * time.Second), now.Add(61 * time.Second)},
	} {
		until, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownBusy, item.at, item.duration, "模型繁忙")
		if err != nil || !until.Equal(item.until) {
			t.Fatalf("mark at=%s until=%s want=%s err=%v", item.at, until, item.until, err)
		}
		got := pool.Status()[0].ShortCooldowns["playground:gemini"][ShortCooldownBusy]
		if !got.MarkedAt.Equal(item.markedAt) || !got.Until.Equal(item.until) || got.Reason != "模型繁忙" {
			t.Fatalf("cooldown=%+v want marked=%s until=%s", got, item.markedAt, item.until)
		}
	}
}

// TestShortCooldownPersistsAcrossPools 验证短冷却与旧冷却分别落盘，重载及另一个账户池的更新不会丢失任一原因
func TestShortCooldownPersistsAcrossPools(t *testing.T) {
	store := NewAccountStore(t.TempDir())
	config := DefaultAccountConfig("a@example.com")
	config.Locale, config.Timezone = "en-US", "UTC"
	account, publish, err := store.Create(config, StorageState{})
	if err != nil {
		t.Fatal(err)
	}
	if err := publish.Release(); err != nil {
		t.Fatal(err)
	}
	pool := NewAccountPool([]*Account{account}, 1)
	now := time.Now().UTC()
	quotaUntil := now.Add(time.Hour)
	if err := pool.MarkCooldownIfGeneration(account.ID, "gemini", pool.ModelAccessGeneration(account.ID), now.Add(-time.Second), quotaUntil, "旧额度冷却"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkShortCooldown(account.ID, ChannelPlayground, "gemini", ShortCooldownRace, now, 30*time.Second, "首字竞速被取代"); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load accounts=%d err=%v failures=%v", len(loaded), err, store.Failures())
	}
	other := NewAccountPool(loaded, 1)
	if _, err := other.MarkShortCooldown(account.ID, ChannelPlayground, "gemini", ShortCooldownBusy, now, 10*time.Second, "模型繁忙"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkShortCooldown(account.ID, ChannelBuild, "gemini", ShortCooldownBusy, now, 20*time.Second, "Build 模型繁忙"); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.Load()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("reload accounts=%d err=%v failures=%v", len(loaded), err, store.Failures())
	}
	status := NewAccountPool(loaded, 1).Status()[0]
	if !status.Cooldowns["gemini"].Until.Equal(quotaUntil) || status.Cooldowns["gemini"].Reason != "旧额度冷却" {
		t.Fatalf("quota cooldown changed=%+v", status.Cooldowns)
	}
	if len(status.ShortCooldowns["playground:gemini"]) != 2 || len(status.ShortCooldowns["build:gemini"]) != 1 {
		t.Fatalf("persisted short cooldowns=%+v", status.ShortCooldowns)
	}
	if got := status.ShortCooldowns["playground:gemini"][ShortCooldownRace]; !got.MarkedAt.Equal(now) || !got.Until.Equal(now.Add(30*time.Second)) {
		t.Fatalf("persisted race=%+v", got)
	}
}

// TestShortCooldownClearRemovesOnlyTargetKind 验证手动清除只删除目标类型，不误删同范围其他短项或旧冷却
func TestShortCooldownClearRemovesOnlyTargetKind(t *testing.T) {
	pool := newShortCooldownPool("a@example.com")
	now := time.Now().UTC()
	if err := pool.MarkCooldownIfGeneration("a@example.com", "gemini", pool.ModelAccessGeneration("a@example.com"), now.Add(-time.Second), now.Add(time.Hour), "旧额度冷却"); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		channel Channel
		kind    ShortCooldownKind
	}{
		{ChannelPlayground, ShortCooldownRace},
		{ChannelPlayground, ShortCooldownBusy},
		{ChannelBuild, ShortCooldownRace},
	} {
		if _, err := pool.MarkShortCooldown("a@example.com", item.channel, "gemini", item.kind, now, time.Minute, "fixture"); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.ClearShortCooldown("a@example.com", "playground:gemini", ShortCooldownRace); err != nil {
		t.Fatal(err)
	}
	status := pool.Status()[0]
	if _, exists := status.ShortCooldowns["playground:gemini"][ShortCooldownRace]; exists || len(status.ShortCooldowns["playground:gemini"]) != 1 || len(status.ShortCooldowns["build:gemini"]) != 1 || len(status.Cooldowns) != 1 {
		t.Fatalf("cleared unrelated cooldowns=%+v short=%+v", status.Cooldowns, status.ShortCooldowns)
	}
	if err := pool.ClearShortCooldown("a@example.com", "playground:gemini", ShortCooldownBusy); err != nil {
		t.Fatal(err)
	}
	if _, exists := pool.Status()[0].ShortCooldowns["playground:gemini"]; exists {
		t.Fatal("empty scope retained after clearing its last kind")
	}
	if err := pool.ClearShortCooldown("a@example.com", "playground:gemini", ShortCooldownRace); err != nil {
		t.Fatalf("repeated clear err=%v", err)
	}
}

// TestShortCooldownSelectionBlocksQuotaFallback 验证短冷却独立阻止普通与额度回退选号，并且不扩大到其他账户、模型或通道
func TestShortCooldownSelectionBlocksQuotaFallback(t *testing.T) {
	for _, ignoreQuota := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "quota_fallback"}[ignoreQuota], func(t *testing.T) {
			pool := newShortCooldownPool("a@example.com", "b@example.com")
			now := time.Now().UTC()
			if _, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownRace, now, time.Minute, "首字竞速被取代"); err != nil {
				t.Fatal(err)
			}
			selection := AccountSelection{ModelID: "models/gemini", Method: "generateContent", IgnoreQuotaCooldown: ignoreQuota}
			lease, _, err := pool.TryAcquireFor(context.Background(), selection)
			if err != nil || lease == nil {
				t.Fatalf("lease=%v err=%v", lease, err)
			}
			if got := lease.Account().ID; got != "b@example.com" {
				t.Errorf("selected cooling account=%s", got)
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
			selection.AccountID = "a@example.com"
			lease, waitable, err := pool.TryAcquireFor(context.Background(), selection)
			if lease != nil {
				_ = lease.Release()
			}
			if err != nil || lease != nil || !waitable {
				t.Fatalf("bound cooling lease=%v waitable=%v err=%v", lease, waitable, err)
			}
			selection.ModelID = "other"
			lease, _, err = pool.TryAcquireFor(context.Background(), selection)
			if err != nil || lease == nil {
				t.Fatalf("other model lease=%v err=%v", lease, err)
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestShortCooldownChannelsAndCandidates 验证优先通道冷却后可用其他通道接住，候选分类仍保留各通道共同限制的最早恢复时间
func TestShortCooldownChannelsAndCandidates(t *testing.T) {
	pool := newShortCooldownPool("a@example.com")
	pool.SetUpstreamChannels([]Channel{ChannelPlayground, ChannelBuild})
	if err := pool.SetBuildCatalog("a@example.com", []Model{{ID: "gemini", Methods: []string{"generateContent"}}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := pool.MarkShortCooldown("a@example.com", ChannelBuild, "gemini", ShortCooldownBusy, now, 10*time.Second, "Build 模型繁忙"); err != nil {
		t.Fatal(err)
	}
	selection := AccountSelection{ModelID: "gemini", Method: "generateContent", PreferredChannel: ChannelBuild, IgnoreQuotaCooldown: true}
	lease, _, err := pool.TryAcquireFor(context.Background(), selection)
	if err != nil || lease == nil {
		t.Fatalf("fallback channel lease=%v err=%v", lease, err)
	}
	if lease.Channel() != ChannelPlayground {
		t.Errorf("channel=%s want playground", lease.Channel())
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownRace, now, 30*time.Second, "首字竞速被取代"); err != nil {
		t.Fatal(err)
	}
	groups, err := pool.ClassifyCandidates(context.Background(), selection, []string{"a@example.com"})
	if err != nil || !groups.Eligible || len(groups.WarmReady)+len(groups.WarmAvailable)+len(groups.WarmBusy) != 0 || !groups.EarliestCooldown.Until.Equal(now.Add(10*time.Second)) {
		t.Fatalf("cooling candidates=%+v err=%v", groups, err)
	}
	if pool.AccountChannelAvailable("a@example.com", selection) {
		t.Fatal("all short-cooled channels reported available")
	}
	if err := pool.ClearShortCooldown("a@example.com", "build:gemini", ShortCooldownBusy); err != nil {
		t.Fatal(err)
	}
	groups, err = pool.ClassifyCandidates(context.Background(), selection, []string{"a@example.com"})
	if err != nil || len(groups.WarmReady) != 1 || !pool.AccountChannelAvailable("a@example.com", selection) {
		t.Fatalf("reopened candidates=%+v err=%v", groups, err)
	}
}

// TestShortCooldownLeaseGateKeepsInflightAndChecksResource 验证冷却前租约继续执行，文件绑定仍可选号以进入复制换号路径，但冷却后的新租约不能发送
func TestShortCooldownLeaseGateKeepsInflightAndChecksResource(t *testing.T) {
	for _, ignoreQuota := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "quota_fallback"}[ignoreQuota], func(t *testing.T) {
			pool := newShortCooldownPool("a@example.com")
			selection := AccountSelection{ModelID: "gemini", Method: "generateContent", IgnoreQuotaCooldown: ignoreQuota}
			inflight, _, err := pool.TryAcquireFor(context.Background(), selection)
			if err != nil || inflight == nil {
				t.Fatalf("inflight lease=%v err=%v", inflight, err)
			}
			defer inflight.Release()
			if err := pool.BindResourceKind("files/fixture", "a@example.com", "drive-file"); err != nil {
				t.Fatal(err)
			}
			markedAt := inflight.CheckedAt().Add(time.Nanosecond)
			if _, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownBusy, markedAt, time.Minute, "模型繁忙"); err != nil {
				t.Fatal(err)
			}
			if inflight.CoolingDown("gemini") {
				t.Fatal("new short cooldown interrupted an earlier lease")
			}
			selection.ResourceID = "files/fixture"
			fresh, _, err := pool.TryAcquireFor(context.Background(), selection)
			if err != nil || fresh == nil {
				t.Fatalf("resource selection must reach send gate lease=%v err=%v", fresh, err)
			}
			defer fresh.Release()
			if !fresh.CoolingDown("models/gemini") {
				t.Fatal("new resource lease bypassed short cooldown at send gate")
			}
			if fresh.CoolingDown("other") {
				t.Fatal("model-specific short cooldown blocked another model")
			}
			if err := pool.MarkCooldownIfGeneration("a@example.com", "gemini", pool.ModelAccessGeneration("a@example.com"), time.Now().UTC(), time.Now().Add(time.Hour), "旧 429 冷却"); err != nil {
				t.Fatal(err)
			}
			if !inflight.CoolingDown("gemini") {
				t.Fatal("short-cooldown lease exemption also bypassed old 429 cooldown")
			}
		})
	}
}

// TestShortCooldownLeaseRevalidatesPersistedState 验证选号时内存尚未见到的跨池短冷却在取得租约后复验，不能被额度回退绕过
func TestShortCooldownLeaseRevalidatesPersistedState(t *testing.T) {
	store := NewAccountStore(t.TempDir())
	config := DefaultAccountConfig("a@example.com")
	config.Locale, config.Timezone = "en-US", "UTC"
	account, publish, err := store.Create(config, StorageState{})
	if err != nil {
		t.Fatal(err)
	}
	if err := publish.Release(); err != nil {
		t.Fatal(err)
	}
	pool := NewAccountPool([]*Account{account}, 1)
	model := Model{ID: "gemini", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}}
	if err := pool.SetCatalog(account.ID, 0, []Model{model}); err != nil {
		t.Fatal(err)
	}
	if err := pool.MarkReady(account.ID); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load accounts=%d err=%v failures=%v", len(loaded), err, store.Failures())
	}
	other := NewAccountPool(loaded, 1)
	if _, err := other.MarkShortCooldown(account.ID, ChannelPlayground, "gemini", ShortCooldownBusy, time.Now().UTC(), time.Minute, "模型繁忙"); err != nil {
		t.Fatal(err)
	}
	selection := AccountSelection{ModelID: "gemini", Method: "generateContent", IgnoreQuotaCooldown: true}
	lease, waitable, err := pool.TryAcquireFor(context.Background(), selection)
	if lease != nil {
		_ = lease.Release()
	}
	if err != nil || lease != nil || !waitable {
		t.Fatalf("stale runtime lease=%v waitable=%v err=%v", lease, waitable, err)
	}
}

// TestShortCooldownSuccessAndRefreshKeepIndependentState 验证其他请求成功、清理旧冷却或刷新目录都不提前删除短项
func TestShortCooldownSuccessAndRefreshKeepIndependentState(t *testing.T) {
	pool := newShortCooldownPool("a@example.com")
	account, err := pool.Account("a@example.com")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, kind := range []ShortCooldownKind{ShortCooldownRace, ShortCooldownBusy} {
		if _, err := pool.MarkShortCooldown(account.ID, ChannelPlayground, "gemini", kind, now, time.Minute, string(kind)); err != nil {
			t.Fatal(err)
		}
	}
	before := pool.Status()[0].ShortCooldowns["playground:gemini"]
	generation := pool.ModelAccessGeneration(account.ID)
	if err := pool.MarkCooldownIfGeneration(account.ID, "gemini", generation, now.Add(-time.Second), now.Add(time.Hour), "旧额度冷却"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkModelAccessVerifiedIfGeneration(account.ID, "gemini", generation, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkModelAccessVerifiedIfGeneration(account.ID, "gemini", generation, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := pool.ClearCooldownIfGeneration(account.ID, "gemini", generation, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := pool.ClearPageCooldownIfGeneration(account.ID, generation, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := pool.SetCatalog(account.ID, 0, account.Models); err != nil {
		t.Fatal(err)
	}
	if err := pool.ClearCooldown(account.ID, "gemini"); err != nil {
		t.Fatal(err)
	}
	status := pool.Status()[0]
	if len(status.Cooldowns) != 0 || len(status.ShortCooldowns["playground:gemini"]) != 2 {
		t.Fatalf("success/refresh cooldowns=%+v short=%+v", status.Cooldowns, status.ShortCooldowns)
	}
	for kind, want := range before {
		if got := status.ShortCooldowns["playground:gemini"][kind]; got != want {
			t.Fatalf("short cooldown changed by success/refresh kind=%s got=%+v want=%+v", kind, got, want)
		}
	}
}

// TestShortCooldownValidationAndDisabledSettings 验证 0 时长不新增或清除冷却，无效手动清除参数不能误删已有短项
func TestShortCooldownValidationAndDisabledSettings(t *testing.T) {
	pool := newShortCooldownPool("a@example.com")
	now := time.Now().UTC()
	if _, err := pool.MarkShortCooldown("a@example.com", "", " models/gemini ", ShortCooldownRace, now, time.Minute, "fixture"); err != nil {
		t.Fatal(err)
	}
	before := pool.Status()[0].ShortCooldowns["playground:gemini"][ShortCooldownRace]
	until, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownRace, now.Add(time.Second), 0, "disabled")
	if err != nil || !until.IsZero() || pool.Status()[0].ShortCooldowns["playground:gemini"][ShortCooldownRace] != before {
		t.Fatalf("disabled cooldown changed state until=%s err=%v", until, err)
	}
	for _, item := range []struct {
		channel  Channel
		modelID  string
		kind     ShortCooldownKind
		at       time.Time
		duration time.Duration
	}{
		{ChannelPlayground, "gemini", "other", now, time.Minute},
		{Channel("other"), "gemini", ShortCooldownBusy, now, time.Minute},
		{ChannelPlayground, "", ShortCooldownBusy, now, time.Minute},
		{ChannelPlayground, "gemini", ShortCooldownBusy, time.Time{}, time.Minute},
		{ChannelPlayground, "gemini", ShortCooldownBusy, now, -time.Second},
	} {
		if _, err := pool.MarkShortCooldown("a@example.com", item.channel, item.modelID, item.kind, item.at, item.duration, "invalid"); err == nil {
			t.Errorf("invalid mark accepted=%+v", item)
		}
	}
	for _, item := range []struct {
		scope string
		kind  ShortCooldownKind
	}{
		{"gemini", ShortCooldownRace},
		{"other:gemini", ShortCooldownRace},
		{"playground:", ShortCooldownRace},
		{"playground:gemini", "other"},
	} {
		if err := pool.ClearShortCooldown("a@example.com", item.scope, item.kind); err == nil {
			t.Errorf("invalid clear accepted=%+v", item)
		}
	}
	if _, err := pool.MarkShortCooldown("missing@example.com", ChannelPlayground, "gemini", ShortCooldownBusy, now, time.Minute, "fixture"); err == nil {
		t.Error("mark accepted a missing account")
	}
	if err := pool.ClearShortCooldown("missing@example.com", "playground:gemini", ShortCooldownBusy); err == nil {
		t.Error("clear accepted a missing account")
	}
	status := pool.Status()[0]
	if len(status.ShortCooldowns) != 1 || len(status.ShortCooldowns["playground:gemini"]) != 1 || status.ShortCooldowns["playground:gemini"][ShortCooldownRace] != before {
		t.Fatalf("invalid arguments changed short cooldowns=%+v", status.ShortCooldowns)
	}
}

// TestShortCooldownLayersKeepLatestEffectiveLimit 验证每个通道按有效限制的最晚恢复时间调度，额度回退只忽略旧额度而不忽略任何短项
func TestShortCooldownLayersKeepLatestEffectiveLimit(t *testing.T) {
	pool := newShortCooldownPool("a@example.com")
	now := time.Now().UTC()
	if err := pool.MarkCooldownIfGeneration("a@example.com", "gemini", pool.ModelAccessGeneration("a@example.com"), now.Add(-time.Second), now.Add(time.Hour), PlanLimitKind+": fixture"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownRace, now, 30*time.Second, "首字竞速被取代"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownBusy, now, 10*time.Second, "模型繁忙"); err != nil {
		t.Fatal(err)
	}
	selection := AccountSelection{ModelID: "gemini", Method: "generateContent"}
	groups, err := pool.ClassifyCandidates(context.Background(), selection, nil)
	if err != nil || !groups.EarliestCooldown.Until.Equal(now.Add(time.Hour)) {
		t.Fatalf("normal effective cooldown=%+v err=%v", groups, err)
	}
	selection.IgnoreQuotaCooldown = true
	groups, err = pool.ClassifyCandidates(context.Background(), selection, nil)
	if err != nil || !groups.EarliestCooldown.Until.Equal(now.Add(30*time.Second)) || groups.EarliestCooldown.Reason != "首字竞速被取代" {
		t.Fatalf("fallback effective cooldown=%+v err=%v", groups, err)
	}
	if err := pool.ClearShortCooldown("a@example.com", "playground:gemini", ShortCooldownRace); err != nil {
		t.Fatal(err)
	}
	groups, err = pool.ClassifyCandidates(context.Background(), selection, nil)
	if err != nil || !groups.EarliestCooldown.Until.Equal(now.Add(10*time.Second)) || groups.EarliestCooldown.Reason != "模型繁忙" {
		t.Fatalf("remaining busy cooldown=%+v err=%v", groups, err)
	}
	if err := pool.ClearShortCooldown("a@example.com", "playground:gemini", ShortCooldownBusy); err != nil {
		t.Fatal(err)
	}
	lease, _, err := pool.TryAcquireFor(context.Background(), selection)
	if err != nil || lease == nil {
		t.Fatalf("quota fallback after manual short clear lease=%v err=%v", lease, err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if len(pool.Status()[0].Cooldowns) != 1 {
		t.Fatal("manual short clear removed old quota cooldown")
	}
}

// TestShortCooldownReasonChangeKeepsFirstMark 验证同类连续冷却不因上游原因文案变化而把期间取得的文件租约误判为冷却前租约
func TestShortCooldownReasonChangeKeepsFirstMark(t *testing.T) {
	pool := newShortCooldownPool("a@example.com")
	if err := pool.BindResourceKind("files/fixture", "a@example.com", "drive-file"); err != nil {
		t.Fatal(err)
	}
	markedAt := time.Now().UTC().Add(-time.Second)
	until, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownBusy, markedAt, time.Minute, "The model is overloaded")
	if err != nil {
		t.Fatal(err)
	}
	lease, _, err := pool.TryAcquireFor(context.Background(), AccountSelection{ModelID: "gemini", Method: "generateContent", ResourceID: "files/fixture"})
	if err != nil || lease == nil {
		t.Fatalf("resource lease=%v err=%v", lease, err)
	}
	defer lease.Release()
	nextUntil, err := pool.MarkShortCooldown("a@example.com", ChannelPlayground, "gemini", ShortCooldownBusy, lease.CheckedAt().Add(time.Nanosecond), time.Second, "The service is currently unavailable")
	if err != nil {
		t.Fatal(err)
	}
	state := pool.Status()[0].ShortCooldowns["playground:gemini"][ShortCooldownBusy]
	if !state.MarkedAt.Equal(markedAt) || !nextUntil.Equal(until) || state.Reason != "The service is currently unavailable" {
		t.Errorf("continued busy cooldown=%+v until=%s want marked=%s until=%s", state, nextUntil, markedAt, until)
	}
	if !lease.CoolingDown("gemini") {
		t.Error("reason change exempted a lease acquired during an active busy cooldown")
	}
}

// TestShortCooldownExpiredLimitsDoNotBlockLease 验证过去的旧冷却与短项不会被合并成有效限制
func TestShortCooldownExpiredLimitsDoNotBlockLease(t *testing.T) {
	expiredAt := time.Now().UTC().Add(-time.Minute)
	pool := NewAccountPool([]*Account{{
		ID: "a@example.com", Config: AccountConfig{Enabled: true}, State: AccountReady,
		Models: []Model{{ID: "gemini", Methods: []string{"generateContent"}}},
		runtime: accountRuntimeState{
			Cooldowns: map[string]CooldownState{"gemini": {Until: expiredAt, Reason: "旧 429 冷却"}},
			ShortCooldowns: map[string]map[ShortCooldownKind]ShortCooldownState{
				"playground:gemini": {ShortCooldownBusy: {MarkedAt: expiredAt.Add(-time.Second), Until: expiredAt, Reason: "模型繁忙"}},
			},
		},
	}}, 1)
	selection := AccountSelection{ModelID: "gemini", Method: "generateContent"}
	groups, err := pool.ClassifyCandidates(context.Background(), selection, []string{"a@example.com"})
	if err != nil || len(groups.WarmReady) != 1 || !groups.EarliestCooldown.Until.IsZero() {
		t.Fatalf("expired candidate limits=%+v err=%v", groups, err)
	}
	lease, _, err := pool.TryAcquireFor(context.Background(), selection)
	if err != nil || lease == nil {
		t.Fatalf("expired limits blocked lease=%v err=%v", lease, err)
	}
	defer lease.Release()
	if lease.CoolingDown("gemini") {
		t.Fatal("expired cooldown became active at send gate")
	}
}

func newShortCooldownPool(accountIDs ...string) *AccountPool {
	models := []Model{
		{ID: "gemini", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}},
		{ID: "other", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}},
	}
	accounts := make([]*Account, 0, len(accountIDs))
	for _, accountID := range accountIDs {
		accounts = append(accounts, &Account{
			ID: accountID, Config: AccountConfig{Enabled: true}, State: AccountReady, Models: models,
		})
	}
	return NewAccountPool(accounts, 3)
}
