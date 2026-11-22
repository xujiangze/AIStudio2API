package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/api"
	"github.com/Mag1cFall/AIStudio2API/internal/config"
)

// firstEventPolicyRecorder 记录管理保存传给生成服务的首字策略，其他即时更新操作为空实现。
type firstEventPolicyRecorder struct {
	managedService
	policies []config.Config
}

func (*firstEventPolicyRecorder) SetAPIKeyAccounts(map[string][]string)     {}
func (*firstEventPolicyRecorder) ApplyCapacity(int, int, int, int)          {}
func (*firstEventPolicyRecorder) SetRequestPolicy([]config.ModelAlias, int) {}
func (service *firstEventPolicyRecorder) SetFirstEventPolicy(cfg config.Config) {
	service.policies = append(service.policies, cfg)
}

// newFirstEventConfigHandler 在临时配置上装配真实的管理读取、保存与即时更新接口。
func newFirstEventConfigHandler(t *testing.T) (http.Handler, *firstEventPolicyRecorder, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	cfg := config.Default()
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pool := aistudio.NewAccountPool(nil, 1)
	requests := newRequestRegistry(ctx)
	admin := &runtimeAdmin{configPath: path, requests: requests, pool: pool, service: &trackedService{pool: pool}}
	service := &firstEventPolicyRecorder{}
	manager := &runtimeManager{
		activeManagement: cfg, requests: requests, logs: &logOutput{},
		current: &runtimeGeneration{service: service, admin: admin, config: cfg},
	}
	return api.NewHandler(manager, api.Config{Admin: manager}), service, path
}

func firstEventConfigExchange(handler http.Handler, method, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "http://localhost/api/config", strings.NewReader(body))
	request.RemoteAddr = "127.0.0.1:5000"
	request.Header.Set("Origin", "http://localhost")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// TestFirstEventAdminConfig 验证管理 API 完整暴露并保存首字策略，且只更新新请求、不要求重启。
func TestFirstEventAdminConfig(t *testing.T) {
	handler, service, path := newFirstEventConfigHandler(t)
	response := firstEventConfigExchange(handler, http.MethodGet, "")
	var value api.RuntimeConfig
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &value) != nil {
		t.Fatalf("读取配置=%d %s", response.Code, response.Body)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"first_event_hedge": `"20"`, "first_event_hedge_models": `"*"`,
		"first_event_hedge_max_concurrency": "16", "first_event_timeout_seconds": "60",
		"first_event_race_cooldown_seconds": "30", "model_busy_cooldown_seconds": "10",
	} {
		if got := string(fields[key]); got != want {
			t.Fatalf("JSON %s=%s，期望 %s", key, got, want)
		}
	}
	if value.FirstEventHedge != "20" || value.FirstEventHedgeModels != "*" || value.FirstEventHedgeMaxConcurrency != 16 ||
		value.FirstEventTimeoutSeconds != 60 || value.FirstEventRaceCooldownSeconds != 30 || value.ModelBusyCooldownSeconds != 10 {
		t.Fatalf("默认策略=%s", response.Body)
	}
	value.FirstEventHedge = "off"
	value.FirstEventHedgeModels = ""
	value.FirstEventHedgeMaxConcurrency = 6
	value.FirstEventTimeoutSeconds = 0
	value.FirstEventRaceCooldownSeconds = 0
	value.ModelBusyCooldownSeconds = 12
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	response = firstEventConfigExchange(handler, http.MethodPut, string(raw))
	if response.Code != http.StatusOK {
		t.Fatalf("保存配置=%d %s", response.Code, response.Body)
	}
	var updated api.RuntimeConfig
	if err := json.Unmarshal(response.Body.Bytes(), &updated); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(updated, value) || updated.ServiceRestartRequired || updated.ManagementRestartRequired {
		t.Fatalf("保存返回值改变或要求重启: %+v", updated)
	}
	saved, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(service.policies) != 1 {
		t.Fatalf("即时策略应用次数=%d", len(service.policies))
	}
	applied := service.policies[0]
	if applied.FirstEventHedge != "off" || applied.FirstEventHedgeModels != "" || applied.FirstEventHedgeMaxConcurrency != 6 ||
		applied.FirstEventTimeoutSeconds != 0 || applied.FirstEventRaceCooldownSeconds != 0 || applied.ModelBusyCooldownSeconds != 12 {
		t.Fatalf("即时策略=%+v", applied)
	}
	if saved.FirstEventHedge != applied.FirstEventHedge || saved.FirstEventHedgeModels != applied.FirstEventHedgeModels ||
		saved.FirstEventHedgeMaxConcurrency != applied.FirstEventHedgeMaxConcurrency || saved.FirstEventTimeoutSeconds != applied.FirstEventTimeoutSeconds ||
		saved.FirstEventRaceCooldownSeconds != applied.FirstEventRaceCooldownSeconds || saved.ModelBusyCooldownSeconds != applied.ModelBusyCooldownSeconds {
		t.Fatalf("保存的策略与即时策略不同: %+v", saved)
	}
}

// TestFirstEventAdminRejectsInvalidConfig 验证 API 拒绝非正备用并发与负秒数，不改文件、不应用策略。
func TestFirstEventAdminRejectsInvalidConfig(t *testing.T) {
	for _, field := range []struct {
		name string
		set  func(*api.RuntimeConfig)
	}{
		{"zero concurrency", func(c *api.RuntimeConfig) { c.FirstEventHedgeMaxConcurrency = 0 }},
		{"negative concurrency", func(c *api.RuntimeConfig) { c.FirstEventHedgeMaxConcurrency = -1 }},
		{"negative timeout", func(c *api.RuntimeConfig) { c.FirstEventTimeoutSeconds = -1 }},
		{"negative race cooldown", func(c *api.RuntimeConfig) { c.FirstEventRaceCooldownSeconds = -1 }},
		{"negative busy cooldown", func(c *api.RuntimeConfig) { c.ModelBusyCooldownSeconds = -1 }},
	} {
		t.Run(field.name, func(t *testing.T) {
			handler, service, path := newFirstEventConfigHandler(t)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			value := runtimeConfigDTO(config.Default())
			field.set(&value)
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			response := firstEventConfigExchange(handler, http.MethodPut, string(raw))
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), `"code":"invalid_config"`) {
				t.Fatalf("无效配置=%d %s", response.Code, response.Body)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(before) || len(service.policies) != 0 {
				t.Fatalf("无效保存产生副作用: policies=%d err=%v", len(service.policies), err)
			}
		})
	}
}

// TestFirstEventAdminEnvironmentLocks 验证管理保存不能覆盖首字环境变量，应用的是服务端更新后的值。
func TestFirstEventAdminEnvironmentLocks(t *testing.T) {
	handler, service, path := newFirstEventConfigHandler(t)
	for key, value := range map[string]string{
		"FIRST_EVENT_HEDGE": "off", "FIRST_EVENT_HEDGE_MODELS": "gemini-4-argon",
		"FIRST_EVENT_HEDGE_MAX_CONCURRENCY": "3", "FIRST_EVENT_TIMEOUT_SECONDS": "0",
		"FIRST_EVENT_RACE_COOLDOWN_SECONDS": "0", "MODEL_BUSY_COOLDOWN_SECONDS": "0",
	} {
		t.Setenv(key, value)
	}
	raw, err := json.Marshal(runtimeConfigDTO(config.Default()))
	if err != nil {
		t.Fatal(err)
	}
	response := firstEventConfigExchange(handler, http.MethodPut, string(raw))
	var updated api.RuntimeConfig
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &updated) != nil {
		t.Fatalf("环境变量保存=%d %s", response.Code, response.Body)
	}
	if updated.FirstEventHedge != "off" || updated.FirstEventHedgeModels != "gemini-4-argon" || updated.FirstEventHedgeMaxConcurrency != 3 ||
		updated.FirstEventTimeoutSeconds != 0 || updated.FirstEventRaceCooldownSeconds != 0 || updated.ModelBusyCooldownSeconds != 0 {
		t.Fatalf("环境变量被覆盖: %+v", updated)
	}
	if !slices.Contains(updated.Environment, "FIRST_EVENT_TIMEOUT_SECONDS") || len(service.policies) != 1 || service.policies[0].FirstEventHedgeMaxConcurrency != 3 {
		t.Fatalf("未使用服务端更新值: %+v", service.policies)
	}
	file, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(file), "FIRST_EVENT_") || strings.Contains(string(file), "MODEL_BUSY_") {
		t.Fatalf("环境变量写入了文件: %q err=%v", file, err)
	}
}

// TestAdminShortCooldownsIndependent 验证同范围旧冷却与每类短冷却分别列出、手动清除互不影响。
func TestAdminShortCooldownsIndependent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	modelID, accountID := "gemini", "a@example.com"
	account := &aistudio.Account{
		ID: accountID, Config: aistudio.AccountConfig{Enabled: true, Label: accountID}, State: aistudio.AccountReady,
		Models: []aistudio.Model{{ID: modelID, Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}}},
	}
	pool := aistudio.NewAccountPool([]*aistudio.Account{account}, 2)
	admin := &runtimeAdmin{pool: pool, store: &aistudio.AccountStore{}, requests: newRequestRegistry(ctx)}
	now := time.Now().UTC()
	legacyUntil, raceUntil, busyUntil := now.Add(time.Hour), now.Add(30*time.Second), now.Add(10*time.Second)
	if err := pool.MarkCooldownIfGeneration(accountID, modelID, 0, now, legacyUntil, "额度冷却: quota"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkShortCooldown(accountID, aistudio.ChannelPlayground, modelID, aistudio.ShortCooldownRace, now, 30*time.Second, "竞速冷却: 被备用请求取代"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkShortCooldown(accountID, aistudio.ChannelPlayground, modelID, aistudio.ShortCooldownBusy, now, 10*time.Second, "繁忙冷却: model busy"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkShortCooldown(accountID, aistudio.ChannelBuild, modelID, aistudio.ShortCooldownBusy, now, 10*time.Second, "繁忙冷却: Build busy"); err != nil {
		t.Fatal(err)
	}
	cooldowns, err := admin.Cooldowns(ctx)
	if err != nil || len(cooldowns) != 4 {
		t.Fatalf("独立冷却=%+v err=%v", cooldowns, err)
	}
	seen := make(map[string]api.AdminCooldown)
	for _, cooldown := range cooldowns {
		seen[cooldown.Scope+"/"+string(cooldown.Kind)] = cooldown
	}
	for key, want := range map[string]struct {
		channel string
		until   time.Time
	}{
		"gemini/":                {"playground", legacyUntil},
		"playground:gemini/race": {"playground", raceUntil},
		"playground:gemini/busy": {"playground", busyUntil},
		"build:gemini/busy":      {"build", busyUntil},
	} {
		if got, ok := seen[key]; !ok || got.Channel != want.channel || got.ModelID != modelID || !got.Until.Equal(want.until) || got.Reason == "" {
			t.Fatalf("%s=%+v", key, got)
		}
	}
	if err := admin.ClearCooldown(ctx, accountID, "playground:gemini", "unknown"); err == nil {
		t.Fatal("未知 kind 被接受")
	}
	if err := admin.ClearCooldown(ctx, accountID, "playground:gemini", aistudio.ShortCooldownRace); err != nil {
		t.Fatal(err)
	}
	cooldowns, _ = admin.Cooldowns(ctx)
	if len(cooldowns) != 3 || slices.ContainsFunc(cooldowns, func(c api.AdminCooldown) bool { return c.Kind == aistudio.ShortCooldownRace }) {
		t.Fatalf("清除 race 误删其他冷却=%+v", cooldowns)
	}
	if err := admin.ClearCooldown(ctx, accountID, modelID, ""); err != nil {
		t.Fatal(err)
	}
	cooldowns, _ = admin.Cooldowns(ctx)
	if len(cooldowns) != 2 || slices.ContainsFunc(cooldowns, func(c api.AdminCooldown) bool { return c.Kind == "" }) {
		t.Fatalf("缺省清除误删短冷却=%+v", cooldowns)
	}
	if err := admin.ClearCooldown(ctx, accountID, "playground:gemini", aistudio.ShortCooldownBusy); err != nil {
		t.Fatal(err)
	}
	cooldowns, _ = admin.Cooldowns(ctx)
	if len(cooldowns) != 1 || cooldowns[0].Channel != "build" || cooldowns[0].Kind != aistudio.ShortCooldownBusy {
		t.Fatalf("清除 busy 误删其他通道=%+v", cooldowns)
	}
}

// TestAdminConfig 验证凭据保存、密码保留与管理重启标记
func TestAdminConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	cfg := config.Default()
	cfg.AdminAuthEnabled = true
	cfg.AdminUsername = "operator"
	cfg.AdminPassword = "test-password"
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := aistudio.NewAccountPool(nil, 1)
	admin := &runtimeAdmin{configPath: path, requests: newRequestRegistry(ctx), pool: pool, service: &trackedService{pool: pool}}
	value, err := admin.RuntimeConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "test-password") || !value.AdminPasswordSet || !value.BuildNativeNonstream {
		t.Fatalf("config=%s", raw)
	}
	value.Proxy = "http://127.0.0.1:1234"
	value.AutoStart = true
	if _, err := admin.UpdateRuntimeConfig(ctx, value); err != nil {
		t.Fatal(err)
	}
	saved, err := config.Load(path)
	if err != nil || saved.AdminPassword != cfg.AdminPassword {
		t.Fatalf("password changed: %v", err)
	}
	if !saved.AutoStart {
		t.Fatal("auto_start was not saved")
	}
	manager := &runtimeManager{activeManagement: cfg}
	password := "new-test-password"
	value.AdminPassword = &password
	value, err = admin.UpdateRuntimeConfig(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decorated := manager.decorateRuntimeConfig(value, cfg)
	if !decorated.ManagementRestartRequired || decorated.AdminPassword != nil {
		t.Fatalf("restart=%+v", decorated)
	}
	value.AdminPassword = new(string)
	_, err = admin.UpdateRuntimeConfig(ctx, value)
	var status interface{ HTTPStatus() int }
	if !errors.As(err, &status) || status.HTTPStatus() != http.StatusBadRequest {
		t.Fatalf("empty enabled password error = %v", err)
	}
	value.AdminAuthEnabled = false
	if _, err := admin.UpdateRuntimeConfig(ctx, value); err != nil {
		t.Fatal(err)
	}
}
