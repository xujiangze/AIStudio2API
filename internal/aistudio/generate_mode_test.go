package aistudio_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
)

func TestFirstEventStreamingBuildBenefitUsesPlaygroundEntry(t *testing.T) {
	transport := &modeTestTransport{err: errors.New("stop before upstream")}
	client := aistudio.NewClient(aistudio.ClientOptions{Transport: transport, Protected: transport})
	models := applyModeTestCatalog(t, client, "account", []modeTestModel{{id: "gemini-fixture", accessModes: []int64{3}}})
	pool := aistudio.NewAccountPool([]*aistudio.Account{{
		ID: "account", Config: aistudio.AccountConfig{Enabled: true}, State: aistudio.AccountReady,
		BenefitTier: aistudio.BenefitTierUltra, Models: models,
	}}, 1)
	pool.SetUpstreamChannels([]aistudio.Channel{aistudio.ChannelPlayground, aistudio.ChannelBuild})
	if err := pool.SetBuildCatalog("account", []aistudio.Model{{
		ID: "gemini-fixture", Methods: []string{"generateContent"}, OutputTokenLimit: 1024,
	}}); err != nil {
		t.Fatal(err)
	}
	service := aistudio.NewPooledService(pool, client)
	lease, _, err := pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{
		AccountID: "account", ModelID: "gemini-fixture", Method: "generateContent", Channel: aistudio.ChannelBuild,
	})
	if err != nil || lease == nil {
		t.Fatalf("lease=%v err=%v", lease, err)
	}
	t.Cleanup(func() {
		if err := lease.Release(); err != nil {
			t.Error(err)
		}
	})
	request := aistudio.GenerateRequest{
		Model: "models/gemini-fixture", Contents: []aistudio.Content{{Role: aistudio.RoleUser, Parts: []aistudio.Part{{Text: "hi"}}}},
	}
	ctx := aistudio.ContextWithAccountLease(context.Background(), lease)
	if service.FirstEventStreaming(ctx, request, lease) {
		t.Fatal("Build benefit model must not enable first-event protection, even with Unary=false")
	}
	if service.FirstEventStreamingForAccount(ctx, request, "account", aistudio.ChannelBuild) {
		t.Fatal("candidate prefilter must use the preferred Playground entry, not the free Build entry")
	}
	var method, mode string
	ctx = aistudio.ContextWithUpstreamModeObserver(ctx, func(actualMethod, actualMode string, _ int, _ string) {
		method, mode = actualMethod, actualMode
	})
	if _, err := service.Generate(ctx, request); !errors.Is(err, transport.err) {
		t.Fatalf("Generate error=%v, want transport stop", err)
	}
	if method != "ProxyUnaryCall" || mode != "native" || transport.protectedCalls != 1 {
		t.Fatalf("actual route=%s/%s calls=%d, want ProxyUnaryCall/native", method, mode, transport.protectedCalls)
	}
}

func TestFirstEventStreamingMatchesGenerateMode(t *testing.T) {
	for _, test := range []struct {
		name          string
		channel       aistudio.Channel
		unary         bool
		accessModes   []int64
		buildAccess   []int64
		capabilities  []int
		interaction   bool
		wantStreaming bool
		wantMethod    string
		wantMode      string
	}{
		{name: "Playground stream", channel: aistudio.ChannelPlayground, wantStreaming: true, wantMethod: "GenerateContent", wantMode: "stream"},
		{name: "Playground collects stream for Unary", channel: aistudio.ChannelPlayground, unary: true, wantStreaming: true, wantMethod: "GenerateContent", wantMode: "stream"},
		{name: "Playground benefit still streams", channel: aistudio.ChannelPlayground, accessModes: []int64{4}, wantStreaming: true, wantMethod: "GenerateContent", wantMode: "stream"},
		{name: "Build stream", channel: aistudio.ChannelBuild, wantStreaming: true, wantMethod: "ProxyStreamedCall", wantMode: "stream"},
		{name: "Build explicit Unary", channel: aistudio.ChannelBuild, unary: true, wantMethod: "ProxyUnaryCall", wantMode: "native"},
		{name: "Build Pro benefit implies native", channel: aistudio.ChannelBuild, accessModes: []int64{3}, wantMethod: "ProxyUnaryCall", wantMode: "native"},
		{name: "Build Ultra benefit implies native", channel: aistudio.ChannelBuild, accessModes: []int64{4}, wantMethod: "ProxyUnaryCall", wantMode: "native"},
		{name: "Build prefers free Playground over paid Build", channel: aistudio.ChannelBuild, buildAccess: []int64{4}, wantStreaming: true, wantMethod: "ProxyStreamedCall", wantMode: "stream"},
		{name: "Playground image stream", channel: aistudio.ChannelPlayground, capabilities: []int{1, 21}, wantStreaming: true, wantMethod: "GenerateContent", wantMode: "stream"},
		{name: "Build speech stream", channel: aistudio.ChannelBuild, capabilities: []int{1, 37}, wantStreaming: true, wantMethod: "ProxyStreamedCall", wantMode: "stream"},
		{name: "foreground Interaction stream", channel: aistudio.ChannelPlayground, interaction: true, wantStreaming: true, wantMethod: "CreateInteractionStream", wantMode: "stream"},
		{name: "foreground Interaction Unary still streams", channel: aistudio.ChannelPlayground, interaction: true, unary: true, wantStreaming: true, wantMethod: "CreateInteractionStream", wantMode: "stream"},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &modeTestTransport{err: errors.New("stop before upstream")}
			client := aistudio.NewClient(aistudio.ClientOptions{Transport: transport, Protected: transport})
			models := applyModeTestCatalog(t, client, "account", []modeTestModel{{
				id: "gemini-fixture", accessModes: test.accessModes, capabilities: test.capabilities, interaction: test.interaction,
			}})
			pool := aistudio.NewAccountPool([]*aistudio.Account{{
				ID: "account", Config: aistudio.AccountConfig{Enabled: true}, State: aistudio.AccountReady,
				BenefitTier: aistudio.BenefitTierUltra, Models: models,
			}}, 1)
			pool.SetUpstreamChannels([]aistudio.Channel{aistudio.ChannelPlayground, aistudio.ChannelBuild})
			if err := pool.SetBuildCatalog("account", []aistudio.Model{{
				ID: "gemini-fixture", Methods: []string{"generateContent"}, OutputTokenLimit: 1024, AccessModes: test.buildAccess,
			}}); err != nil {
				t.Fatal(err)
			}
			service := aistudio.NewPooledService(pool, client)
			lease, _, err := pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{
				AccountID: "account", ModelID: "gemini-fixture", Method: "generateContent", Channel: test.channel,
			})
			if err != nil || lease == nil {
				t.Fatalf("lease=%v err=%v", lease, err)
			}
			t.Cleanup(func() {
				if err := lease.Release(); err != nil {
					t.Error(err)
				}
			})
			request := aistudio.GenerateRequest{
				Model: "models/gemini-fixture", Unary: test.unary,
				Contents: []aistudio.Content{{Role: aistudio.RoleUser, Parts: []aistudio.Part{{Text: "hi"}}}},
			}
			ctx := aistudio.ContextWithAccountLease(context.Background(), lease)
			if got := service.FirstEventStreaming(ctx, request, lease); got != test.wantStreaming {
				t.Errorf("lease mode=%v, want %v", got, test.wantStreaming)
			}
			if got := service.FirstEventStreamingForAccount(ctx, request, "account", test.channel); got != test.wantStreaming {
				t.Errorf("candidate mode=%v, want %v", got, test.wantStreaming)
			}
			if transport.protectedCalls != 0 || transport.unprotectedCalls != 0 {
				t.Fatal("mode queries must not send requests")
			}
			var method, mode string
			ctx = aistudio.ContextWithUpstreamModeObserver(ctx, func(actualMethod, actualMode string, _ int, _ string) {
				method, mode = actualMethod, actualMode
			})
			if _, err := service.Generate(ctx, request); !errors.Is(err, transport.err) {
				t.Fatalf("Generate error=%v, want transport stop", err)
			}
			if method != test.wantMethod || mode != test.wantMode {
				t.Errorf("actual route=%s/%s, want %s/%s", method, mode, test.wantMethod, test.wantMode)
			}
		})
	}
}

func TestFirstEventStreamingExcludesBackgroundAndAssistantRoutes(t *testing.T) {
	client := aistudio.NewClient(aistudio.ClientOptions{})
	models := applyModeTestCatalog(t, client, "account", []modeTestModel{
		{id: "gemini-fixture"},
		{id: "gemini-agent", interaction: true, background: true},
		{id: "gemini-assistant", capabilities: []int{1, 48}},
	})
	pool := aistudio.NewAccountPool([]*aistudio.Account{{
		ID: "account", Config: aistudio.AccountConfig{Enabled: true}, State: aistudio.AccountReady,
		BenefitTier: aistudio.BenefitTierUltra, Models: models,
	}}, 1)
	service := aistudio.NewPooledService(pool, client)
	for _, test := range []struct {
		name    string
		request aistudio.GenerateRequest
		channel aistudio.Channel
	}{
		{name: "background", request: aistudio.GenerateRequest{Model: "gemini-fixture", Background: true}, channel: aistudio.ChannelPlayground},
		{name: "background Build", request: aistudio.GenerateRequest{Model: "gemini-fixture", Background: true}, channel: aistudio.ChannelBuild},
		{name: "Interaction task", request: aistudio.GenerateRequest{Model: "gemini-agent"}, channel: aistudio.ChannelPlayground},
		{name: "assistant Playground", request: aistudio.GenerateRequest{Model: "gemini-assistant-build"}, channel: aistudio.ChannelPlayground},
		{name: "assistant Build", request: aistudio.GenerateRequest{Model: "gemini-assistant-build"}, channel: aistudio.ChannelBuild},
		{name: "assistant channel", request: aistudio.GenerateRequest{Model: "gemini-fixture"}, channel: aistudio.ChannelAssistant},
		{name: "unknown channel", request: aistudio.GenerateRequest{Model: "gemini-fixture"}, channel: aistudio.Channel("other")},
		{name: "no selected channel", request: aistudio.GenerateRequest{Model: "gemini-fixture"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if service.FirstEventStreamingForAccount(context.Background(), test.request, "account", test.channel) {
				t.Fatal("excluded or unknown route must not enable first-event protection")
			}
		})
	}
	if service.FirstEventStreaming(context.Background(), aistudio.GenerateRequest{Model: "gemini-fixture"}, nil) {
		t.Fatal("missing lease must not enable first-event protection")
	}
}

func TestFirstEventStreamingForAccountUsesCandidateCatalog(t *testing.T) {
	client := aistudio.NewClient(aistudio.ClientOptions{})
	applyModeTestCatalog(t, client, "original", []modeTestModel{{id: "gemini-fixture"}})
	applyModeTestCatalog(t, client, "candidate", []modeTestModel{{id: "gemini-fixture", accessModes: []int64{4}}})
	service := aistudio.NewPooledService(aistudio.NewAccountPool(nil, 1), client)
	request := aistudio.GenerateRequest{Model: "models/gemini-fixture", AccountID: "original"}
	if !service.FirstEventStreamingForAccount(context.Background(), request, "original", aistudio.ChannelBuild) {
		t.Fatal("original free model should stream on Build")
	}
	if service.FirstEventStreamingForAccount(context.Background(), request, "candidate", aistudio.ChannelBuild) {
		t.Fatal("candidate benefit entry must win over request.AccountID and other account catalogs")
	}
	if service.FirstEventStreamingForAccount(context.Background(), request, "uncached", aistudio.ChannelBuild) {
		t.Fatal("other accounts' cached models cannot determine an uncached candidate mode")
	}
}

func TestFirstEventStreamingUnknownBuildEntryDoesNotLoadOrGuessFallback(t *testing.T) {
	for _, test := range []struct {
		name           string
		warmCatalog    bool
		buildAccess    []int64
		reloadedModels []modeTestModel
		wantMethod     string
		wantMode       string
	}{
		{name: "cold Build-only free", wantMethod: "ProxyStreamedCall", wantMode: "stream"},
		{name: "warm Build-only free", warmCatalog: true, wantMethod: "ProxyStreamedCall", wantMode: "stream"},
		{name: "Build-only benefit native", warmCatalog: true, buildAccess: []int64{4}, wantMethod: "ProxyUnaryCall", wantMode: "native"},
		{name: "reload adds preferred benefit Playground entry", warmCatalog: true, reloadedModels: []modeTestModel{{id: "gemini-fallback", accessModes: []int64{4}}}, wantMethod: "ProxyUnaryCall", wantMode: "native"},
		{name: "reload adds preferred free Playground entry", warmCatalog: true, buildAccess: []int64{4}, reloadedModels: []modeTestModel{{id: "gemini-fallback"}}, wantMethod: "ProxyStreamedCall", wantMode: "stream"},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &modeTestTransport{err: errors.New("stop before upstream")}
			pages := &modeTestPageProvider{page: modeTestPageCatalog(t, test.reloadedModels)}
			client := aistudio.NewClient(aistudio.ClientOptions{Transport: transport, Protected: transport, Pages: pages})
			models := []aistudio.Model{{ID: "unrelated", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}}}
			if test.warmCatalog {
				models = applyModeTestCatalog(t, client, "account", []modeTestModel{{id: "unrelated"}})
			}
			pool := aistudio.NewAccountPool([]*aistudio.Account{{
				ID: "account", Config: aistudio.AccountConfig{Enabled: true}, State: aistudio.AccountReady,
				BenefitTier: aistudio.BenefitTierUltra, Models: models,
			}}, 1)
			pool.SetUpstreamChannels([]aistudio.Channel{aistudio.ChannelPlayground, aistudio.ChannelBuild})
			if err := pool.SetBuildCatalog("account", []aistudio.Model{{
				ID: "gemini-fallback", Methods: []string{"generateContent"}, OutputTokenLimit: 1024, AccessModes: test.buildAccess,
			}}); err != nil {
				t.Fatal(err)
			}
			service := aistudio.NewPooledService(pool, client)
			lease, _, err := pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{
				AccountID: "account", ModelID: "gemini-fallback", Method: "generateContent", Channel: aistudio.ChannelBuild,
			})
			if err != nil || lease == nil {
				t.Fatalf("lease=%v err=%v", lease, err)
			}
			t.Cleanup(func() {
				if err := lease.Release(); err != nil {
					t.Error(err)
				}
			})
			request := aistudio.GenerateRequest{
				Model: "models/gemini-fallback", Contents: []aistudio.Content{{Role: aistudio.RoleUser, Parts: []aistudio.Part{{Text: "hi"}}}},
			}
			ctx := aistudio.ContextWithAccountLease(context.Background(), lease)
			if service.FirstEventStreaming(ctx, request, lease) || service.FirstEventStreamingForAccount(ctx, request, "account", aistudio.ChannelBuild) {
				t.Fatal("missing Playground entry must be conservatively excluded, even if Build entry is cached")
			}
			if pages.calls != 0 || transport.protectedCalls != 0 || transport.unprotectedCalls != 0 {
				t.Fatalf("query caused side effects: page=%d protected=%d unprotected=%d", pages.calls, transport.protectedCalls, transport.unprotectedCalls)
			}
			var method, mode string
			ctx = aistudio.ContextWithUpstreamModeObserver(ctx, func(actualMethod, actualMode string, _ int, _ string) {
				method, mode = actualMethod, actualMode
			})
			if _, err := service.Generate(ctx, request); !errors.Is(err, transport.err) {
				t.Fatalf("Generate error=%v, want transport stop", err)
			}
			if method != test.wantMethod || mode != test.wantMode || pages.calls != 1 {
				t.Errorf("actual route=%s/%s page loads=%d, want %s/%s and existing reload behavior", method, mode, pages.calls, test.wantMethod, test.wantMode)
			}
		})
	}
}

func TestFirstEventStreamingModeDistinguishesUnknownFromNative(t *testing.T) {
	transport := &modeTestTransport{err: errors.New("stop before upstream")}
	pages := &modeTestPageProvider{page: modeTestPageCatalog(t, nil)}
	client := aistudio.NewClient(aistudio.ClientOptions{Transport: transport, Protected: transport, Pages: pages})
	applyModeTestCatalog(t, client, "account", []modeTestModel{{id: "free"}, {id: "benefit", accessModes: []int64{4}}})
	service := aistudio.NewPooledService(aistudio.NewAccountPool(nil, 1), client)
	for _, test := range []struct {
		name      string
		accountID string
		request   aistudio.GenerateRequest
		channel   aistudio.Channel
		streaming bool
		known     bool
	}{
		{name: "free Build", accountID: "account", request: aistudio.GenerateRequest{Model: "free"}, channel: aistudio.ChannelBuild, streaming: true, known: true},
		{name: "benefit Build", accountID: "account", request: aistudio.GenerateRequest{Model: "benefit"}, channel: aistudio.ChannelBuild, known: true},
		{name: "explicit native Build", accountID: "account", request: aistudio.GenerateRequest{Model: "free", Unary: true}, channel: aistudio.ChannelBuild, known: true},
		{name: "Playground Unary", accountID: "account", request: aistudio.GenerateRequest{Model: "free", Unary: true}, channel: aistudio.ChannelPlayground, streaming: true, known: true},
		{name: "missing entry", accountID: "account", request: aistudio.GenerateRequest{Model: "build-only"}, channel: aistudio.ChannelBuild},
		{name: "missing catalog", accountID: "cold", request: aistudio.GenerateRequest{Model: "free"}, channel: aistudio.ChannelBuild},
	} {
		t.Run(test.name, func(t *testing.T) {
			streaming, known := service.FirstEventStreamingMode(context.Background(), test.request, test.accountID, test.channel)
			if streaming != test.streaming || known != test.known {
				t.Fatalf("mode=(%v,%v), want (%v,%v)", streaming, known, test.streaming, test.known)
			}
			if got := service.FirstEventStreamingForAccount(context.Background(), test.request, test.accountID, test.channel); got != test.streaming {
				t.Fatalf("bool mode=%v, want %v", got, test.streaming)
			}
		})
	}
	if pages.calls != 0 || transport.protectedCalls != 0 || transport.unprotectedCalls != 0 {
		t.Fatal("mode query must remain side-effect-free")
	}
}

func TestGenerateStreamingOnlyAllowsUnknownBuildOnlyStream(t *testing.T) {
	for _, warmCatalog := range []bool{false, true} {
		name := "cold"
		if warmCatalog {
			name = "warm"
		}
		t.Run(name, func(t *testing.T) {
			transport := &modeTestTransport{err: errors.New("stop before upstream")}
			pages := &modeTestPageProvider{page: modeTestPageCatalog(t, nil)}
			client := aistudio.NewClient(aistudio.ClientOptions{Transport: transport, Protected: transport, Pages: pages})
			models := []aistudio.Model{{ID: "unrelated", Methods: []string{"generateContent"}, Capabilities: map[string]bool{"chat_model": true}}}
			if warmCatalog {
				models = applyModeTestCatalog(t, client, "account", []modeTestModel{{id: "unrelated"}})
			}
			pool := aistudio.NewAccountPool([]*aistudio.Account{{
				ID: "account", Config: aistudio.AccountConfig{Enabled: true}, State: aistudio.AccountReady,
				BenefitTier: aistudio.BenefitTierFree, Models: models,
			}}, 1)
			pool.SetUpstreamChannels([]aistudio.Channel{aistudio.ChannelPlayground, aistudio.ChannelBuild})
			if err := pool.SetBuildCatalog("account", []aistudio.Model{{
				ID: "gemini-build-only", Methods: []string{"generateContent"}, OutputTokenLimit: 1024,
			}}); err != nil {
				t.Fatal(err)
			}
			service := aistudio.NewPooledService(pool, client)
			lease, _, err := pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{
				AccountID: "account", ModelID: "gemini-build-only", Method: "generateContent", Channel: aistudio.ChannelBuild,
			})
			if err != nil || lease == nil {
				t.Fatalf("lease=%v err=%v", lease, err)
			}
			defer lease.Release()
			request := aistudio.GenerateRequest{
				Model: "models/gemini-build-only", Contents: []aistudio.Content{{Role: aistudio.RoleUser, Parts: []aistudio.Part{{Text: "hi"}}}},
			}
			ctx := aistudio.ContextWithStreamingOnly(aistudio.ContextWithAccountLease(context.Background(), lease))
			if streaming, known := service.FirstEventStreamingMode(ctx, request, "account", aistudio.ChannelBuild); streaming || known {
				t.Fatalf("preflight=(%v,%v), want unknown Build-only", streaming, known)
			}
			var method, mode string
			ctx = aistudio.ContextWithUpstreamModeObserver(ctx, func(actualMethod, actualMode string, _ int, _ string) {
				method, mode = actualMethod, actualMode
			})
			if _, err := service.Generate(ctx, request); !errors.Is(err, transport.err) {
				t.Fatalf("stream-only Generate=%v, want actual streaming transport", err)
			}
			if method != "ProxyStreamedCall" || mode != "stream" || transport.protectedCalls != 1 || transport.unprotectedCalls != 0 || pages.calls != 1 {
				t.Fatalf("route=%s/%s protected=%d unprotected=%d reloads=%d", method, mode, transport.protectedCalls, transport.unprotectedCalls, pages.calls)
			}
		})
	}
}

func TestGenerateStreamingOnlyRejectsReloadedNativeBeforeRPC(t *testing.T) {
	transport := &modeTestTransport{err: errors.New("stop before upstream")}
	pages := &modeTestPageProvider{page: modeTestPageCatalog(t, []modeTestModel{{id: "gemini-build-only", accessModes: []int64{4}}})}
	client := aistudio.NewClient(aistudio.ClientOptions{Transport: transport, Protected: transport, Pages: pages})
	models := applyModeTestCatalog(t, client, "account", []modeTestModel{{id: "unrelated"}})
	pool := aistudio.NewAccountPool([]*aistudio.Account{{
		ID: "account", Config: aistudio.AccountConfig{Enabled: true}, State: aistudio.AccountReady,
		BenefitTier: aistudio.BenefitTierUltra, Models: models,
	}}, 1)
	pool.SetUpstreamChannels([]aistudio.Channel{aistudio.ChannelPlayground, aistudio.ChannelBuild})
	if err := pool.SetBuildCatalog("account", []aistudio.Model{{
		ID: "gemini-build-only", Methods: []string{"generateContent"}, OutputTokenLimit: 1024,
	}}); err != nil {
		t.Fatal(err)
	}
	service := aistudio.NewPooledService(pool, client)
	lease, _, err := pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{
		AccountID: "account", ModelID: "gemini-build-only", Method: "generateContent", Channel: aistudio.ChannelBuild,
	})
	if err != nil || lease == nil {
		t.Fatalf("lease=%v err=%v", lease, err)
	}
	defer lease.Release()
	request := aistudio.GenerateRequest{
		Model: "models/gemini-build-only", Contents: []aistudio.Content{{Role: aistudio.RoleUser, Parts: []aistudio.Part{{Text: "hi"}}}},
	}
	ctx := aistudio.ContextWithAccountLease(context.Background(), lease)
	if streaming, known := service.FirstEventStreamingMode(ctx, request, "account", aistudio.ChannelBuild); streaming || known {
		t.Fatalf("pre-reload mode=(%v,%v), want unknown", streaming, known)
	}
	observations := 0
	var method, mode string
	ctx = aistudio.ContextWithUpstreamModeObserver(ctx, func(actualMethod, actualMode string, _ int, _ string) {
		observations++
		method, mode = actualMethod, actualMode
	})
	if events, err := service.Generate(aistudio.ContextWithStreamingOnly(ctx), request); !errors.Is(err, aistudio.ErrFirstEventNonstreaming) || events != nil {
		t.Fatalf("streaming-only result=(%v,%v), want nonstreaming sentinel", events, err)
	}
	if pages.calls != 1 || transport.protectedCalls != 0 || transport.unprotectedCalls != 0 || observations != 0 {
		t.Fatalf("native exclusion sent work: reload=%d protected=%d unprotected=%d observed=%d", pages.calls, transport.protectedCalls, transport.unprotectedCalls, observations)
	}
	if streaming, known := service.FirstEventStreamingMode(ctx, request, "account", aistudio.ChannelBuild); streaming || !known {
		t.Fatalf("post-reload mode=(%v,%v), want known native", streaming, known)
	}
	if _, err := service.Generate(ctx, request); !errors.Is(err, transport.err) {
		t.Fatalf("ordinary request changed: %v", err)
	}
	if method != "ProxyUnaryCall" || mode != "native" || transport.protectedCalls != 1 || transport.unprotectedCalls != 0 || observations != 1 {
		t.Fatalf("ordinary route=%s/%s protected=%d unprotected=%d observed=%d", method, mode, transport.protectedCalls, transport.unprotectedCalls, observations)
	}
}

// TestGenerateUnknownBuildOnlyModeObservedBeforeTransport 给调度层提供先未知、发送时才确认流式的离线缝。
func TestGenerateUnknownBuildOnlyModeObservedBeforeTransport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type upstreamMode struct {
			method string
			mode   string
			round  int
		}
		observed := make(chan upstreamMode, 1)
		transport := &modeTestBlockingTransport{entered: make(chan aistudio.RPCRequest, 1)}
		pages := &modeTestPageProvider{page: modeTestPageCatalog(t, nil)}
		client := aistudio.NewClient(aistudio.ClientOptions{Transport: transport, Protected: transport, Pages: pages})
		models := applyModeTestCatalog(t, client, "account", []modeTestModel{{id: "unrelated"}})
		pool := aistudio.NewAccountPool([]*aistudio.Account{{
			ID: "account", Config: aistudio.AccountConfig{Enabled: true}, State: aistudio.AccountReady,
			BenefitTier: aistudio.BenefitTierFree, Models: models,
		}}, 1)
		pool.SetUpstreamChannels([]aistudio.Channel{aistudio.ChannelPlayground, aistudio.ChannelBuild})
		if err := pool.SetBuildCatalog("account", []aistudio.Model{{
			ID: "gemini-build-only", Methods: []string{"generateContent"}, OutputTokenLimit: 1024,
		}}); err != nil {
			t.Fatal(err)
		}
		service := aistudio.NewPooledService(pool, client)
		lease, _, err := pool.TryAcquireFor(context.Background(), aistudio.AccountSelection{
			AccountID: "account", ModelID: "gemini-build-only", Method: "generateContent", Channel: aistudio.ChannelBuild,
		})
		if err != nil || lease == nil {
			t.Fatalf("lease=%v err=%v", lease, err)
		}
		defer lease.Release()
		request := aistudio.GenerateRequest{
			Model: "models/gemini-build-only", Contents: []aistudio.Content{{Role: aistudio.RoleUser, Parts: []aistudio.Part{{Text: "hi"}}}},
		}
		ctx, cancel := context.WithCancel(aistudio.ContextWithAccountLease(context.Background(), lease))
		defer cancel()
		ctx = aistudio.ContextWithUpstreamModeObserver(ctx, func(method, mode string, round int, _ string) {
			observed <- upstreamMode{method: method, mode: mode, round: round}
		})
		if service.FirstEventStreaming(ctx, request, lease) || service.FirstEventStreamingForAccount(ctx, request, "account", aistudio.ChannelBuild) {
			t.Fatal("missing Playground entry must remain unknown before its existing reload")
		}
		if pages.calls != 0 {
			t.Fatal("mode preflight loaded a page")
		}
		result := make(chan error, 1)
		go func() {
			_, err := service.Generate(ctx, request)
			result <- err
		}()
		synctest.Wait()
		select {
		case mode := <-observed:
			if mode.method != "ProxyStreamedCall" || mode.mode != "stream" || mode.round != 0 {
				t.Fatalf("runtime mode=%+v, want initial Build stream", mode)
			}
		default:
			t.Fatal("Generate must reveal actual stream mode before waiting for transport or an upstream event")
		}
		select {
		case rpc := <-transport.entered:
			if rpc.Method != "ProxyStreamedCall" || !rpc.Streaming {
				t.Fatalf("actual send=%+v, want streaming Build RPC", rpc)
			}
		default:
			t.Fatal("controlled transport not entered")
		}
		if pages.calls != 1 {
			t.Fatalf("page loads=%d, want preserved reload before Build fallback", pages.calls)
		}
		// Build-only 仍没有 PG 条目，单纯重复缓存查询不能代替实际发送的模式通知。
		if service.FirstEventStreaming(ctx, request, lease) {
			t.Fatal("cached preflight must not pretend Build-only became known")
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("Generate completed before the controlled transport was canceled: %v", err)
		default:
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Generate cancellation=%v", err)
		}
	})
}

type modeTestBlockingTransport struct {
	entered chan aistudio.RPCRequest
}

func (*modeTestBlockingTransport) Do(context.Context, aistudio.RPCRequest) (*aistudio.RPCResponse, error) {
	return nil, errors.New("unexpected unprotected RPC")
}

func (transport *modeTestBlockingTransport) DoProtected(ctx context.Context, _ aistudio.GenerateRequest, rpc aistudio.RPCRequest) (*aistudio.RPCResponse, error) {
	transport.entered <- rpc
	<-ctx.Done()
	return nil, ctx.Err()
}

type modeTestModel struct {
	id           string
	accessModes  []int64
	capabilities []int
	interaction  bool
	background   bool
}

func applyModeTestCatalog(t *testing.T, client *aistudio.Client, accountID string, models []modeTestModel) []aistudio.Model {
	t.Helper()
	_, parsed, err := client.ApplyPageCatalog(context.Background(), accountID, modeTestPageCatalog(t, models))
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func modeTestPageCatalog(t *testing.T, models []modeTestModel) camoufoxnative.PageCatalog {
	t.Helper()
	rows := make([]any, 0, len(models))
	for _, model := range models {
		row := make([]any, 83)
		row[0], row[3], row[5], row[6] = "models/"+model.id, model.id, 8192, 1024
		row[7], row[64], row[82] = []string{"generateContent"}, []int{1}, model.accessModes
		if model.capabilities != nil {
			row[64] = model.capabilities
		}
		if model.interaction {
			row[78] = []any{nil, nil, model.background, 2}
		}
		rows = append(rows, row)
	}
	raw, err := json.Marshal([]any{rows})
	if err != nil {
		t.Fatal(err)
	}
	return camoufoxnative.PageCatalog{Logging: []byte(`[]`), Tier: []byte(`[2]`), Models: raw}
}

type modeTestPageProvider struct {
	page  camoufoxnative.PageCatalog
	calls int
}

func (provider *modeTestPageProvider) PageCatalog(context.Context, string, bool) (camoufoxnative.PageCatalog, error) {
	provider.calls++
	return provider.page, nil
}

type modeTestTransport struct {
	err              error
	protectedCalls   int
	unprotectedCalls int
}

func (transport *modeTestTransport) Do(context.Context, aistudio.RPCRequest) (*aistudio.RPCResponse, error) {
	transport.unprotectedCalls++
	return nil, errors.New("unexpected unprotected RPC")
}

func (transport *modeTestTransport) DoProtected(context.Context, aistudio.GenerateRequest, aistudio.RPCRequest) (*aistudio.RPCResponse, error) {
	transport.protectedCalls++
	return nil, transport.err
}
