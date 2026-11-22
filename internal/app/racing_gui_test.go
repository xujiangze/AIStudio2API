package app

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/api"
	"github.com/Mag1cFall/AIStudio2API/internal/config"
	"github.com/Mag1cFall/AIStudio2API/internal/webui"
)

// TestRacingGUIFixture 只在显式设置本机监听时装配离线管理页，不载入真实账户或请求上游
func TestRacingGUIFixture(t *testing.T) {
	address := os.Getenv("AISTUDIO_RACING_GUI_FIXTURE")
	if address == "" {
		t.Skip("set AISTUDIO_RACING_GUI_FIXTURE to a loopback address for manual GUI verification")
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("GUI fixture must only listen on a loopback address")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), ".env")
	cfg := config.Default()
	cfg.LogFile = false
	cfg.AuthStates = filepath.Join(t.TempDir(), "accounts")
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	service := newRacingService(t, func(context.Context, aistudio.GenerateRequest) (<-chan aistudio.Event, error) {
		events := make(chan aistudio.Event)
		close(events)
		return events, nil
	})
	requests := service.requests
	admin := &runtimeAdmin{configPath: path, pool: service.pool, store: &aistudio.AccountStore{}, requests: requests, service: service}
	manager := &runtimeManager{
		lifecycle: ctx, configPath: path, activeManagement: cfg, requests: requests, logs: &logOutput{},
		current: &runtimeGeneration{service: service, admin: admin, config: cfg},
	}
	manager.settings.Store(requestSettingsOf(cfg))
	for _, kind := range []aistudio.ShortCooldownKind{aistudio.ShortCooldownRace, aistudio.ShortCooldownBusy} {
		if _, err := service.pool.MarkShortCooldown("a", aistudio.ChannelPlayground, "gemini", kind, time.Now(), 10*time.Minute, "离线验证: "+string(kind)); err != nil {
			t.Fatal(err)
		}
	}
	handler := api.NewHandler(manager, api.Config{Admin: manager})
	mux := http.NewServeMux()
	mux.Handle("/api/", handler)
	mux.Handle("/v1/", handler)
	mux.Handle("/", webui.Handler())
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	t.Logf("Offline GUI fixture ready at http://%s", listener.Addr())
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}
