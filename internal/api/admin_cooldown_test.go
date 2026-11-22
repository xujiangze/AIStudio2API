package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
)

// cooldownAdmin 记录 HTTP 冷却操作传入的账户、作用域与可选类型。
type cooldownAdmin struct {
	AdminService
	cooldowns []AdminCooldown
	cleared   []struct {
		account, scope string
		kind           aistudio.ShortCooldownKind
	}
}

func (admin *cooldownAdmin) Cooldowns(context.Context) ([]AdminCooldown, error) {
	return admin.cooldowns, nil
}

func (admin *cooldownAdmin) ClearCooldown(_ context.Context, accountID, scope string, kind aistudio.ShortCooldownKind) error {
	admin.cleared = append(admin.cleared, struct {
		account, scope string
		kind           aistudio.ShortCooldownKind
	}{accountID, scope, kind})
	return nil
}

// TestAdminClearCooldownKind 验证缺省清除旧记录、指定短项精确转发，未知 kind 不进入服务层。
func TestAdminClearCooldownKind(t *testing.T) {
	for _, test := range []struct {
		name, scope, query string
		kind               aistudio.ShortCooldownKind
		status             int
	}{
		{"legacy", "gemini", "", "", http.StatusNoContent},
		{"race", "playground:gemini", "?kind=race", aistudio.ShortCooldownRace, http.StatusNoContent},
		{"busy", "build:gemini", "?kind=busy", aistudio.ShortCooldownBusy, http.StatusNoContent},
		{"unknown", "playground:gemini", "?kind=other", "", http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			admin := &cooldownAdmin{}
			handler := NewHandler(nil, Config{Admin: admin})
			request := httptest.NewRequest(http.MethodDelete, "http://localhost/api/accounts/a%40example.com/cooldowns/"+test.scope+test.query, nil)
			request.RemoteAddr = "127.0.0.1:5000"
			request.Header.Set("Origin", "http://localhost")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("清除=%d %s", response.Code, response.Body)
			}
			if test.status == http.StatusBadRequest {
				if len(admin.cleared) != 0 {
					t.Fatalf("未知 kind 转发到了服务层: %+v", admin.cleared)
				}
				return
			}
			if len(admin.cleared) != 1 || admin.cleared[0].account != "a@example.com" || admin.cleared[0].scope != test.scope || admin.cleared[0].kind != test.kind {
				t.Fatalf("清除目标=%+v", admin.cleared)
			}
		})
	}
}

// TestAdminCooldownKindJSON 验证同模型不同原因的列表项均保留，旧记录省略 kind。
func TestAdminCooldownKindJSON(t *testing.T) {
	until := time.Now().Add(time.Minute).UTC()
	admin := &cooldownAdmin{cooldowns: []AdminCooldown{
		{AccountID: "a@example.com", Channel: "playground", ModelID: "gemini", Scope: "gemini", Until: until},
		{AccountID: "a@example.com", Channel: "playground", ModelID: "gemini", Scope: "playground:gemini", Kind: aistudio.ShortCooldownRace, Until: until},
		{AccountID: "a@example.com", Channel: "playground", ModelID: "gemini", Scope: "playground:gemini", Kind: aistudio.ShortCooldownBusy, Until: until},
	}}
	handler := NewHandler(nil, Config{Admin: admin})
	request := httptest.NewRequest(http.MethodGet, "http://localhost/api/cooldowns", nil)
	request.RemoteAddr = "127.0.0.1:5000"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var result struct {
		Cooldowns []map[string]json.RawMessage `json:"cooldowns"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result.Cooldowns) != 3 {
		t.Fatalf("冷却列表=%d %s", response.Code, response.Body)
	}
	if _, present := result.Cooldowns[0]["kind"]; present || string(result.Cooldowns[1]["kind"]) != `"race"` || string(result.Cooldowns[2]["kind"]) != `"busy"` {
		t.Fatalf("kind JSON=%s", response.Body)
	}
}
