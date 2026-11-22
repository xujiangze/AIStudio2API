package config

import (
	"reflect"
	"testing"
)

// TestFirstEventDefaultsAndExplicitSelection 验证缺省配置启用 20 秒竞速与全部模型，同时保留旧显式关闭、指定模型与空列表。
func TestFirstEventDefaultsAndExplicitSelection(t *testing.T) {
	for _, test := range []struct {
		name, env, hedge, models string
	}{
		{"missing", "", "20", "*"},
		{"off", "FIRST_EVENT_HEDGE=off\nFIRST_EVENT_HEDGE_MODELS=gemini-4-argon\n", "off", "gemini-4-argon"},
		{"selected", "FIRST_EVENT_HEDGE=auto\nFIRST_EVENT_HEDGE_MODELS=gemini-4-argon,gemini-flash\n", "auto", "gemini-4-argon,gemini-flash"},
		{"empty", "FIRST_EVENT_HEDGE_MODELS=\n", "20", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeTestEnv(t, test.env)
			cfg, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.FirstEventHedge != test.hedge || cfg.FirstEventHedgeModels != test.models {
				t.Fatalf("竞速=%q 模型=%q", cfg.FirstEventHedge, cfg.FirstEventHedgeModels)
			}
			if cfg.FirstEventHedgeMaxConcurrency != 16 || cfg.FirstEventTimeoutSeconds != 60 ||
				cfg.FirstEventRaceCooldownSeconds != 30 || cfg.ModelBusyCooldownSeconds != 10 {
				t.Fatalf("首字策略默认值=%+v", cfg)
			}
			if err := cfg.Save(path); err != nil {
				t.Fatal(err)
			}
			reloaded, err := Load(path)
			if err != nil || !reflect.DeepEqual(cfg, reloaded) {
				t.Fatalf("保存重载后配置改变: %+v, err=%v", reloaded, err)
			}
		})
	}
}

// TestFirstEventSecondsValidationAndSave 验证全局备用并发必须为正、三个秒数可用 0 关闭，且保存拒绝非法值。
func TestFirstEventSecondsValidationAndSave(t *testing.T) {
	for _, field := range []struct {
		key      string
		value    func(*Config) *int
		zeroOK   bool
		fallback int
	}{
		{"FIRST_EVENT_HEDGE_MAX_CONCURRENCY", func(c *Config) *int { return &c.FirstEventHedgeMaxConcurrency }, false, 16},
		{"FIRST_EVENT_TIMEOUT_SECONDS", func(c *Config) *int { return &c.FirstEventTimeoutSeconds }, true, 60},
		{"FIRST_EVENT_RACE_COOLDOWN_SECONDS", func(c *Config) *int { return &c.FirstEventRaceCooldownSeconds }, true, 30},
		{"MODEL_BUSY_COOLDOWN_SECONDS", func(c *Config) *int { return &c.ModelBusyCooldownSeconds }, true, 10},
	} {
		t.Run(field.key, func(t *testing.T) {
			path := writeTestEnv(t, field.key+"=7\n")
			cfg, err := Load(path)
			if err != nil || *field.value(&cfg) != 7 {
				t.Fatalf("加载正整数: %+v, err=%v", cfg, err)
			}
			*field.value(&cfg) = 0
			if err := cfg.Save(path); (err == nil) != field.zeroOK {
				t.Fatalf("保存 0: err=%v, 可关闭=%v", err, field.zeroOK)
			}
			if field.zeroOK {
				loaded, err := Load(path)
				if err != nil || *field.value(&loaded) != 0 {
					t.Fatalf("关闭后重载: %+v, err=%v", loaded, err)
				}
			}
			before := readTestEnv(t, path)
			*field.value(&cfg) = -1
			if err := cfg.Save(path); err == nil {
				t.Fatal("保存接受了负数")
			}
			if got := readTestEnv(t, path); got != before {
				t.Fatal("无效配置改写了文件")
			}
			invalidValues := []string{"-1", "abc", "1.5", ""}
			if !field.zeroOK {
				invalidValues = append(invalidValues, "0")
			}
			for _, invalid := range invalidValues {
				loaded, err := Load(writeTestEnv(t, field.key+"="+invalid+"\n"))
				if err != nil || *field.value(&loaded) != field.fallback || len(loaded.Invalid) != 1 || loaded.Invalid[0].Key != field.key {
					t.Fatalf("无效值 %q: %+v, err=%v", invalid, loaded, err)
				}
			}
		})
	}
}

// TestValidateProxyCredentials 验证代理 URL 接受账号密码并拒绝空用户名
func TestValidateProxyCredentials(t *testing.T) {
	for _, value := range []string{"http://user:secret@127.0.0.1:8080", "socks5://user:secret@127.0.0.1:1080"} {
		if err := ValidateProxy(value); err != nil {
			t.Errorf("ValidateProxy(%q) = %v", value, err)
		}
	}
	if err := ValidateProxy("http://:secret@127.0.0.1:8080"); err == nil {
		t.Error("空用户名被接受")
	}
}
