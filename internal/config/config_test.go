package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Code-oclock/hawk-synthetic-monitor/internal/config"
)

func env(extra map[string]string) func(string) string {
	base := map[string]string{
		"HAWK_COLLECTOR_URL":     "http://c/",
		"HAWK_API_URL":           "http://a/graphql",
		"HAWK_PROJECT_ID":        "p",
		"HAWK_INTEGRATION_TOKEN": "t",
		"HAWK_EMAIL":             "e@x",
		"HAWK_PASSWORD":          "pw",
	}
	for k, v := range extra {
		base[k] = v
	}

	return func(key string) string { return base[key] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Interval != 10*time.Minute || cfg.MaxAttempts != 3 || cfg.HTTPAddr != ":8080" || cfg.CleanupEnabled {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadMissingRequired(t *testing.T) {
	_, err := config.Load(func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "HAWK_COLLECTOR_URL") || !strings.Contains(err.Error(), "HAWK_PASSWORD") {
		t.Fatalf("expected all missing keys reported, got %v", err)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	cases := map[string]map[string]string{
		"bad duration":   {"MONITOR_INTERVAL": "soon"},
		"negative retry": {"MONITOR_MAX_ATTEMPTS": "0"},
		"bad bool":       {"MONITOR_CLEANUP": "maybe"},
		"short run":      {"MONITOR_RUN_TIMEOUT": "5s"},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := config.Load(env(extra)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
