package main

import (
	"os"
	"testing"
)

func TestEnvOrDefault(t *testing.T) {
	// Test default value
	val := envOrDefault("TEST_NONEXISTENT_VAR_12345", "default")
	if val != "default" {
		t.Fatalf("expected default, got %q", val)
	}

	// Test env var override
	os.Setenv("TEST_ENV_OR_DEFAULT", "fromenv")
	defer os.Unsetenv("TEST_ENV_OR_DEFAULT")
	val = envOrDefault("TEST_ENV_OR_DEFAULT", "default")
	if val != "fromenv" {
		t.Fatalf("expected fromenv, got %q", val)
	}

	// Test empty env var still overrides default
	os.Setenv("TEST_ENV_OR_DEFAULT_EMPTY", "")
	defer os.Unsetenv("TEST_ENV_OR_DEFAULT_EMPTY")
	val = envOrDefault("TEST_ENV_OR_DEFAULT_EMPTY", "default")
	if val != "" {
		t.Fatalf("expected empty string, got %q", val)
	}
}

func TestDetectOutboundIP(t *testing.T) {
	ip, err := detectOutboundIP()
	if err != nil {
		t.Skipf("cannot detect outbound IP (no network?): %v", err)
	}
	if ip == "" {
		t.Fatal("detected empty IP")
	}
	if ip == "127.0.0.1" {
		t.Fatal("detected loopback")
	}
}
