package config

import "testing"

func TestParseWebhookMaxMediaBytes(t *testing.T) {
	cases := map[string]int64{
		"":         DefaultWebhookMaxMediaBytes,
		"  ":       DefaultWebhookMaxMediaBytes,
		"abc":      DefaultWebhookMaxMediaBytes,
		"10485760": 10485760,
		" 2048 ":   2048,
		"0":        0,
		"-1":       -1,
	}
	for in, want := range cases {
		if got := parseWebhookMaxMediaBytes(in); got != want {
			t.Errorf("parse(%q) = %d, want %d", in, got, want)
		}
	}
	if DefaultWebhookMaxMediaBytes != 10485760 {
		t.Fatalf("padrão deve ser 10485760, é %d", DefaultWebhookMaxMediaBytes)
	}
}
