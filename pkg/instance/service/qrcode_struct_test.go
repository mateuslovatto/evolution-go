package instance_service

import (
	"encoding/json"
	"testing"
)

func TestQrcodeStructMarshalKeepsLegacyKeys(t *testing.T) {
	b, err := json.Marshal(&QrcodeStruct{Qrcode: "data:image/png;base64,AAA", Code: "2@abc"})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"Qrcode", "qrcode"} {
		if m[k] != "data:image/png;base64,AAA" {
			t.Errorf("key %q = %v, want qrcode value (json: %s)", k, m[k], b)
		}
	}
	for _, k := range []string{"Code", "code"} {
		if m[k] != "2@abc" {
			t.Errorf("key %q = %v, want code value (json: %s)", k, m[k], b)
		}
	}
	if _, ok := m["passkeyStage"]; ok {
		t.Errorf("empty passkeyStage should be omitted (json: %s)", b)
	}

	// Value (non-pointer) marshaling, as used inside response maps.
	b2, _ := json.Marshal(QrcodeStruct{Qrcode: "q", Code: "c", PasskeyStage: "challenge"})
	var m2 map[string]interface{}
	_ = json.Unmarshal(b2, &m2)
	if m2["Qrcode"] != "q" || m2["code"] != "c" || m2["passkeyStage"] != "challenge" {
		t.Errorf("unexpected value marshal: %s", b2)
	}
}
