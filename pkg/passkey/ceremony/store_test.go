package ceremony

import "testing"

func TestSetAwaitingConfirmationDoesNotRegressConfirmation(t *testing.T) {
	s := NewStore()
	tok := s.Start("inst", []byte(`{}`))

	// The server's PairPasskeyConfirmation is handled before
	// SendPasskeyResponse returns.
	s.SetConfirmation("inst", "123456", false)
	s.SetAwaitingConfirmation("inst")

	_, st, ok := s.Lookup(tok)
	if !ok {
		t.Fatal("ceremony not found")
	}
	if st.Stage != StageConfirmation || st.Code != "123456" {
		t.Fatalf("stage regressed: got stage=%q code=%q", st.Stage, st.Code)
	}
}

func TestSetAwaitingConfirmationFromChallenge(t *testing.T) {
	s := NewStore()
	tok := s.Start("inst", []byte(`{"challenge":"x"}`))
	s.SetAwaitingConfirmation("inst")

	_, st, _ := s.Lookup(tok)
	if st.Stage != StageAwaitingConfirmation {
		t.Fatalf("expected %q, got %q", StageAwaitingConfirmation, st.Stage)
	}
	if st.PublicKey != nil {
		t.Fatal("publicKey should be cleared once the response was submitted")
	}
}
