package whatsmeow_service

import (
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
)

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestQRCodeExpiry(t *testing.T) {
	if got := qrCodeExpiry(0); got != 60*time.Second {
		t.Errorf("code 0 expiry = %v, want 60s", got)
	}
	if got := qrCodeExpiry(1); got != 80*time.Second {
		t.Errorf("code 1 expiry = %v, want 80s", got)
	}
}

func TestNewQRBatchCancelsPreviousRotation(t *testing.T) {
	mycli := &MyClient{config: &config.Config{}}

	first := mycli.beginQRRotation()
	second := mycli.beginQRRotation()
	if !isClosed(first) {
		t.Fatal("previous rotation must be cancelled when a new QR batch arrives")
	}
	if isClosed(second) || !mycli.qrRotationCurrent(second) {
		t.Fatal("new rotation must be live")
	}
	// A stale rotation can no longer end pairing for the instance.
	if _, ok := mycli.endQRPairing(first, "x"); ok {
		t.Fatal("stale rotation must not be able to tear down")
	}
	if ended, _ := mycli.qrPairingEnded(); ended {
		t.Fatal("pairing must not be marked as ended by a stale rotation")
	}

	mycli.stopQRRotation()
	if !isClosed(second) {
		t.Fatal("stopQRRotation must cancel the live rotation")
	}
	mycli.stopQRRotation() // idempotent
}

func TestEndQRPairingPreventsRestart(t *testing.T) {
	mycli := &MyClient{config: &config.Config{}}
	stop := mycli.beginQRRotation()

	if _, ok := mycli.endQRPairing(stop, "QR code limit reached (3)"); !ok {
		t.Fatal("live rotation must be able to end pairing")
	}
	if _, ok := mycli.endQRPairing(stop, "again"); ok {
		t.Fatal("pairing can only be ended once")
	}
	ended, reason := mycli.qrPairingEnded()
	if !ended || reason != "QR code limit reached (3)" {
		t.Fatalf("qrPairingEnded = %v, %q", ended, reason)
	}
	// Late events.QR after teardown are ignored.
	if mycli.beginQRRotation() != nil {
		t.Fatal("no new rotation may start after QR pairing ended")
	}
}

func TestNextQRCountLimit(t *testing.T) {
	mycli := &MyClient{config: &config.Config{QrcodeMaxCount: 2}}
	if n, lim := mycli.nextQRCount(); n != 1 || lim {
		t.Fatalf("first = %d, %v", n, lim)
	}
	if n, lim := mycli.nextQRCount(); n != 2 || !lim {
		t.Fatalf("second = %d, %v", n, lim)
	}

	unlimited := &MyClient{config: &config.Config{QrcodeMaxCount: 0}}
	for i := 0; i < 10; i++ {
		if _, lim := unlimited.nextQRCount(); lim {
			t.Fatal("0 must disable the limit")
		}
	}
}

func TestSignalKill(t *testing.T) {
	stop := make(chan struct{})

	// Delivered to a receiver.
	ch := make(chan bool)
	mycli := &MyClient{killCh: ch}
	go func() { <-ch }()
	if !mycli.signalKill(stop) {
		t.Fatal("signal should be delivered")
	}

	// Nil channel (instance deleted before the client started): no block.
	if (&MyClient{}).signalKill(stop) {
		t.Fatal("nil kill channel cannot be signaled")
	}

	// Channel closed by ClearInstanceCache: must not panic.
	closed := make(chan bool)
	close(closed)
	if (&MyClient{killCh: closed}).signalKill(stop) {
		t.Fatal("send on a closed channel must report failure")
	}

	// Rotation cancelled while waiting: returns without blocking.
	cancelled := make(chan struct{})
	close(cancelled)
	if !(&MyClient{killCh: make(chan bool)}).signalKill(cancelled) {
		t.Fatal("cancelled rotation means the kill branch is already running")
	}
}
