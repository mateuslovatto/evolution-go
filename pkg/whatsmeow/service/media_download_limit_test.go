package whatsmeow_service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

const testMediaBodyBytes = 4096

func testMediaServer(t *testing.T) *httptest.Server {
	t.Helper()
	body := make([]byte, testMediaBodyBytes)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func fetchTestMedia(t *testing.T, ctx context.Context, url string) ([]byte, error) {
	t.Helper()
	client := &http.Client{Transport: mediaLimitTransport{base: http.DefaultTransport}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func TestMediaLimitTransport_CortaAcimaDoTeto(t *testing.T) {
	srv := testMediaServer(t)

	_, err := fetchTestMedia(t, WithMediaLimit(context.Background(), 1024), srv.URL)

	var tooLarge *MediaTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("esperado MediaTooLargeError, veio %v", err)
	}
	if !errors.Is(err, ErrMediaTooLarge) {
		t.Fatalf("errors.Is(ErrMediaTooLarge) = false para %v", err)
	}
	if tooLarge.Max != 1024 {
		t.Fatalf("Max = %d, esperado 1024", tooLarge.Max)
	}
	if tooLarge.Size <= tooLarge.Max {
		t.Fatalf("Size = %d, deveria ser > Max (%d)", tooLarge.Size, tooLarge.Max)
	}
}

func TestMediaLimitTransport_AbaixoDoTetoBaixaTudo(t *testing.T) {
	srv := testMediaServer(t)

	data, err := fetchTestMedia(t, WithMediaLimit(context.Background(), testMediaBodyBytes), srv.URL)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if len(data) != testMediaBodyBytes {
		t.Fatalf("baixou %d bytes, esperado %d", len(data), testMediaBodyBytes)
	}
}

func TestMediaLimitTransport_SemTetoBaixaTudo(t *testing.T) {
	for name, ctx := range map[string]context.Context{
		"sem valor":  context.Background(),
		"teto zero":  WithMediaLimit(context.Background(), 0),
		"teto negat": WithMediaLimit(context.Background(), -1),
	} {
		t.Run(name, func(t *testing.T) {
			srv := testMediaServer(t)
			data, err := fetchTestMedia(t, ctx, srv.URL)
			if err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if len(data) != testMediaBodyBytes {
				t.Fatalf("baixou %d bytes, esperado %d", len(data), testMediaBodyBytes)
			}
		})
	}
}
