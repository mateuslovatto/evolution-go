package send_service

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"testing"
)

func pngBytes(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 4, 4))
	img.Set(0, 0, color.White)
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestFetchButtonHeaderMedia(t *testing.T) {
	pngData := pngBytes(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok.png":
			w.Header().Set("Content-Type", "image/jpeg") // wrong header: must be sniffed
			_, _ = w.Write(pngData)
		case "/notfound":
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("<html><body>not found</body></html>"))
		case "/html":
			_, _ = w.Write([]byte("<html><body>hello</body></html>"))
		}
	}))
	defer srv.Close()

	data, mime, err := fetchButtonHeaderMedia(srv.URL+"/ok.png", "image/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mime != "image/png" {
		t.Errorf("mime = %q, want image/png", mime)
	}
	if !bytes.Equal(data, pngData) {
		t.Error("data mismatch")
	}

	if _, _, err := fetchButtonHeaderMedia(srv.URL+"/notfound", "image/"); err == nil {
		t.Error("expected error for 404 response")
	}
	if _, _, err := fetchButtonHeaderMedia(srv.URL+"/html", "image/"); err == nil {
		t.Error("expected error for non-image content")
	}
	if _, _, err := fetchButtonHeaderMedia(srv.URL+"/ok.png", "video/"); err == nil {
		t.Error("expected error when an image is given as video header")
	}
}
