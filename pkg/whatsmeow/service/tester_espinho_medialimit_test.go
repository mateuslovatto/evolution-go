package whatsmeow_service

// QA adversarial (Espinho) — reteste M2: limite de bytes no transporte de mídia.

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const espinhoLimit = 1 << 20 // 1 MiB

func espinhoServe(t *testing.T, size int, chunked, gz bool) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		body := bytes.Repeat([]byte{0xab}, size)
		if gz {
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)
			_, _ = zw.Write(body)
			_ = zw.Close()
			w.Header().Set("Content-Encoding", "gzip")
			body = buf.Bytes()
		}
		if !chunked {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		}
		for i := 0; i < len(body); i += 32 * 1024 {
			end := min(i+32*1024, len(body))
			_, _ = w.Write(body[i:end])
			if chunked {
				w.(http.Flusher).Flush()
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func espinhoGet(t *testing.T, client *http.Client, url string, limit int64) (int, error) {
	t.Helper()
	req, _ := http.NewRequestWithContext(WithMediaLimit(context.Background(), limit), http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body) // igual ao whatsmeow downloadMedia
	return len(data), err
}

// Verde: o corte vale com Content-Length, sem Content-Length (chunked) e sobre o corpo
// já descomprimido (gzip transparente do transport: bomba de gzip não passa).
func TestEspinhoLimiteChunkedEGzip(t *testing.T) {
	client := mediaHTTPClient("")
	for _, c := range []struct {
		name        string
		chunked, gz bool
	}{{"content-length", false, false}, {"chunked", true, false}, {"gzip", false, true}} {
		t.Run(c.name, func(t *testing.T) {
			_, err := espinhoGet(t, client, espinhoServe(t, espinhoLimit+int(mediaCryptoOverhead)+1, c.chunked, c.gz), espinhoLimit)
			var tooLarge *MediaTooLargeError
			if !errors.As(err, &tooLarge) || !errors.Is(err, ErrMediaTooLarge) || tooLarge.Max != int64(espinhoLimit) {
				t.Fatalf("esperava MediaTooLargeError, veio %v", err)
			}
			n, err := espinhoGet(t, client, espinhoServe(t, espinhoLimit+int(mediaCryptoOverhead), c.chunked, c.gz), espinhoLimit)
			if err != nil || n != espinhoLimit+int(mediaCryptoOverhead) {
				t.Fatalf("no teto+overhead deveria baixar inteiro: n=%d err=%v", n, err)
			}
		})
	}
}

// Verde: sem teto no contexto (MinIO, uploads, history sync interno do whatsmeow) não limita.
func TestEspinhoSemTetoNoContextoNaoLimita(t *testing.T) {
	client := mediaHTTPClient("")
	url := espinhoServe(t, 3*espinhoLimit, true, false)
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil || len(data) != 3*espinhoLimit {
		t.Fatalf("sem teto deveria baixar tudo: n=%d err=%v", len(data), err)
	}
}

// Verde: o proxy http(s) da instância continua sendo usado pelo cliente de mídia.
func TestEspinhoProxyHttpPreservadoNaMidia(t *testing.T) {
	var viaProxy int32
	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&viaProxy, 1)
		if r.URL.Host != "mmg.whatsapp.invalid" || r.Header.Get("Proxy-Authorization") == "" {
			http.Error(w, "bad proxy request", http.StatusBadRequest)
			return
		}
		_, _ = w.Write(bytes.Repeat([]byte{1}, espinhoLimit+100))
	}))
	t.Cleanup(proxySrv.Close)
	proxyURL := "http://user:secret@" + proxySrv.Listener.Addr().String()
	_, err := espinhoGet(t, mediaHTTPClient(proxyURL), "http://mmg.whatsapp.invalid/v/t62/x.enc", espinhoLimit)
	if atomic.LoadInt32(&viaProxy) != 1 {
		t.Fatal("download de mídia não passou pelo proxy da instância")
	}
	if !errors.Is(err, ErrMediaTooLarge) {
		t.Fatalf("teto não aplicado via proxy: %v", err)
	}
}

// Verde: com socks5 a conexão de mídia sai pelo proxy (o listener recebe o handshake
// SOCKS5, byte 0x05), como o whatsmeow faria em SetSOCKSProxy.
func TestEspinhoProxySocks5UsadoNaMidia(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	got := make(chan byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var b [1]byte
		_, _ = conn.Read(b[:])
		got <- b[0]
	}()
	client := mediaHTTPClient("socks5://u:p@" + ln.Addr().String())
	_, _ = espinhoGet(t, client, "http://mmg.whatsapp.invalid/v/t62/x.enc", espinhoLimit)
	select {
	case b := <-got:
		if b != 0x05 {
			t.Fatalf("primeiro byte no proxy = %#x, esperado 0x05 (SOCKS5)", b)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("download de mídia não passou pelo proxy socks5")
	}
}

// zeroThenData devolve (0, nil) uma vez depois de esgotar o teto e só então o resto.
type zeroThenData struct {
	first, rest []byte
	zeroDone    bool
}

func (z *zeroThenData) Read(p []byte) (int, error) {
	if len(z.first) > 0 {
		n := copy(p, z.first)
		z.first = z.first[n:]
		return n, nil
	}
	if !z.zeroDone {
		z.zeroDone = true
		return 0, nil
	}
	if len(z.rest) > 0 {
		n := copy(p, z.rest)
		z.rest = z.rest[n:]
		return n, nil
	}
	return 0, io.EOF
}
func (z *zeroThenData) Close() error { return nil }

// L1 · BAIXA: na sonda depois do teto, um Read (0, nil) — permitido pelo contrato de
// io.Reader — é tratado como EOF: o arquivo maior que o teto sai truncado em silêncio
// (sem MediaTooLargeError). No whatsmeow isso vira erro de SHA/HMAC (mensagem sem mídia
// e sem mediaOmitted). O net/http raramente devolve (0, nil); a sonda deveria repetir
// até ler 1 byte ou receber erro/EOF.
func TestEspinhoSondaComReadZeroNilNaoTrunca(t *testing.T) {
	if os.Getenv("ESPINHO_DEFECTS") != "1" {
		t.Skip("defeito baixo documentado pelo Espinho; rode com ESPINHO_DEFECTS=1")
	}
	limit := int64(100)
	body := &zeroThenData{first: bytes.Repeat([]byte{1}, int(limit+mediaCryptoOverhead)), rest: bytes.Repeat([]byte{1}, 50)}
	capped := &cappedReadCloser{body: body, limit: limit, remaining: limit + mediaCryptoOverhead}
	_, err := io.ReadAll(capped)
	if !errors.Is(err, ErrMediaTooLarge) {
		t.Fatalf("arquivo acima do teto saiu truncado sem erro: %v", err)
	}
}
